"""Self-hosted LiveKit voice agent instrumented with Roark analytics.

Roark captures the transcript, tool calls, per-stage metrics, and a stereo
recording (L = user, R = agent) for every call — you add one line,
``observe_session(...)``, before ``session.start()``.

Two ways to run it (see README.md):

* ``python agent.py console`` — talk to the agent right in your terminal using
  your mic + speakers. No LiveKit server, browser, or token needed. This is the
  simplest way to test the integration end to end.
* ``python agent.py dev`` — register as a worker against a self-hosted LiveKit
  server at ``LIVEKIT_URL`` and connect a browser via the Agents Playground.

Roark talks to its own API, never your LiveKit server, so observability
behaves the same in both.
"""

from __future__ import annotations

import os

from livekit.agents import (
    Agent,
    AgentSession,
    JobContext,
    RunContext,
    TurnHandlingOptions,
    WorkerOptions,
    cli,
    function_tool,
)
from livekit.agents.telemetry import set_tracer_provider
from livekit.plugins import cartesia, openai, silero, speechmatics
from livekit.plugins.turn_detector.multilingual import MultilingualModel
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from roark_analytics_python_livekit import observe_session

# Roark ingests LiveKit's OpenTelemetry traces at this OTLP/HTTP endpoint.
ROARK_TRACES_ENDPOINT = (
    "https://gq5hqc3it7hwlk2boaxcrfx4d40fgofu.lambda-url.us-east-1.on.aws/v1/traces"
)

SYSTEM_PROMPT = (
    "You are a friendly support assistant for an online store. "
    "Keep answers short and conversational. Use the lookup_order tool "
    "whenever a caller asks about the status of an order."
)


class SupportAgent(Agent):
    def __init__(self) -> None:
        super().__init__(instructions=SYSTEM_PROMPT)

    @function_tool
    async def lookup_order(self, context: RunContext, order_id: str) -> dict:
        """Look up the status of a customer order.

        Args:
            order_id: The order identifier the caller provides.
        """
        # Pretend this hits your real backend. The call + result are captured
        # by Roark automatically via the `function_tools_executed` event.
        return {"order_id": order_id, "status": "shipped", "eta": "2 days"}


async def setup_roark_tracer(ctx: JobContext) -> None:
    """Export LiveKit's OpenTelemetry traces (LLM/STT/TTS spans, tool calls) to Roark.

    Roark links a trace to its call by the room sid, which it reads from the
    ``livekit.room.id`` resource attribute — the same id ``observe_session`` uses
    as the ``livekitCallId``. The job carries the server-assigned sid as a plain
    (synchronous) ``ctx.job.room.sid`` field at dispatch, so read it directly.
    """
    resource = Resource.create(
        {
            "livekit.room.id": ctx.job.room.sid,
            # Set True to drop traces for test/synthetic calls instead of ingesting them.
            "roark.skip": False,
        }
    )
    provider = TracerProvider(resource=resource)
    provider.add_span_processor(
        BatchSpanProcessor(
            OTLPSpanExporter(
                endpoint=ROARK_TRACES_ENDPOINT,
                headers={"Authorization": f"Bearer {os.environ['ROARK_API_KEY']}"},
            )
        )
    )
    set_tracer_provider(provider)

    # Spans are batched; flush on shutdown so the tail of the call isn't dropped.
    async def flush_traces() -> None:
        provider.force_flush()

    ctx.add_shutdown_callback(flush_traces)


async def entrypoint(ctx: JobContext) -> None:
    # Connect first: the room's server-assigned ``sid`` (which observe_session uses
    # as the call id, so Roark can link the call to its OpenTelemetry trace) is only
    # available once the room is connected.
    await ctx.connect()

    # Stream LiveKit's OpenTelemetry traces to Roark. Keyed on the same room sid as
    # observe_session below, so the trace and the call line up in the dashboard.
    await setup_roark_tracer(ctx)

    session = AgentSession(
        stt=speechmatics.STT(),
        llm=openai.LLM(model="gpt-4o-mini"),
        tts=cartesia.TTS(),
        vad=silero.VAD.load(),
        turn_detection=MultilingualModel(),
        # Use local VAD-based barge-in. The default ("adaptive") interruption
        # mode calls a LiveKit-hosted ML service that needs Cloud credentials —
        # on a self-hosted server / console it just fails and falls back to VAD
        # anyway, so select "vad" up front and keep everything local.
        turn_handling=TurnHandlingOptions(interruption={"mode": "vad"}),
    )

    # --- Roark analytics ---------------------------------------------------
    # Wire this in AFTER ctx.connect() (so the room sid is resolvable) but BEFORE
    # session.start() (so the audio taps are installed before the session streams
    # frames). Failures are logged and swallowed — the agent keeps running even if
    # Roark is unreachable.
    await observe_session(
        ctx,
        session,
        api_key=os.environ["ROARK_API_KEY"],
        agent_id="support-bot-selfhosted-v1",
        agent_name="Support Bot (self-hosted)",
        agent_prompt=SYSTEM_PROMPT,
        capture_audio=True,  # stereo PCM: L = user, R = agent
    )
    # -----------------------------------------------------------------------

    await session.start(room=ctx.room, agent=SupportAgent())

    # Auto-invoke a tool on startup so a tool call always shows up in Roark —
    # handy for verifying the integration without having to steer the
    # conversation by voice. The agent calls `lookup_order` for a sample order,
    # and Roark captures the call + result via `function_tools_executed`.
    await session.generate_reply(
        instructions=(
            "Greet the caller, then immediately call the lookup_order tool for "
            "order 'TEST-1234' and tell them its status."
        )
    )


if __name__ == "__main__":
    cli.run_app(WorkerOptions(entrypoint_fnc=entrypoint))

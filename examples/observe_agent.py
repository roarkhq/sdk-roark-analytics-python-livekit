"""Minimal LiveKit Agents entrypoint instrumented with Roark observe_session.

Run with::

    LIVEKIT_URL=wss://... \\
    LIVEKIT_API_KEY=... \\
    LIVEKIT_API_SECRET=... \\
    ROARK_API_KEY=rk_live_... \\
        python -m livekit.agents.cli dev examples/observe_agent.py

STT / LLM / TTS providers are left as TODOs — wire whichever you prefer.
"""

from __future__ import annotations

import os

from livekit.agents import Agent, AgentSession, JobContext, WorkerOptions, cli

from roark_analytics_python_livekit import observe_session

SYSTEM_PROMPT = "You are a friendly voice assistant. Keep answers short."


class Assistant(Agent):
    def __init__(self) -> None:
        super().__init__(instructions=SYSTEM_PROMPT)


async def entrypoint(ctx: JobContext) -> None:
    await ctx.connect()

    session = AgentSession(
        # Plug your providers in here. Examples (uncomment + install extras):
        # stt=openai.STT(),
        # llm=openai.LLM(model="gpt-4o-mini"),
        # tts=cartesia.TTS(),
        # vad=silero.VAD.load(),
    )

    # Roark observability — captures transcripts, tool calls, metrics, and a stereo
    # audio recording for this session. Failures are swallowed; the agent never breaks.
    await observe_session(
        ctx,
        session,
        api_key=os.environ["ROARK_API_KEY"],
        agent_id="livekit-assistant-v1",
        agent_name="LiveKit Assistant",
        agent_prompt=SYSTEM_PROMPT,
    )

    await session.start(room=ctx.room, agent=Assistant())


if __name__ == "__main__":
    cli.run_app(WorkerOptions(entrypoint_fnc=entrypoint))

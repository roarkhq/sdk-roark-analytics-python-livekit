"""Roark analytics integration for LiveKit Agents.

Drop ``observe_session`` (production) or ``track_session`` (testing /
simulations) into your agent entrypoint — Roark captures call lifecycle,
transcripts, tool calls, metrics, and a stereo audio recording. No other
code changes required.

Example::

    from livekit.agents import AgentSession, JobContext
    from roark_analytics_python_livekit import observe_session

    async def entrypoint(ctx: JobContext):
        await ctx.connect()
        session = AgentSession(stt=..., llm=..., tts=...)
        await observe_session(
            ctx, session,
            api_key="rk_live_...",
            agent_id="support-bot-v3",
            agent_name="Support Bot v3",
            agent_prompt=SYSTEM_PROMPT,
        )
        await session.start(room=ctx.room, agent=Assistant())

See https://docs.roark.ai/integrations/livekit-agents for the full setup guide.
"""

from importlib.metadata import PackageNotFoundError, version

from .client import RoarkClient
from .session import get_simulation_data, observe_session, track_session

try:
    __version__ = version("roark-analytics-python-livekit")
except PackageNotFoundError:  # pragma: no cover — running from a source tree
    __version__ = "0.0.0+unknown"

__all__ = [
    "RoarkClient",
    "get_simulation_data",
    "observe_session",
    "track_session",
]

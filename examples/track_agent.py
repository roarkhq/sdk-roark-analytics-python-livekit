"""Simulation-mode entrypoint instrumented with Roark track_session.

``track_session`` mirrors ``observe_session`` and additionally swaps any
function tools registered on the agent with scripted stubs read from the job
metadata (``roark.mockTools`` block). Use this for end-to-end agent tests where
the LLM should not actually hit real APIs.
"""

from __future__ import annotations

import os

from livekit.agents import Agent, AgentSession, JobContext, WorkerOptions, cli
from livekit.agents.llm import function_tool

from roark_analytics_python_livekit import track_session


class Assistant(Agent):
    def __init__(self) -> None:
        super().__init__(
            instructions=(
                "You are a customer-support assistant. Use the `lookup_order` tool to "
                "find order details when the user asks."
            ),
        )

    @function_tool
    async def lookup_order(self, order_id: str) -> dict[str, str]:
        """Return order details from the database. Tool is replaced in simulation mode."""
        # In production this would hit a real backend. In a Roark simulation,
        # `track_session` swaps this for the scripted reply from job metadata.
        raise NotImplementedError("real backend not wired")


async def entrypoint(ctx: JobContext) -> None:
    await ctx.connect()

    agent = Assistant()
    session = AgentSession()

    # Job metadata typically looks like:
    #   {"roark": {"mockTools": {"lookup_order": {"orderId": "1", "status": "shipped"}}}}
    await track_session(
        ctx,
        session,
        agent=agent,
        api_key=os.environ["ROARK_API_KEY"],
        agent_id="livekit-assistant-v1",
        agent_name="LiveKit Assistant (sim)",
    )

    await session.start(room=ctx.room, agent=agent)


if __name__ == "__main__":
    cli.run_app(WorkerOptions(entrypoint_fnc=entrypoint))

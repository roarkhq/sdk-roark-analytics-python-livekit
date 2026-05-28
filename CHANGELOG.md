# Changelog

## 0.1.0 — 2026-05-28

Initial release.

- `observe_session(ctx, session, ...)` — production helper. Wires `AgentSession`
  events (`conversation_item_added`, `function_tools_executed`,
  `metrics_collected`, `agent_state_changed`) and subscribes to room audio for
  stereo recording.
- `track_session(ctx, session, agent=..., ...)` — simulation helper. Mirrors
  `observe_session` plus mock-tool injection from `ctx.job.metadata.roark.mockTools`.
- `get_simulation_data(ctx)` — parse the `roark.*` block of `JobContext.job.metadata`.
- Kill switches: `ROARK_OBSERVABILITY_ENABLED`, `ROARK_TRACING_ENABLED`,
  `ROARK_MOCK_TOOLS_ENABLED`.
- Endpoints: `https://api.roark.ai/v1/integrations/livekit-sdk`.

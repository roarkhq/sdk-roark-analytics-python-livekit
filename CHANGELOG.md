# Changelog

All notable changes to `roark-analytics-python-livekit` are documented here.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-06-04

Drop-in `observe_session` for LiveKit Agents that ships call lifecycle,
transcripts, tool calls, metrics, and stereo recordings to Roark. Tested against
`livekit-agents >= 1.0, < 2`.

### Added

- `observe_session(ctx, session, ...)` — wires the standard `AgentSession`
  event surface (`conversation_item_added`, `function_tools_executed`,
  `metrics_collected`, `agent_state_changed`) and taps `session.input.audio`
  (user) + `session.output.audio` (agent) for a stereo recording. Works the
  same in `dev` (room) and `console` mode.
- Stereo call recording — L = user, R = agent. Channel alignment (each turn
  placed on the timeline, real silence spliced between turns, faster-than-
  real-time TTS bursts kept at their true duration) reuses livekit-agents' own
  `RecorderIO`, so the recording matches what LiveKit would write to disk and
  transcript/tool markers (placed at `audioOffsetMs`) land on the waveform. The
  sample rate is adopted from the negotiated stream (8 kHz telephony,
  16/24/48 kHz WebRTC, …) and reported on `call-ended` as `recordingSampleRate`.
  Audio is chunked and uploaded to Roark via presigned URLs
  (`/v1/integrations/livekit-sdk/chunk-upload-url`); in-flight uploads are
  drained before `call-ended` is posted.
- Lazy agent registration on the first call seen for a given `agent_id`, plus
  per-call lifecycle webhooks (`call-started` / `call-ended`).
- `aflush(reason=...)` idempotent escape hatch, and an automatic `JobContext`
  shutdown callback so calls finalize when LiveKit ends the job.
- Configuration via keyword arguments: `api_key`, `agent_id` (required);
  `agent_name`, `agent_prompt`, `livekit_call_id`, `capture_audio`,
  `capture_logs`, `is_test`, and free-form `**metadata` (optional).
- `ROARK_OBSERVABILITY_ENABLED` kill switch — set to a falsy value
  (`false` / `0` / `no` / `off`) to make `observe_session` a no-op without
  touching code.
- Failures are logged and swallowed — the helper never raises into the session.
- `RoarkClient` — the underlying async HTTP client for the Roark webhook and
  presigned-upload calls.
- `examples/` — a runnable self-hosted LiveKit + Roark example (uv project)
  with a Roark-instrumented support agent, runnable in local `console` mode or
  against a self-hosted `livekit-server`.

[Unreleased]: https://github.com/roarkhq/sdk-roark-analytics-python-livekit/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/roarkhq/sdk-roark-analytics-python-livekit/releases/tag/v0.1.0

# Self-hosted LiveKit + Roark analytics example

A single voice agent (`agent.py`) instrumented with
[`roark-analytics-python-livekit`](../README.md). Roark captures the transcript,
the `lookup_order` tool call/result, per-stage metrics, and a stereo recording
(L = you, R = agent) for every call.

It also exports LiveKit's
[OpenTelemetry traces](https://docs.roark.ai/documentation/observability/traces#livekit)
to Roark via `setup_roark_tracer` — the LLM/STT/TTS spans for the call, keyed on
the room sid so the trace lines up with the call in the dashboard. No extra
config: it reuses `ROARK_API_KEY`.

Everything here runs **on your own machine** — no LiveKit Cloud involved.

## 1. Install (uv)

This example is a [uv](https://docs.astral.sh/uv/) project. `uv sync` creates a
local `.venv` and installs the agent's dependencies (including this SDK from one
level up, via a path source in `pyproject.toml`):

```bash
cd examples
uv sync
```

## 2. Configure

```bash
cp .env.example .env      # then edit: fill in the provider keys + ROARK_API_KEY
```

## 3. Run it — console mode (recommended)

Talk to the agent right in your terminal using your mic and speakers. No LiveKit
server, browser, or token — fully local:

```bash
uv run --env-file .env agent.py console
```

As you talk you should see Roark log lines:

```
tapping user audio input (...)
tapping agent audio output (...)
call-started: livekitCallId=...
```

On startup the agent **automatically calls the `lookup_order` tool** for a
sample order (`TEST-1234`) — so a tool call always shows up in Roark without you
having to steer the conversation by voice. You'll see it captured via the
`function_tools_executed` event.

…and, after a few seconds of audio, the chunked recording upload begins. When
you end the call (Ctrl+C), `call-ended` flushes the transcript, tool calls,
metrics, and the final audio chunk to Roark.

## 4. Run it — local server mode (optional)

This is **still self-hosted** — you run a `livekit-server` binary on your own
machine; nothing routes through LiveKit Cloud. Use this if you want to test
against a real LiveKit room rather than the terminal.

```bash
# 1. Run a local LiveKit server (a single self-hosted binary):
brew install livekit
livekit-server --dev          # listens on ws://localhost:7880, dev key devkey/secret

# 2. Point LIVEKIT_URL at it in .env, then run the worker:
uv run --env-file .env agent.py dev
```

Then join the room with a participant. Options, most-local first:

- **`lk` CLI** (fully local): `lk room join --identity me --publish-mic test`
  ([LiveKit CLI](https://docs.livekit.io/home/cli/cli-setup/)).
- **Self-hosted Agents Playground**: run
  [`livekit/agents-playground`](https://github.com/livekit/agents-playground)
  locally and point it at `ws://localhost:7880`.
- **Hosted Agents Playground** (<https://agents-playground.livekit.io/>): the
  web page is served by LiveKit, but the audio connects directly to *your* local
  server — convenient, though it does load a third-party page.

## Kill switch

Set `ROARK_OBSERVABILITY_ENABLED=false` in `.env` to make `observe_session` a
no-op without touching code.

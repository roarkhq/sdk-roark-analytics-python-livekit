# roark-analytics-python-livekit

A [Roark](https://roark.ai) analytics integration for
[LiveKit Agents](https://docs.livekit.io/agents/). Drop one helper into your
agent entrypoint — Roark captures call lifecycle, transcripts, tool calls,
metrics, and a stereo audio recording. No other code changes required.

- **Tested with** `livekit-agents` 1.x
- **Python** 3.10+
- **Built for self-hosted LiveKit** — works against your own `livekit-server`
  and in local `console` mode

> Maintained by [Roark](https://roark.ai). File issues at
> <https://github.com/roarkhq/sdk-roark-analytics-python-livekit/issues>.

---

## Contents

- [Quick start](#quick-start)
- [How it works](#how-it-works)
- [Examples](#examples)
- [Kill switches](#kill-switches)
- [Troubleshooting](#troubleshooting)
- [Configuration reference](#configuration-reference)
- [Development](#development)
- [License](#license)

---

## Quick start

### 1. Install

```bash
pip install roark-analytics-python-livekit
```

### 2. Create a LiveKit integration & API key

Every Roark API key is **bound to a specific integration** — the key only
works for the integration it was created under. Before you can send calls from
this package, create the integration first:

1. In the [Roark dashboard](https://app.roark.ai), go to **Integrations** and
   create a new **LiveKit** integration.
2. Open that integration and generate an **API key** for it.
3. Copy the key (it looks like `rk_live_...`) — this is the value you'll set as
   `ROARK_API_KEY` below.

> Use the key created **under the LiveKit integration**. A key from a different
> integration (or an account-level key not bound to one) will be rejected.

### 3. Configure

Set one env var:

```bash
ROARK_API_KEY=rk_live_...
```

> The Roark API key is all you configure — the helpers know their own service
> endpoints.

### 4. Wire `observe_session`

```python
from livekit.agents import Agent, AgentSession, JobContext
from roark_analytics_python_livekit import observe_session

SYSTEM_PROMPT = "You are a friendly voice assistant."

class Assistant(Agent):
    def __init__(self):
        super().__init__(instructions=SYSTEM_PROMPT)

async def entrypoint(ctx: JobContext):
    # Connect before observe_session: the room sid (used as the call id) is only
    # available once connected.
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
```

That's it — transcripts, tool calls, metrics, and the stereo recording flow
to Roark automatically.

---

## How it works

The helper subscribes to the standard `AgentSession` event surface and ships
a compact event timeline to Roark:

| Phase | Source | What's captured |
|---|---|---|
| **Session start** | `JobContext.connect()` | `call-started` POST. Agent is lazy-registered on Roark the first time it sees this `agent_id`. |
| **Transcripts** | `session.on("conversation_item_added")` | `ChatMessage` role + content. Both user and assistant turns. |
| **Tool calls** | `session.on("function_tools_executed")` | Paired `tool_call` / `tool_result` records, keyed by `tool_call_id`. |
| **Metrics** | `session.on("metrics_collected")` | EOU / STT / LLM / TTS / Agent latency + LLM token usage. |
| **Audio** | Taps on `session.input.audio` (user) + `session.output.audio` (agent) | Stereo PCM (L=user, R=agent), chunked PUTs to Roark via `/v1/integrations/livekit-sdk/chunk-upload-url`. Tapping the session's own audio I/O works the same in `dev` (room) and `console` mode. |
| **Session end** | `ctx.add_shutdown_callback(...)` (or explicit `await state.aflush()`) | Flushes pending state, drains in-flight uploads, POSTs `call-ended`. Roark stitches the chunks into a WAV on its side. |

Failures are logged and swallowed — **the helpers never raise into the
session**. Your agent keeps running even if Roark is unreachable.

---

## Examples

A complete, runnable self-hosted example ships in
[`examples/`](./examples/) — a [uv](https://docs.astral.sh/uv/) project with a
Roark-instrumented support agent plus a short README. The simplest way to try it
is local console mode (mic + speakers, no LiveKit server or browser needed):

```bash
cd examples
uv sync
cp .env.example .env      # fill in provider keys + ROARK_API_KEY
uv run --env-file .env agent.py console
```

See [`examples/README.md`](./examples/README.md) for local-server mode (a
self-hosted `livekit-server` + a room client). Everything runs on your own
machine — no LiveKit Cloud.

> **Both modes use the same `ROARK_API_KEY`** — the key created under your
> LiveKit integration (see [Create a LiveKit integration & API key](#2-create-a-livekit-integration--api-key)).
> Only where the key is *stored* differs: a local `.env` / secrets manager for
> a self-hosted worker, deployment secrets wherever the worker runs.

---

## Kill switches

Use these env vars to disable Roark instrumentation at runtime without
touching code:

| Variable | Effect |
|----------|--------|
| `ROARK_OBSERVABILITY_ENABLED=false` | `observe_session` becomes a no-op (returns `None`). |

Treated as off: `false`, `0`, `no`, `off` (case-insensitive). Anything else
(including the variable being absent) keeps the feature enabled.

---

## Troubleshooting

<details>
<summary><strong>Calls aren't finalizing on Roark</strong></summary>

<br>

The helper registers a `shutdown_callback` on the `JobContext`, which fires
when LiveKit ends the job. If your transport tears down without firing the
hook, call `await state.aflush(reason="...")` explicitly from your own
disconnect handler — `aflush()` is idempotent.

</details>

<details>
<summary><strong>Transcripts arrive empty</strong></summary>

<br>

Transcripts are sourced from the `conversation_item_added` event on
`AgentSession`. If you're using a custom pipeline that bypasses
`AgentSession.start(room=…, agent=…)`, that event may never fire — verify by
adding your own `session.on("conversation_item_added", print)` listener.

</details>

<details>
<summary><strong>Recording is missing one side</strong></summary>

<br>

The user side is tapped from `session.input.audio`; the agent side from
`session.output.audio`. The taps are installed by `observe_session`, so it
**must be called before `session.start()`** (otherwise the session captures
its audio I/O before the taps are in place). Check the worker logs for
`tapping user audio input` / `tapping agent audio output` INFO lines — if one
is missing, that side will be silent on the merged stereo recording.

</details>

---

## Configuration reference

| Parameter | Type | Default | Notes |
|-----------|------|---------|-------|
| `api_key` | `str` | — | **Required.** Roark API key. |
| `agent_id` | `str` | — | **Required.** Customer-stable agent identifier. |
| `agent_name` | `str \| None` | `None` | Display name. |
| `agent_prompt` | `str \| None` | `None` | System prompt; persisted as the agent's prompt revision. |
| `livekit_call_id` | `str \| None` | `ctx.job.room.sid` → `ctx.room.sid` → `ctx.job.id` → UUID | Stable call identifier, sent on every Roark record as `livekitCallId`. Defaults to the room sid (the id Roark keys the call on); resolve the live `ctx.room.sid` by calling `observe_session` after `ctx.connect()`. |
| `capture_audio` | `bool` | `True` | Set to `False` to skip stereo capture (saves bandwidth). |
| `capture_logs` | `bool` | `True` | Reserved for future log streaming. |
| `is_test` | `bool` | `False` | Tag the call as a test on the Roark dashboard. |
| `**metadata` | `dict` | `{}` | Free-form metadata forwarded on `call-started`. |

---

## Development

```bash
pip install -e ".[dev]"
pytest
ruff check .
```

---

## License

MIT — see [LICENSE](./LICENSE).

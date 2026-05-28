# roark-analytics-python-livekit

A [Roark](https://roark.ai) analytics integration for
[LiveKit Agents](https://docs.livekit.io/agents/). Drop one helper into your
agent entrypoint — Roark captures call lifecycle, transcripts, tool calls,
metrics, and a stereo audio recording. No other code changes required.

- **Tested with** `livekit-agents` 1.x
- **Python** 3.10+
- **Same code** runs self-hosted *and* on LiveKit Cloud workers

> Maintained by [Roark](https://roark.ai). File issues at
> <https://github.com/roarkhq/sdk-roark-analytics-python-livekit/issues>.

---

## Contents

- [Quick start](#quick-start)
- [How it works](#how-it-works)
- [Production vs. simulations](#production-vs-simulations)
- [Examples](#examples)
- [Mock tools (simulation mode)](#mock-tools-simulation-mode)
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

### 2. Configure

Set one env var:

```bash
ROARK_API_KEY=rk_live_...
```

> The Roark API key is all you configure — the helpers know their own service
> endpoints.

### 3. Wire `observe_session`

```python
from livekit.agents import Agent, AgentSession, JobContext
from roark_analytics_python_livekit import observe_session

SYSTEM_PROMPT = "You are a friendly voice assistant."

class Assistant(Agent):
    def __init__(self):
        super().__init__(instructions=SYSTEM_PROMPT)

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
| **Audio** | `ctx.room.on("track_subscribed")` + `ctx.room.on("local_track_published")` | Stereo PCM (L=user, R=agent), chunked PUTs to Roark via `/v1/integrations/livekit-agents/chunk-upload-url`. |
| **Session end** | `ctx.add_shutdown_callback(...)` (or explicit `await state.aflush()`) | Flushes pending state, drains in-flight uploads, POSTs `call-ended`. Roark stitches the chunks into a WAV on its side. |

Failures are logged and swallowed — **the helpers never raise into the
session**. Your agent keeps running even if Roark is unreachable.

---

## Production vs. simulations

| | `observe_session` | `track_session` |
|---|---|---|
| Captures transcripts / tools / metrics / audio | yes | yes |
| Mocks tool implementations from job metadata | no | yes (see below) |
| Kill switch | `ROARK_OBSERVABILITY_ENABLED=false` | `ROARK_TRACING_ENABLED=false` |
| Intended for | live customer traffic | Roark simulation runs / agent tests |

The data captured is identical — `track_session` adds optional mock-tool
injection on top.

---

## Examples

Two example files ship with the package:

- **`examples/observe_agent.py`** — minimal production wiring with
  `observe_session`. STT / LLM / TTS are left as TODOs — drop in your
  providers.
- **`examples/track_agent.py`** — simulation entrypoint using
  `track_session`. Demonstrates the `@function_tool` mocking flow.

```bash
LIVEKIT_URL=wss://... LIVEKIT_API_KEY=... LIVEKIT_API_SECRET=... \\
ROARK_API_KEY=rk_live_... \\
    python -m livekit.agents.cli dev examples/observe_agent.py
```

---

## Mock tools (simulation mode)

`track_session` reads scripted tool replies from the LiveKit job metadata
under the `roark.mockTools` key. The Roark simulation orchestrator dispatches
jobs with metadata like:

```json
{
  "roark": {
    "runId": "sim-run-42",
    "scenarioId": "happy-path",
    "mockTools": {
      "lookup_order": { "orderId": "1", "status": "shipped" },
      "send_email":   { "ok": true }
    }
  }
}
```

When `track_session(ctx, session, agent=my_agent, …)` runs, every function
tool on `my_agent` whose name appears in `mockTools` is swapped for a
coroutine returning the scripted reply. Tools not listed are left as-is, so
the simulation can mock a subset.

To inspect the same metadata yourself (e.g. to branch on scenario id):

```python
from roark_analytics_python_livekit import get_simulation_data

sim = get_simulation_data(ctx)
scenario_id = sim.get("scenarioId")
```

---

## Kill switches

Use these env vars to disable Roark instrumentation at runtime without
touching code:

| Variable | Effect |
|----------|--------|
| `ROARK_OBSERVABILITY_ENABLED=false` | `observe_session` becomes a no-op (returns `None`). |
| `ROARK_TRACING_ENABLED=false`       | `track_session` becomes a no-op (returns `None`). |
| `ROARK_MOCK_TOOLS_ENABLED=false`    | `track_session` still ships data but does NOT swap tool callables. |

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

The user side comes from a remote audio track; the agent side comes from
the local participant's published audio track. If either is missing on the
LiveKit room, that side will be silent on the merged stereo recording. Check
the worker logs for `subscribed to user audio` / `subscribed to agent audio`
INFO lines.

</details>

---

## Configuration reference

| Parameter | Type | Default | Notes |
|-----------|------|---------|-------|
| `api_key` | `str` | — | **Required.** Roark API key. |
| `agent_id` | `str` | — | **Required.** Customer-stable agent identifier. |
| `agent_name` | `str \| None` | `None` | Display name. |
| `agent_prompt` | `str \| None` | `None` | System prompt; persisted as the agent's prompt revision. |
| `livekit_call_id` | `str \| None` | `ctx.job.id` else UUID | Stable call identifier; sent on every Roark record as `livekitCallId`. |
| `capture_audio` | `bool` | `True` | Set to `False` to skip stereo capture (saves bandwidth). |
| `capture_logs` | `bool` | `True` | Reserved for future log streaming. |
| `agent` (track_session only) | `Agent \| None` | `None` | LiveKit `Agent` whose function tools should be mocked. |
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

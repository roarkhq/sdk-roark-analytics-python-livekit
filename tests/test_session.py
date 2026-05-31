"""Tests for the observe_session helper.

These don't require a live livekit-agents runtime — we stub the AgentSession
and JobContext surfaces the helpers touch (the ``on`` event registration, the
shutdown-callback hook, and the room/job attributes).
"""

from __future__ import annotations

import json
from typing import Any

import httpx
import pytest

from roark_analytics_python_livekit.client import API_KEY_HEADER, RoarkClient
from roark_analytics_python_livekit.session import (
    _RoarkSession,
    observe_session,
)


class _StubJob:
    def __init__(self, *, metadata: str | None = None) -> None:
        self.id = "job-123"
        self.metadata = metadata


class _StubRoom:
    name = "room-name"
    sid = "RM_1234"

    def __init__(self) -> None:
        self._handlers: dict[str, Any] = {}

    def on(self, event: str, cb: Any) -> None:
        self._handlers[event] = cb


class _StubCtx:
    def __init__(self, *, metadata: str | None = None) -> None:
        self.job = _StubJob(metadata=metadata)
        self.room = _StubRoom()
        self._shutdown_cb: Any = None

    def add_shutdown_callback(self, cb: Any) -> None:
        self._shutdown_cb = cb


class _StubSession:
    def __init__(self) -> None:
        self._listeners: dict[str, list[Any]] = {}

    def on(self, event: str, cb: Any) -> None:
        self._listeners.setdefault(event, []).append(cb)

    def fire(self, event: str, payload: Any) -> None:
        for cb in self._listeners.get(event, []):
            cb(payload)


class _Item:
    def __init__(self, role: str, content: str) -> None:
        self.role = role
        self.content = content


class _FuncCall:
    def __init__(self, name: str, arguments: Any, result: Any) -> None:
        self.tool_call_id = f"call-{name}"
        self.name = name
        self.arguments = arguments
        self.result = result


def _mocked_client() -> tuple[RoarkClient, list[dict[str, Any]]]:
    """Construct a RoarkClient whose underlying httpx clients are mocked."""
    posted: list[dict[str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        posted.append(
            {
                "url": str(request.url),
                "body": json.loads(request.content) if request.content else {},
            }
        )
        return httpx.Response(200, json={"ok": True})

    client = RoarkClient(api_key="rk_test")
    client._client = httpx.AsyncClient(  # type: ignore[attr-defined]
        transport=httpx.MockTransport(handler),
        headers={API_KEY_HEADER: "rk_test"},
    )
    client._s3_client = httpx.AsyncClient(  # type: ignore[attr-defined]
        transport=httpx.MockTransport(handler),
    )
    return client, posted


@pytest.mark.asyncio
async def test_observe_session_posts_call_started_and_ended() -> None:
    client, posted = _mocked_client()
    ctx = _StubCtx()
    session = _StubSession()

    state = _RoarkSession(
        ctx=ctx,  # type: ignore[arg-type]
        session=session,  # type: ignore[arg-type]
        api_key="rk_test",
        agent_id="agent-1",
        agent_name="Agent One",
        agent_prompt="be nice",
        livekit_call_id="call-xyz",
        capture_audio=False,
        is_test=False,
        metadata={},
    )
    state._client = client

    await state.start()
    assert posted[-1]["body"]["event"] == "call-started"
    assert posted[-1]["body"]["livekitCallId"] == "call-xyz"
    assert posted[-1]["body"]["agentId"] == "agent-1"

    # Fire transcript + tool events; both should land on the call-ended payload.
    session.fire("conversation_item_added", _Item("user", "Hello there"))
    session.fire("conversation_item_added", _Item("assistant", "Hi! How can I help?"))

    class _Event:
        called_functions = [_FuncCall("lookup_order", {"id": 1}, {"status": "ok"})]

    session.fire("function_tools_executed", _Event())

    await state.aflush(reason="agent-ended")
    ended = posted[-1]["body"]
    assert ended["event"] == "call-ended"
    assert ended["callEndedReason"] == "agent-ended"
    assert len(ended["transcript"]) == 2
    assert ended["transcript"][0]["role"] == "user"
    assert ended["transcript"][0]["content"] == "Hello there"
    tool_kinds = [m["kind"] for m in ended["toolCalls"]]
    assert "tool_call" in tool_kinds and "tool_result" in tool_kinds


@pytest.mark.asyncio
async def test_call_started_tolerates_nonstring_room_attrs() -> None:
    """Console mode exposes room.name/.sid as mock objects (and some livekit
    versions make room.sid a coroutine). start() must still produce a
    JSON-serializable call-started payload, dropping the non-string fields."""
    from unittest.mock import AsyncMock

    class _MockRoom:
        name = AsyncMock()
        sid = AsyncMock()

        def on(self, event: str, cb: Any) -> None:  # noqa: ARG002
            pass

    class _MockCtx:
        def __init__(self) -> None:
            self.job = _StubJob()
            self.room = _MockRoom()

        def add_shutdown_callback(self, cb: Any) -> None:  # noqa: ARG002
            pass

    client, posted = _mocked_client()
    state = _RoarkSession(
        ctx=_MockCtx(),  # type: ignore[arg-type]
        session=_StubSession(),  # type: ignore[arg-type]
        api_key="rk_test",
        agent_id="agent-1",
        agent_name=None,
        agent_prompt=None,
        livekit_call_id="mock-job-1",
        capture_audio=False,
        is_test=False,
        metadata={},
    )
    state._client = client

    await state.start()  # must not raise

    body = posted[-1]["body"]
    assert body["event"] == "call-started"
    assert body["jobId"] == "job-123"  # real string survives
    assert "roomName" not in body  # AsyncMock dropped
    assert "roomSid" not in body


@pytest.mark.asyncio
async def test_call_started_awaits_async_room_sid() -> None:
    """Current livekit-rtc exposes ``room.sid`` as an async property (it awaits
    the server-assigned RM_… id). start() must await it and ship the resolved
    string as ``roomSid`` — the id the OTel tracing integration links on."""

    class _AsyncSidRoom:
        name = "room-name"

        @property
        async def sid(self) -> str:
            return "RM_async_5678"

        def on(self, event: str, cb: Any) -> None:  # noqa: ARG002
            pass

    class _AsyncSidCtx:
        def __init__(self) -> None:
            self.job = _StubJob()
            self.room = _AsyncSidRoom()

        def add_shutdown_callback(self, cb: Any) -> None:  # noqa: ARG002
            pass

    client, posted = _mocked_client()
    state = _RoarkSession(
        ctx=_AsyncSidCtx(),  # type: ignore[arg-type]
        session=_StubSession(),  # type: ignore[arg-type]
        api_key="rk_test",
        agent_id="agent-1",
        agent_name=None,
        agent_prompt=None,
        livekit_call_id="job-123",
        capture_audio=False,
        is_test=False,
        metadata={},
    )
    state._client = client

    await state.start()

    body = posted[-1]["body"]
    assert body["roomName"] == "room-name"
    assert body["roomSid"] == "RM_async_5678"  # awaited, not a coroutine repr


@pytest.mark.asyncio
async def test_aflush_is_idempotent() -> None:
    client, posted = _mocked_client()
    state = _RoarkSession(
        ctx=_StubCtx(),  # type: ignore[arg-type]
        session=_StubSession(),  # type: ignore[arg-type]
        api_key="rk_test",
        agent_id="a",
        agent_name=None,
        agent_prompt=None,
        livekit_call_id="c1",
        capture_audio=False,
        is_test=False,
        metadata={},
    )
    state._client = client
    await state.start()
    await state.aflush(reason="r1")
    posted_count = len(posted)
    await state.aflush(reason="r2")  # second flush is a no-op
    assert len(posted) == posted_count


@pytest.mark.asyncio
async def test_observe_session_respects_kill_switch(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ROARK_OBSERVABILITY_ENABLED", "false")
    out = await observe_session(
        _StubCtx(),  # type: ignore[arg-type]
        _StubSession(),  # type: ignore[arg-type]
        api_key="rk",
        agent_id="a",
    )
    assert out is None


def test_install_audio_setter_wraps_current_and_future() -> None:
    """The setter interceptor must wrap both the value already present and any
    value assigned later (this is how it catches RoomIO / console assigning
    ``input.audio`` during ``session.start()``)."""

    class _IO:
        def __init__(self) -> None:
            self._a: Any = "preexisting"

        @property
        def audio(self) -> Any:
            return self._a

        @audio.setter
        def audio(self, value: Any) -> None:
            self._a = value

    io = _IO()
    _RoarkSession._install_audio_setter(io, lambda v: f"wrapped:{v}")

    assert io.audio == "wrapped:preexisting"  # current value wrapped on install
    io.audio = "mic"
    assert io.audio == "wrapped:mic"  # future assignment wrapped too
    io.audio = None
    assert io.audio is None  # None passes through unwrapped


class _StateEvent:
    def __init__(self, new_state: str) -> None:
        self.new_state = new_state


class _ItemWithMetrics:
    """A committed ChatMessage carrying LiveKit's per-utterance speaking metrics."""

    def __init__(
        self, role: str, content: str, started: float | None, stopped: float | None
    ) -> None:
        self.role = role
        self.content = content
        self.metrics: dict[str, float] = {}
        if started is not None:
            self.metrics["started_speaking_at"] = started
        if stopped is not None:
            self.metrics["stopped_speaking_at"] = stopped


@pytest.mark.asyncio
async def test_transcript_offsets_use_exact_voice_metrics(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Primary path: when the committed item carries ``started_speaking_at`` /
    ``stopped_speaking_at`` (VAD onset/offset for the user, real playback for the
    agent), the span is read from those exact voice boundaries — independent of
    when ``conversation_item_added`` fired and of any TTS burst."""
    import roark_analytics_python_livekit.session as session_mod

    client, posted = _mocked_client()
    state = _RoarkSession(
        ctx=_StubCtx(),  # type: ignore[arg-type]
        session=_StubSession(),  # type: ignore[arg-type]
        api_key="rk_test",
        agent_id="a",
        agent_name=None,
        agent_prompt=None,
        livekit_call_id="c1",
        capture_audio=True,
        is_test=False,
        metadata={},
    )
    state._client = client
    assert state._audio is not None

    # Recording anchor at t=100. The clock reads t=500 throughout the turns to
    # prove the span is NOT derived from "now" (the commit instant).
    clk = {"t": 500.0}
    monkeypatch.setattr(session_mod.time, "time", lambda: clk["t"])
    state._recording_started_at = 100.0
    state._audio.add_stereo_pcm(b"\x00\x00\x00\x00")  # first_audio_observed → True
    state._wire_session_listeners()
    session = state._session

    # User voiced [100.2s, 101.0s] → [200ms, 1000ms]. No speaking-state event fired.
    session.fire("conversation_item_added", _ItemWithMetrics("user", "hi", 100.2, 101.0))  # type: ignore[attr-defined]
    # Agent voiced [102.0s, 104.5s] → [2000ms, 4500ms] (burst-delivered, irrelevant).
    session.fire("conversation_item_added", _ItemWithMetrics("assistant", "ok", 102.0, 104.5))  # type: ignore[attr-defined]

    await state.aflush(reason="agent-ended")
    transcript = posted[-1]["body"]["transcript"]

    user_turn, agent_turn = transcript[0], transcript[1]
    assert (user_turn["audioOffsetMs"], user_turn["endAudioOffsetMs"]) == (200, 1_000)
    assert (agent_turn["audioOffsetMs"], agent_turn["endAudioOffsetMs"]) == (2_000, 4_500)
    # Non-degenerate spans: start strictly before end (the "0-second timestamp" bug).
    assert user_turn["timestamp"] != user_turn["endTimestamp"]


@pytest.mark.asyncio
async def test_transcript_offsets_track_recording_wall_clock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fallback path (no per-utterance metrics, e.g. a realtime model without VAD):
    each turn's start is its ``speaking`` transition and its end is when the item
    was committed, both relative to recording start — the same clock RecorderIO
    aligns its audio to. A faster-than-real-time TTS burst can't collapse the span
    because the span is measured off these signals, not off frame delivery."""
    import roark_analytics_python_livekit.session as session_mod

    client, posted = _mocked_client()
    state = _RoarkSession(
        ctx=_StubCtx(),  # type: ignore[arg-type]
        session=_StubSession(),  # type: ignore[arg-type]
        api_key="rk_test",
        agent_id="a",
        agent_name=None,
        agent_prompt=None,
        livekit_call_id="c1",
        capture_audio=True,
        is_test=False,
        metadata={},
    )
    state._client = client
    assert state._audio is not None

    # Deterministic clock for the session's time.time() reads.
    clk = {"t": 100.0}
    monkeypatch.setattr(session_mod.time, "time", lambda: clk["t"])

    # Arm recording at t=100 and mark audio as observed (skip _wire_audio_taps,
    # which needs a real livekit session); wire only the event listeners.
    state._recording_started_at = 100.0
    state._audio.add_stereo_pcm(b"\x00\x00\x00\x00")  # first_audio_observed → True
    state._wire_session_listeners()
    session = state._session

    # User turn: speaking onset at t=100.0, committed at t=100.5 → [0ms, 500ms].
    clk["t"] = 100.0
    session.fire("user_state_changed", _StateEvent("speaking"))  # type: ignore[attr-defined]
    clk["t"] = 100.5
    session.fire("conversation_item_added", _Item("user", "Hi, checking my order"))  # type: ignore[attr-defined]

    # Agent turn: a real 1.0s gap, speaking onset at t=101.0, committed at t=103.0
    # → [1000ms, 3000ms], even though the TTS frames may have arrived in a burst.
    clk["t"] = 101.0
    session.fire("agent_state_changed", _StateEvent("speaking"))  # type: ignore[attr-defined]
    clk["t"] = 103.0
    session.fire("conversation_item_added", _Item("assistant", "Sure, let me check"))  # type: ignore[attr-defined]

    await state.aflush(reason="agent-ended")
    transcript = posted[-1]["body"]["transcript"]

    user_turn, agent_turn = transcript[0], transcript[1]
    assert user_turn["audioOffsetMs"] == 0
    assert user_turn["endAudioOffsetMs"] == 500
    assert agent_turn["audioOffsetMs"] == 1_000
    assert agent_turn["endAudioOffsetMs"] == 3_000

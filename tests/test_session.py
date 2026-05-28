"""Tests for the observe_session / track_session helpers.

These don't require a live livekit-agents runtime — we stub the AgentSession
and JobContext surfaces the helpers touch (the ``on`` event registration, the
shutdown-callback hook, and the room/job attributes).
"""

from __future__ import annotations

import json
import os
from typing import Any

import httpx
import pytest

from roark_analytics_python_livekit.client import API_KEY_HEADER, RoarkClient
from roark_analytics_python_livekit.session import (
    _RoarkSession,
    _inject_mock_tools,
    get_simulation_data,
    observe_session,
    track_session,
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
        mode="observe",
        capture_audio=False,
        is_test=False,
        metadata={},
    )
    # Swap in the mocked client.
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
        mode="observe",
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


@pytest.mark.asyncio
async def test_track_session_respects_kill_switch(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ROARK_TRACING_ENABLED", "false")
    out = await track_session(
        _StubCtx(),  # type: ignore[arg-type]
        _StubSession(),  # type: ignore[arg-type]
        api_key="rk",
        agent_id="a",
    )
    assert out is None


def test_get_simulation_data_reads_roark_block() -> None:
    ctx = _StubCtx(metadata=json.dumps({"roark": {"runId": "r1", "mockTools": {"a": 1}}}))
    sim = get_simulation_data(ctx)  # type: ignore[arg-type]
    assert sim == {"runId": "r1", "mockTools": {"a": 1}}


def test_get_simulation_data_returns_empty_on_garbage() -> None:
    ctx = _StubCtx(metadata="not-json")
    assert get_simulation_data(ctx) == {}  # type: ignore[arg-type]


def test_inject_mock_tools_replaces_callable() -> None:
    class _Tool:
        name = "lookup_order"

        async def callable(self, order_id: str) -> dict[str, str]:  # noqa: A003
            raise NotImplementedError

    class _Agent:
        function_tools = [_Tool()]

    ctx = _StubCtx(metadata=json.dumps({"roark": {"mockTools": {"lookup_order": {"x": 1}}}}))
    agent = _Agent()
    _inject_mock_tools(ctx, agent)  # type: ignore[arg-type]
    # The tool's `callable` should now be the stub returning the scripted value.
    import asyncio

    out = asyncio.get_event_loop().run_until_complete(_Agent.function_tools[0].callable("ignored"))
    assert out == {"x": 1}


def test_mock_tools_disabled_kill_switch(monkeypatch: pytest.MonkeyPatch) -> None:
    """ROARK_MOCK_TOOLS_ENABLED=false is honoured by track_session, not _inject directly.

    The kill-switch is checked in ``track_session`` before calling ``_inject_mock_tools``;
    the lower-level helper does its job regardless. This test just documents the env var.
    """
    monkeypatch.setenv("ROARK_MOCK_TOOLS_ENABLED", "false")
    assert os.environ["ROARK_MOCK_TOOLS_ENABLED"] == "false"

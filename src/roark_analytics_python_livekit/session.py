"""``observe_session`` — wire an ``AgentSession`` to Roark.

``observe_session(ctx, session, ...)`` (production):
    Registers listeners on the LiveKit ``AgentSession`` for transcripts, tool
    calls, and metrics; subscribes to room audio for stereo recording; POSTs
    ``call-started`` immediately and ``call-ended`` on ``ctx.shutdown_callback``.

Failures are logged and swallowed — Roark must never break the agent.
Kill-switch env var:

* ``ROARK_OBSERVABILITY_ENABLED=false`` — disable ``observe_session`` outright.
"""

from __future__ import annotations

import contextlib
import inspect
import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Literal

from ._types import (
    CallEndedPayload,
    CallStartedPayload,
    MetricMessage,
    ToolCallMessage,
    ToolResultMessage,
    TranscriptMessage,
)
from .audio import AudioCapture, _downmix_to_mono
from .client import RoarkClient

if TYPE_CHECKING:  # pragma: no cover
    from livekit.agents import AgentSession, JobContext

log = logging.getLogger("roark_analytics_python_livekit.session")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _env_disabled(name: str) -> bool:
    """Read ``name`` from the env; treat ``false`` / ``0`` / ``no`` as off."""
    value = os.environ.get(name)
    if value is None:
        return False
    return value.strip().lower() in {"false", "0", "no", "off"}


def _to_json_string(value: object) -> str:
    """Best-effort stringify for tool arguments / results. Never raises."""
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    try:
        return json.dumps(value, separators=(",", ":"), default=str)
    except (TypeError, ValueError):
        return str(value)


class _RoarkSession:
    """Internal state for one observed agent session.

    Public callers use ``observe_session`` — it constructs one of these and
    registers it on the supplied ``AgentSession`` / ``JobContext``.
    """

    def __init__(
        self,
        *,
        ctx: JobContext,
        session: AgentSession,
        api_key: str,
        agent_id: str,
        agent_name: str | None,
        agent_prompt: str | None,
        livekit_call_id: str | None,
        capture_audio: bool,
        is_test: bool,
        metadata: dict[str, Any],
    ) -> None:
        self._ctx = ctx
        self._session = session
        self._metadata = metadata

        self._client = RoarkClient(api_key=api_key)
        self._agent_id = agent_id
        self._agent_name = agent_name
        self._agent_prompt = agent_prompt
        self._is_test = is_test
        self._livekit_call_id = livekit_call_id or str(uuid.uuid4())

        self._transcript: list[TranscriptMessage] = []
        self._tool_calls: list[ToolCallMessage | ToolResultMessage] = []
        self._metrics: list[MetricMessage] = []
        self._call_started_iso: str | None = None
        self._first_speaker: Literal["assistant", "user"] | None = None
        self._end_flushed = False

        # Audio capture is optional — disabling it lets a deployer keep call rows
        # / transcripts without paying the chunked-upload bandwidth.
        self._audio = (
            AudioCapture(client=self._client, livekit_call_id=self._livekit_call_id)
            if capture_audio
            else None
        )

        # Anchor for ``audioOffsetMs`` on transcript/tool/metric records — set at
        # first observed audio frame so offsets align with the recording's WAV
        # sample 0, not wall clock. ``None`` until the first frame arrives.
        self._recording_anchor_monotonic: float | None = None

    @property
    def livekit_call_id(self) -> str:
        return self._livekit_call_id

    async def start(self) -> None:
        """POST ``call-started``, register session/room listeners, arm audio."""
        self._call_started_iso = _utc_now_iso()
        payload: CallStartedPayload = {
            "event": "call-started",
            "livekitCallId": self._livekit_call_id,
            "eventTimestamp": self._call_started_iso,
            "agentId": self._agent_id,
            "isTest": self._is_test,
        }
        if self._agent_name:
            payload["agentName"] = self._agent_name
        if self._agent_prompt:
            payload["agentPrompt"] = self._agent_prompt
        # Best-effort room / job metadata for the Roark side. ``roomSid`` is the
        # server-assigned ``RM_…`` id (present on self-hosted *and* Cloud) that
        # the OpenTelemetry tracing integration keys on as ``livekit.room.id`` —
        # capturing it here lets Roark link OTel traces to this call.
        # Console mode exposes these as mock objects, and on current livekit-rtc
        # ``room.sid`` is an *async* property (it awaits the server-assigned id),
        # so resolve awaitables and keep only real, non-empty strings — a
        # non-serializable or coroutine value must never reach json.dumps.
        with contextlib.suppress(Exception):
            job_id = getattr(self._ctx.job, "id", "")
            if isinstance(job_id, str) and job_id:
                payload["jobId"] = job_id
        with contextlib.suppress(Exception):
            room = self._ctx.room
            room_name = getattr(room, "name", "")
            if isinstance(room_name, str) and room_name:
                payload["roomName"] = room_name
            room_sid: Any = getattr(room, "sid", "")
            if inspect.isawaitable(room_sid):
                room_sid = await room_sid
            if isinstance(room_sid, str) and room_sid:
                payload["roomSid"] = room_sid

        log.info(
            "call-started: livekitCallId=%s agentId=%s",
            self._livekit_call_id,
            self._agent_id,
        )
        await self._client.post_call_started(payload)

        self._wire_session_listeners()
        self._wire_audio_taps()
        self._wire_shutdown_callback()

    async def aflush(self, *, reason: str = "agent-ended") -> None:
        """Idempotently flush pending state and POST ``call-ended``."""
        if self._end_flushed:
            return
        self._end_flushed = True

        # Drain audio first — chunks are uploaded async, and call-ended carries the
        # recording metadata so Roark knows whether to look for chunks.
        if self._audio is not None:
            await self._audio.aflush()

        ended_iso = _utc_now_iso()
        payload: CallEndedPayload = {
            "event": "call-ended",
            "livekitCallId": self._livekit_call_id,
            "eventTimestamp": ended_iso,
            "callStartedAt": self._call_started_iso,
            "callEndedAt": ended_iso,
            "callEndedReason": reason,
        }
        if self._first_speaker is not None:
            payload["agentSpokeFirst"] = self._first_speaker == "assistant"
        # Only advertise a recording when chunks actually reached S3. Gating on
        # chunk_index (chunks *enqueued*) would tell Roark to merge a recording
        # that never uploaded — e.g. when every chunk-upload-url request 502s.
        if self._audio is not None and self._audio.uploaded_count > 0:
            payload["recordingSampleRate"] = self._audio.sample_rate
            payload["recordingNumChannels"] = self._audio.num_channels
        if self._transcript:
            payload["transcript"] = list(self._transcript)
        if self._tool_calls:
            payload["toolCalls"] = list(self._tool_calls)
        if self._metrics:
            payload["metrics"] = list(self._metrics)

        log.info(
            "call-ended: livekitCallId=%s reason=%s transcript=%d toolCalls=%d "
            "metrics=%d chunks=%d/%d uploaded",
            self._livekit_call_id,
            reason,
            len(self._transcript),
            len(self._tool_calls),
            len(self._metrics),
            self._audio.uploaded_count if self._audio is not None else 0,
            self._audio.chunk_index if self._audio is not None else 0,
        )
        await self._client.post_call_ended(payload)
        await self._client.aclose()

    def _wire_session_listeners(self) -> None:
        """Hook the AgentSession event surface.

        AgentSession exposes a Pipecat-style ``on(event, callback)`` API. The
        callbacks may be sync or async — we wrap async work in tasks so the
        listener returns control to the session immediately.
        """
        session = self._session

        def on_conversation_item_added(ev: Any) -> None:
            try:
                self._handle_conversation_item(getattr(ev, "item", ev))
            except Exception as err:  # defensive — never raise into the session
                log.warning("conversation_item_added handler failed: %r", err)

        def on_function_tools_executed(ev: Any) -> None:
            try:
                self._handle_function_tools_executed(ev)
            except Exception as err:
                log.warning("function_tools_executed handler failed: %r", err)

        def on_metrics_collected(ev: Any) -> None:
            try:
                self._handle_metrics(getattr(ev, "metrics", ev))
            except Exception as err:
                log.warning("metrics_collected handler failed: %r", err)

        def on_agent_state_changed(ev: Any) -> None:
            try:
                new_state = getattr(ev, "new_state", None)
                # First speaker — recorded once the agent first transitions into
                # speaking before the user has been observed.
                if new_state == "speaking" and self._first_speaker is None:
                    self._first_speaker = "assistant"
            except Exception as err:
                log.warning("agent_state_changed handler failed: %r", err)

        with contextlib.suppress(Exception):
            session.on("conversation_item_added", on_conversation_item_added)
        with contextlib.suppress(Exception):
            session.on("function_tools_executed", on_function_tools_executed)
        with contextlib.suppress(Exception):
            session.on("metrics_collected", on_metrics_collected)
        with contextlib.suppress(Exception):
            session.on("agent_state_changed", on_agent_state_changed)

    def _wire_audio_taps(self) -> None:
        """Tap the AgentSession's own audio I/O for the stereo recording.

        We intercept ``session.input.audio`` (the user mic / inbound track) and
        ``session.output.audio`` (the agent's post-TTS audio) rather than
        subscribing to room tracks. This is mode-agnostic: it captures audio
        identically whether the agent runs against a real LiveKit room
        (``python agent.py dev``) or the local CLI (``python agent.py console``,
        which has no room and so never fires ``track_subscribed``).

        ``observe_session`` runs *before* ``session.start()``, and the session
        only assigns ``input.audio`` / ``output.audio`` during ``start()`` (then
        immediately hands ``input.audio`` to a forwarding task that captures the
        reference once). So we can't wrap the streams after the fact — instead we
        intercept the ``audio`` setters now, so whatever the session assigns gets
        transparently wrapped at assignment time, before the forwarding task
        reads it.
        """
        if self._audio is None:
            return
        try:
            from livekit.agents.voice import io as lk_io  # type: ignore[import-not-found]
        except Exception as err:  # pragma: no cover — livekit not installed in tests
            log.warning("livekit audio io import failed; audio capture disabled: %r", err)
            self._audio = None
            return

        session = self._session
        agent_input = getattr(session, "input", None)
        agent_output = getattr(session, "output", None)
        if agent_input is None or agent_output is None:
            log.warning("session has no input/output; audio capture disabled")
            self._audio = None
            return

        observer = self

        class _UserAudioTap(lk_io.AudioInput):
            """Pass-through AudioInput that copies each user frame into the mixer."""

            def __init__(self, source: Any) -> None:
                super().__init__(label="roark-user-tap", source=source)

            async def __anext__(self) -> Any:
                frame = await super().__anext__()
                observer._on_audio_frame(frame, channel=0)
                return frame

        def _wrap_input(stream: Any) -> Any:
            if stream is None or isinstance(stream, _UserAudioTap):
                return stream
            log.info("tapping user audio input (%r)", getattr(stream, "label", "?"))
            return _UserAudioTap(stream)

        def _wrap_output(sink: Any) -> Any:
            # The agent output sink is consumed via ``capture_frame`` (a regular
            # method, so instance-level patching is honoured). Wrapping the method
            # in place avoids re-implementing the AudioOutput ABC / event wiring.
            if sink is None or getattr(sink, "_roark_tapped", False):
                return sink
            original = sink.capture_frame

            async def _tapped(frame: Any, _orig: Any = original) -> Any:
                observer._on_audio_frame(frame, channel=1)
                return await _orig(frame)

            with contextlib.suppress(Exception):
                sink.capture_frame = _tapped  # type: ignore[method-assign]
                sink._roark_tapped = True  # type: ignore[attr-defined]
                log.info("tapping agent audio output (%r)", getattr(sink, "label", "?"))
            return sink

        self._install_audio_setter(agent_input, _wrap_input)
        self._install_audio_setter(agent_output, _wrap_output)

    @staticmethod
    def _install_audio_setter(agent_io: Any, wrap: Any) -> None:
        """Make ``agent_io.audio = x`` transparently store ``wrap(x)`` instead.

        Wraps the current value (if already set) and reassigns ``agent_io``'s
        class to a subclass whose ``audio`` setter runs ``wrap`` first, so future
        assignments by RoomIO / the console are wrapped too. Best-effort: any
        failure leaves the session untouched.
        """
        try:
            cls = type(agent_io)
            prop = cls.audio

            with contextlib.suppress(Exception):
                current = prop.fget(agent_io)
                if current is not None:
                    wrapped = wrap(current)
                    if wrapped is not current:
                        prop.fset(agent_io, wrapped)

            def _setter(self_io: Any, value: Any, _prop: Any = prop, _wrap: Any = wrap) -> None:
                _prop.fset(self_io, _wrap(value) if value is not None else None)

            intercepted = type(
                f"_RoarkTapped{cls.__name__}",
                (cls,),
                {"audio": property(prop.fget, _setter)},
            )
            agent_io.__class__ = intercepted
        except Exception as err:
            log.warning("failed to install audio tap on %r: %r", type(agent_io).__name__, err)

    def _on_audio_frame(self, frame: Any, *, channel: int) -> None:
        """Feed one tapped frame (user=0 / agent=1) into the stereo mixer."""
        if self._audio is None:
            return
        try:
            self._anchor_recording_clock()
            pcm = bytes(getattr(frame, "data", b""))
            if not pcm:
                return
            sample_rate = int(getattr(frame, "sample_rate", self._audio.sample_rate))
            num_channels = int(getattr(frame, "num_channels", 1))
            if num_channels > 1:
                pcm = _downmix_to_mono(pcm, num_channels)
            if channel == 0:
                self._audio.add_user_frame(pcm, sample_rate=sample_rate)
            else:
                self._audio.add_agent_frame(pcm, sample_rate=sample_rate)
        except Exception as err:  # pragma: no cover — never raise into the session
            log.warning("audio frame tap failed (channel=%d): %r", channel, err)

    def _wire_shutdown_callback(self) -> None:
        """Register call-ended on the JobContext's shutdown hook."""
        try:
            register = getattr(self._ctx, "add_shutdown_callback", None)
            if register is None:
                # Older livekit-agents versions use ``shutdown_callback`` as a property.
                register = lambda cb: setattr(self._ctx, "shutdown_callback", cb)  # noqa: E731

            async def _on_shutdown() -> None:
                await self.aflush(reason="agent-ended")

            register(_on_shutdown)
        except Exception as err:
            log.warning("failed to register shutdown callback: %r", err)

    def _handle_conversation_item(self, item: Any) -> None:
        """Translate one ``ChatMessage`` into a Roark transcript entry."""
        role = str(getattr(item, "role", "")).lower()
        if role not in {"user", "assistant", "system"}:
            return
        content = getattr(item, "content", None)
        if isinstance(content, list):
            # LiveKit ChatMessage.content can be a list of segments (string mixed
            # with multimodal parts); concat the string ones.
            text = "".join(part for part in content if isinstance(part, str)).strip()
        else:
            text = str(content or "").strip()
        if not text:
            return

        now_iso = _utc_now_iso()
        entry: TranscriptMessage = {
            "role": role,  # type: ignore[typeddict-item]
            "content": text,
            "timestamp": now_iso,
            "endTimestamp": now_iso,
        }
        offset = self._current_audio_offset_ms()
        if offset is not None:
            entry["audioOffsetMs"] = offset
            entry["endAudioOffsetMs"] = offset
        if self._first_speaker is None and role in {"user", "assistant"}:
            self._first_speaker = "assistant" if role == "assistant" else "user"
        self._transcript.append(entry)

    def _handle_function_tools_executed(self, ev: Any) -> None:
        """Translate a function-tools batch into tool_call + tool_result records."""
        # livekit-agents passes ``zip()`` of called functions + results in the event.
        # API has varied across versions — accept both ``called_functions`` and
        # ``function_calls`` and pair by ``call_id`` / ``tool_call_id``.
        calls = (
            getattr(ev, "called_functions", None)
            or getattr(ev, "function_calls", None)
            or getattr(ev, "items", None)
            or []
        )
        for call in calls:
            tool_call_id = str(
                getattr(call, "tool_call_id", None) or getattr(call, "call_id", None) or ""
            )
            name = str(getattr(call, "name", None) or getattr(call, "function_name", "") or "")
            arguments = (
                getattr(call, "arguments", None)
                or getattr(call, "raw_arguments", None)
                or getattr(call, "args", None)
            )
            result = (
                getattr(call, "result", None)
                or getattr(call, "output", None)
                or getattr(call, "return_value", None)
            )

            offset = self._current_audio_offset_ms()
            now_iso = _utc_now_iso()

            invocation: ToolCallMessage = {
                "kind": "tool_call",
                "toolCallId": tool_call_id,
                "name": name,
                "arguments": _to_json_string(arguments) or "{}",
                "timestamp": now_iso,
            }
            if offset is not None:
                invocation["audioOffsetMs"] = offset
            self._tool_calls.append(invocation)

            if result is not None:
                tool_result: ToolResultMessage = {
                    "kind": "tool_result",
                    "toolCallId": tool_call_id,
                    "content": _to_json_string(result),
                    "timestamp": now_iso,
                }
                if offset is not None:
                    tool_result["audioOffsetMs"] = offset
                self._tool_calls.append(tool_result)

    def _handle_metrics(self, metrics: Any) -> None:
        """Translate a livekit metrics event into a MetricMessage."""
        # ``metrics_collected`` emits a single metric instance per fire — kind is
        # inferred from the class name (EOUMetrics / STTMetrics / LLMMetrics / …).
        cls_name = type(metrics).__name__.lower()
        kind: Literal["eou", "stt", "llm", "tts", "agent"]
        if "eou" in cls_name or "endofutterance" in cls_name:
            kind = "eou"
        elif "stt" in cls_name:
            kind = "stt"
        elif "llm" in cls_name:
            kind = "llm"
        elif "tts" in cls_name:
            kind = "tts"
        else:
            kind = "agent"

        offset = self._current_audio_offset_ms()
        entry: MetricMessage = {
            "kind": kind,
            "timestamp": _utc_now_iso(),
        }
        if offset is not None:
            entry["audioOffsetMs"] = offset

        # Forward every public, JSON-encodable scalar field. The Roark backend
        # reads the ones it understands and stores the rest as JSON.
        extra: dict[str, Any] = {}
        for attr in dir(metrics):
            if attr.startswith("_"):
                continue
            try:
                value = getattr(metrics, attr)
            except Exception:
                continue
            if callable(value):
                continue
            # ``bool`` is an ``int`` subclass, so exclude it here and handle it in
            # the ``elif`` below — otherwise ``True``/``False`` land in numeric slots.
            if isinstance(value, (int, float, str)) and not isinstance(value, bool):
                # Map known field names onto the typed slots; unknown scalars fall
                # through into ``extra``.
                if attr in (
                    "end_of_utterance_delay",
                    "endOfUtteranceDelay",
                    "transcription_delay",
                    "transcriptionDelay",
                    "on_conversation_item_added_delay",
                    "onConversationItemAddedDelay",
                    "ttft",
                    "duration",
                    "ttfb",
                    "audio_duration",
                    "audioDuration",
                    "prompt_tokens",
                    "promptTokens",
                    "completion_tokens",
                    "completionTokens",
                    "total_tokens",
                    "totalTokens",
                    "cached_tokens",
                    "cachedTokens",
                    "tokens_per_second",
                    "tokensPerSecond",
                ):
                    camel = _snake_to_camel(attr)
                    entry[camel] = value  # type: ignore[literal-required]
                else:
                    extra[attr] = value
            elif isinstance(value, bool):
                if attr in ("streamed",):
                    entry["streamed"] = value
                else:
                    extra[attr] = value
        if extra:
            entry["extra"] = extra
        self._metrics.append(entry)

    def _anchor_recording_clock(self) -> None:
        if self._recording_anchor_monotonic is None:
            self._recording_anchor_monotonic = time.monotonic()

    def _current_audio_offset_ms(self) -> int | None:
        anchor = self._recording_anchor_monotonic
        if anchor is None:
            return None
        return max(0, round((time.monotonic() - anchor) * 1000))


def _snake_to_camel(name: str) -> str:
    if "_" not in name:
        return name
    head, *rest = name.split("_")
    return head + "".join(p.capitalize() for p in rest)


async def observe_session(
    ctx: JobContext,
    session: AgentSession,
    *,
    api_key: str,
    agent_id: str,
    agent_name: str | None = None,
    agent_prompt: str | None = None,
    livekit_call_id: str | None = None,
    capture_audio: bool = True,
    capture_logs: bool = True,  # noqa: ARG001 — reserved for future log streaming
    is_test: bool = False,
    **metadata: Any,
) -> _RoarkSession | None:
    """Capture a production LiveKit Agents session and ship it to Roark.

    Args:
        ctx: The ``JobContext`` passed to your agent entrypoint.
        session: The ``AgentSession`` you're about to ``start``.
        api_key: Roark API key (``rk_live_...``).
        agent_id: Customer-stable agent identifier. Roark lazy-registers the
            agent the first time it sees this id.
        agent_name: Display name on the Roark dashboard.
        agent_prompt: System prompt; persisted as the agent's prompt revision
            so changes are tracked over time.
        livekit_call_id: Optional stable identifier (defaults to ``ctx.job.id``
            if available, else a random UUID). Sent on every Roark record.
        capture_audio: Set to ``False`` to skip stereo audio capture (saves
            bandwidth; transcripts and tool data still ship).
        capture_logs: Reserved for future log streaming.
        is_test: Tag the call as a test on the Roark dashboard. Defaults to
            ``False`` (production traffic).
        **metadata: Free-form metadata for Roark-side correlation (passed
            through to call-started).

    Returns:
        The active ``_RoarkSession`` (kept alive by the registered listeners +
        shutdown callback). ``None`` when ``ROARK_OBSERVABILITY_ENABLED=false``.

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
            )
            await session.start(room=ctx.room, agent=Assistant())
    """
    if _env_disabled("ROARK_OBSERVABILITY_ENABLED"):
        log.info("observe_session: disabled via ROARK_OBSERVABILITY_ENABLED=false")
        return None
    if livekit_call_id is None:
        livekit_call_id = _resolve_call_id(ctx)
    state = _RoarkSession(
        ctx=ctx,
        session=session,
        api_key=api_key,
        agent_id=agent_id,
        agent_name=agent_name,
        agent_prompt=agent_prompt,
        livekit_call_id=livekit_call_id,
        capture_audio=capture_audio,
        is_test=is_test,
        metadata=metadata,
    )
    await state.start()
    return state


def _resolve_call_id(ctx: JobContext) -> str:
    """Derive a stable call id from the job context, falling back to a UUID."""
    try:
        job_id = getattr(ctx.job, "id", None)
        if isinstance(job_id, str) and job_id:
            return job_id
    except Exception:
        pass
    return str(uuid.uuid4())


__all__ = ["observe_session"]

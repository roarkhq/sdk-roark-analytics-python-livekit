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

import asyncio
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
from .audio import AudioCapture
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

        # livekit-agents' RecorderIO (subclassed) does the stereo alignment; this
        # holds the instance so call-ended can close it (flushing the tail) and so
        # offsets can read its ``recording_started_at`` anchor. ``None`` until the
        # audio taps are wired (and stays ``None`` if audio capture is off).
        self._recorder: Any | None = None
        # Wall-clock (``time.time``) of when recording was armed; the fallback
        # anchor before the recorder has seen its first frame.
        self._recording_started_at: float | None = None
        # Wall-clock of the latest ``speaking`` transition per side. Fallback anchor
        # for a turn's start when the committed item carries no per-utterance metrics
        # (the recorder aligns its output to the same clock).
        self._user_speaking_at: float | None = None
        self._agent_speaking_at: float | None = None
        # Recording sample rate adopted from the negotiated agent output rate (the
        # rate the recorder resamples both channels to), resolved when the output
        # audio is wrapped. ``None`` until then → the AudioCapture default is used.
        self._resolved_sample_rate: int | None = None

        # Audio capture is optional — disabling it lets a deployer keep call rows
        # / transcripts without paying the chunked-upload bandwidth.
        self._audio = (
            AudioCapture(client=self._client, livekit_call_id=self._livekit_call_id)
            if capture_audio
            else None
        )

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

        # Close the recorder first so its encode thread flushes every remaining
        # aligned pair into the upload buffer, then drain that buffer's tail.
        # call-ended carries the recording metadata so Roark knows whether to look
        # for chunks.
        if self._recorder is not None:
            with contextlib.suppress(Exception):
                await self._recorder.aclose()
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
                if getattr(ev, "new_state", None) == "speaking":
                    # Stamp this agent turn's onset on the same wall clock the
                    # recorder aligns its output to, so the transcript span starts
                    # where the agent's audio actually begins on the waveform.
                    self._agent_speaking_at = time.time()
                    if self._first_speaker is None:
                        self._first_speaker = "assistant"
            except Exception as err:
                log.warning("agent_state_changed handler failed: %r", err)

        def on_user_state_changed(ev: Any) -> None:
            try:
                if getattr(ev, "new_state", None) == "speaking":
                    # Same for the user side: the VAD ``speaking`` transition marks
                    # the turn onset on the recording's wall clock.
                    self._user_speaking_at = time.time()
                    if self._first_speaker is None:
                        self._first_speaker = "user"
            except Exception as err:
                log.warning("user_state_changed handler failed: %r", err)

        with contextlib.suppress(Exception):
            session.on("conversation_item_added", on_conversation_item_added)
        with contextlib.suppress(Exception):
            session.on("function_tools_executed", on_function_tools_executed)
        with contextlib.suppress(Exception):
            session.on("metrics_collected", on_metrics_collected)
        with contextlib.suppress(Exception):
            session.on("agent_state_changed", on_agent_state_changed)
        with contextlib.suppress(Exception):
            session.on("user_state_changed", on_user_state_changed)

    def _wire_audio_taps(self) -> None:
        """Wire livekit-agents' ``RecorderIO`` onto the session's audio I/O.

        Rather than mixing audio ourselves, we let livekit-agents' own recorder
        do the channel alignment (it knows the agent's true playback position and
        splices real inter-turn silence) and only swap its file-encode step for
        chunked PCM upload — see ``RoarkRecorderIO``.

        The recorder wraps ``session.input.audio`` and ``session.output.audio``.
        Those are only assigned during ``session.start()`` (which runs *after*
        ``observe_session``), so we install setter interceptors now: whatever the
        session later assigns is handed to ``record_input`` / ``record_output`` at
        assignment time. The recorder is started once both sides are wrapped.

        This is mode-agnostic — it captures identically against a real LiveKit
        room (``agent.py dev``) and the local CLI (``agent.py console``).

        Note: livekit-agents records session audio to a local OGG by default. To
        avoid that redundant local recording, pass ``record=False`` (or
        ``record={"audio": False}``) to ``session.start()`` — Roark captures the
        audio itself here.
        """
        if self._audio is None:
            return
        try:
            from livekit.agents.voice.recorder_io import (  # type: ignore[import-not-found]
                RecorderAudioInput,
                RecorderAudioOutput,
            )

            from ._recorder import RoarkRecorderIO
        except Exception as err:  # pragma: no cover — livekit not installed in tests
            log.warning("livekit recorder import failed; audio capture disabled: %r", err)
            self._audio = None
            return

        session = self._session
        agent_input = getattr(session, "input", None)
        agent_output = getattr(session, "output", None)
        if agent_input is None or agent_output is None:
            log.warning("session has no input/output; audio capture disabled")
            self._audio = None
            return

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:  # pragma: no cover — observe_session is async, so a loop exists
            loop = None
        recorder = RoarkRecorderIO(
            agent_session=session,
            sink=self._audio.add_stereo_pcm,
            sample_rate=self._audio.sample_rate,
            loop=loop,
        )
        self._recorder = recorder

        def _maybe_start() -> None:
            # ``input.audio`` and ``output.audio`` are assigned independently during
            # session.start(); start the recorder only once both sides are wrapped.
            if recorder._in_record and recorder._out_record and not recorder.recording:
                # Adopt the negotiated rate as the recorder's resample target (and the
                # rate reported on call-ended). Must be set before start() — the encode
                # thread reads it. Falls back to the AudioCapture default when unknown.
                if self._resolved_sample_rate is not None:
                    recorder.set_target_sample_rate(self._resolved_sample_rate)
                    if self._audio is not None:
                        self._audio.set_sample_rate(self._resolved_sample_rate)
                self._recording_started_at = time.time()
                with contextlib.suppress(Exception):
                    asyncio.get_running_loop().create_task(recorder.start())

        def _wrap_input(stream: Any) -> Any:
            if stream is None or isinstance(stream, RecorderAudioInput):
                return stream
            with contextlib.suppress(Exception):
                wrapped = recorder.record_input(stream)
                log.info("recording user audio input (%r)", getattr(stream, "label", "?"))
                _maybe_start()
                return wrapped
            return stream

        def _wrap_output(sink: Any) -> Any:
            if sink is None or isinstance(sink, RecorderAudioOutput):
                return sink
            with contextlib.suppress(Exception):
                # The output sink advertises the negotiated agent audio rate; adopt it
                # so the recording isn't force-resampled to a fixed rate (dynamic rate,
                # provider-agnostic). ``None`` means "any rate" → keep the default.
                rate = getattr(sink, "sample_rate", None)
                if isinstance(rate, int) and rate > 0:
                    self._resolved_sample_rate = rate
                wrapped = recorder.record_output(sink)
                log.info("recording agent audio output (%r)", getattr(sink, "label", "?"))
                _maybe_start()
                return wrapped
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

        # Span the entry over the real spoken interval so it lands on the waveform.
        # The exact voice boundaries come from the committed item's metrics
        # (VAD/playback derived); see ``_utterance_span``. Falls back to ``None``
        # offsets when audio capture is off (nothing to align to).
        start_offset, end_offset = self._utterance_span(role, item)
        entry: TranscriptMessage = {
            "role": role,  # type: ignore[typeddict-item]
            "content": text,
            "timestamp": self._iso_at_offset(start_offset),
            "endTimestamp": self._iso_at_offset(end_offset),
        }
        if start_offset is not None and end_offset is not None:
            entry["audioOffsetMs"] = start_offset
            entry["endAudioOffsetMs"] = end_offset
        if self._first_speaker is None and role in {"user", "assistant"}:
            self._first_speaker = "assistant" if role == "assistant" else "user"
        self._transcript.append(entry)

    def _utterance_span(self, role: str, item: Any) -> tuple[int | None, int | None]:
        """(start, end) offsets in ms on the recording timeline for this turn.

        Prefers the *exact* voice boundaries LiveKit attaches to the committed
        item — ``item.metrics['started_speaking_at']`` / ``['stopped_speaking_at']``
        (VAD onset/offset for the user, real playback start/stop for the agent),
        all on the ``time.time`` clock the recorder anchors to. Falls back to the
        last ``speaking`` transition for the start and ``now`` for the end when an
        item carries no metrics (e.g. a realtime model without VAD). Returns
        ``(None, None)`` when audio capture is off or no audio has been observed.
        """
        anchor = self._recording_anchor_time()
        if self._audio is None or not self._audio.first_audio_observed or anchor is None:
            return None, None
        if role not in {"user", "assistant"}:
            return None, None
        started_at, stopped_at = self._voice_times_from_metrics(item)
        if started_at is None:
            started_at = self._agent_speaking_at if role == "assistant" else self._user_speaking_at
        now = time.time()
        end_at = stopped_at if stopped_at is not None else now
        start_at = started_at if started_at is not None else end_at
        start = max(0, round((start_at - anchor) * 1000))
        end = max(0, round((end_at - anchor) * 1000))
        if start > end:
            start = end
        return start, end

    @staticmethod
    def _voice_times_from_metrics(item: Any) -> tuple[float | None, float | None]:
        """Exact (started, stopped) speaking wall-clock times for a committed item.

        LiveKit attaches these to ``ChatMessage.metrics`` (a plain dict at runtime):
        VAD-derived for the user turn, playback-derived for the agent turn. Either
        may be absent depending on the provider / path, so both are optional.
        """
        metrics = getattr(item, "metrics", None)
        if not isinstance(metrics, dict):
            return None, None

        def _num(key: str) -> float | None:
            value = metrics.get(key)
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                return float(value)
            return None

        return _num("started_speaking_at"), _num("stopped_speaking_at")

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

    def _recording_anchor_time(self) -> float | None:
        """Wall-clock (``time.time``) of the recording's sample 0.

        Prefers the recorder's own ``recording_started_at`` (wall time of its
        first frame); falls back to when recording was armed. ``None`` before the
        recorder exists.
        """
        if self._recorder is not None:
            with contextlib.suppress(Exception):
                started = self._recorder.recording_started_at
                if started is not None:
                    return float(started)
        return self._recording_started_at

    def _current_audio_offset_ms(self) -> int | None:
        """Current position on the recording timeline (for tool/metric markers)."""
        anchor = self._recording_anchor_time()
        if self._audio is None or not self._audio.first_audio_observed or anchor is None:
            return None
        return max(0, round((time.time() - anchor) * 1000))

    def _iso_at_offset(self, offset_ms: int | None) -> str:
        """Project a recording offset back to an ISO wall-clock timestamp.

        Keeps the ISO ``timestamp`` fields on the same clock as ``audioOffsetMs``.
        Falls back to ``now`` when there's no recording to anchor against.
        """
        anchor = self._recording_anchor_time()
        if anchor is None or offset_ms is None:
            return _utc_now_iso()
        return datetime.fromtimestamp(anchor + offset_ms / 1000, tz=timezone.utc).isoformat()


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

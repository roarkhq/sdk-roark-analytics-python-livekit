"""Drive livekit-agents' ``RecorderIO`` but emit interleaved PCM instead of OGG.

``RoarkRecorderIO`` subclasses livekit-agents' ``RecorderIO`` so we inherit *all*
of its channel-alignment logic — the part that places each turn correctly on the
timeline, splices real silence between turns, trims to the agent's true
``playback_position`` (so a faster-than-real-time TTS burst keeps its real
duration), and pairs the user/agent channels in lockstep through its internal
write queues. None of that is reimplemented here.

The only thing we override is the terminal encode step: where the base class
resamples each matched ``(input, output)`` frame pair and muxes it into an OGG
file, we resample the same pair, interleave it into 16-bit stereo PCM, and hand
the bytes to :class:`~roark_analytics_python_livekit.audio.AudioCapture` for
chunked upload. The wire format Roark receives (raw interleaved PCM that the
server wraps in a WAV header) is unchanged.

This module imports livekit (and, transitively, ``av``/``numpy``) at import time,
so it must only be imported lazily — ``session.py`` does so inside the audio
wiring, after the rest of the integration has loaded.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import threading
from collections.abc import Callable

import numpy as np
from livekit import rtc
from livekit.agents.voice.recorder_io import RecorderIO

log = logging.getLogger("roark_analytics_python_livekit.recorder")


def _frames_to_mono_i16(frames: list[rtc.AudioFrame]) -> np.ndarray:
    """Concatenate resampled frames into one mono int16 array (averaging channels)."""
    if not frames:
        return np.zeros(0, dtype=np.int16)
    parts: list[np.ndarray] = []
    for f in frames:
        count = f.samples_per_channel * f.num_channels
        arr = np.frombuffer(f.data, dtype=np.int16, count=count)
        if f.num_channels > 1:
            arr = arr.reshape(-1, f.num_channels)
            arr = (arr.astype(np.int32).sum(axis=1) // f.num_channels).astype(np.int16)
        parts.append(arr)
    return np.concatenate(parts)


def _interleave_aligned(left: np.ndarray, right: np.ndarray) -> bytes:
    """Pad the shorter mono channel with *leading* silence, then interleave L,R.

    Leading silence (rather than trailing) keeps the pair *end*-aligned, matching
    the base ``RecorderIO`` encode step: within one matched write the two sides
    finish together, so a short agent segment lands at the end of the user's
    concurrently-captured window. Returns 16-bit signed-LE interleaved PCM.
    """
    n = max(len(left), len(right))
    if n == 0:
        return b""
    if len(left) < n:
        left = np.concatenate([np.zeros(n - len(left), dtype=np.int16), left])
    if len(right) < n:
        right = np.concatenate([np.zeros(n - len(right), dtype=np.int16), right])
    stereo = np.empty(n * 2, dtype=np.int16)
    stereo[0::2] = left
    stereo[1::2] = right
    return stereo.tobytes()


class RoarkRecorderIO(RecorderIO):
    """``RecorderIO`` whose encode step streams interleaved stereo PCM to a sink.

    ``sink`` receives ``bytes`` of L,R-interleaved 16-bit signed-LE PCM and is
    always invoked on ``loop`` (the encode runs on a worker thread, same as the
    base class, and hands results back via ``loop.call_soon_threadsafe``).
    """

    def __init__(
        self,
        *,
        agent_session: object,
        sink: Callable[[bytes], None],
        sample_rate: int = 48_000,
        loop: asyncio.AbstractEventLoop | None = None,
    ) -> None:
        super().__init__(agent_session=agent_session, sample_rate=sample_rate, loop=loop)  # type: ignore[arg-type]
        self._sink = sink

    def set_target_sample_rate(self, sample_rate: int) -> None:
        """Set the resample target rate. Must be called before ``start()`` — the
        encode thread reads ``self._sample_rate`` once, when it builds the
        resamplers. Lets the session adopt the negotiated rate (dynamic rate)
        instead of the fixed default."""
        if sample_rate > 0:
            self._sample_rate = sample_rate

    async def start(self) -> None:  # type: ignore[override]
        """Arm recording + spawn the forward task and PCM encode thread.

        Mirrors ``RecorderIO.start`` but needs no output path (we stream PCM
        instead of writing a file).
        """
        async with self._lock:
            if self._started:
                return
            if not self._in_record or not self._out_record:
                raise RuntimeError(
                    "RoarkRecorderIO not initialized: record_input() and record_output() "
                    "must both be called before start()."
                )
            self._started = True
            self._skip_padding_warning = False
            self._close_fut = self._loop.create_future()
            self._forward_atask = asyncio.create_task(self._forward_task())
            threading.Thread(
                target=self._encode_thread, daemon=True, name="roark_recorder_encode_thread"
            ).start()

    def _encode_thread(self) -> None:  # type: ignore[override]
        """Consume aligned ``(input, output)`` frame pairs → interleaved PCM → sink.

        The pairing and silence-alignment are produced by the base class
        (``_write_cb`` / ``_forward_task`` push matched buffers onto the queues);
        here we only resample each side, pad the shorter channel with leading
        silence to keep the pair sample-aligned, interleave, and emit.
        """
        target = self._sample_rate
        in_resampler: rtc.AudioResampler | None = None
        out_resampler: rtc.AudioResampler | None = None

        while True:
            input_buf = self._in_q.get()
            output_buf = self._out_q.get()
            if input_buf is None or output_buf is None:
                break

            if in_resampler is None and input_buf:
                in_resampler = rtc.AudioResampler(
                    input_rate=input_buf[0].sample_rate,
                    output_rate=target,
                    num_channels=input_buf[0].num_channels,
                )
            if out_resampler is None and output_buf:
                out_resampler = rtc.AudioResampler(
                    input_rate=output_buf[0].sample_rate,
                    output_rate=target,
                    num_channels=output_buf[0].num_channels,
                )

            input_resampled: list[rtc.AudioFrame] = []
            for frame in input_buf:
                assert in_resampler is not None
                input_resampled.extend(in_resampler.push(frame))

            output_resampled: list[rtc.AudioFrame] = []
            for frame in output_buf:
                assert out_resampler is not None
                output_resampled.extend(out_resampler.push(frame))
            if output_buf:
                assert out_resampler is not None
                # Output is sent per playback segment — flush so the segment's tail
                # samples aren't held back into the next pair (matches the base class).
                output_resampled.extend(out_resampler.flush())

            left = _frames_to_mono_i16(input_resampled)
            right = _frames_to_mono_i16(output_resampled)
            pcm = _interleave_aligned(left, right)
            if pcm:
                self._loop.call_soon_threadsafe(self._sink, pcm)

        with contextlib.suppress(RuntimeError):
            self._loop.call_soon_threadsafe(self._close_fut.set_result, None)

"""Stereo PCM capture + chunked upload for a LiveKit Agents session.

Pipecat exposes an ``AudioBufferProcessor`` that already produces stereo PCM —
no equivalent ships with livekit-agents, so this module rolls its own. It
subscribes to two audio sources:

* The remote participant track (the human user).
* The agent's own published audio track (post-TTS).

Frames from both sides are resampled to a common rate, mixed into a stereo
buffer (L=user, R=agent) on a wall-clock timeline, and flushed to Roark as
~256 KB chunks via presigned S3 PUTs (same chunk-upload-url contract as
``pipecat-roark``).

Failures are logged and swallowed — the capture never raises into the
surrounding session.
"""

from __future__ import annotations

import asyncio
import logging
import struct
import time

from .client import RoarkClient

log = logging.getLogger("roark_analytics_python_livekit.audio")


# Defaults --------------------------------------------------------------------

# 24 kHz stereo 16-bit ⇒ ~94 KB/s per channel; a 256 KB chunk flushes ~2.7s of audio.
# That's a good trade-off between upload frequency (latency on call-ended) and the
# per-chunk presigned-URL round trip cost. Matches pipecat-roark's default.
DEFAULT_CHUNK_BYTES = 256 * 1024

# 24 kHz is LiveKit Agents' default TTS sample rate and a common STT rate. Picking
# this avoids the worst-case quality loss from downsampling 48 kHz capture to 8 kHz.
# If the negotiated rate of either side differs, we resample to this target.
DEFAULT_SAMPLE_RATE = 24_000

# 16-bit signed little-endian PCM. Two channels, ⇒ 4 bytes per sample frame.
PCM_BYTES_PER_SAMPLE = 2
NUM_CHANNELS = 2
BYTES_PER_STEREO_FRAME = PCM_BYTES_PER_SAMPLE * NUM_CHANNELS

# Per-chunk upload retries. A chunk upload is two hops (presign + S3 PUT), and a
# transient gateway hiccup on either (e.g. a CloudFront 502 from the Roark API)
# would otherwise drop that chunk permanently — leaving Roark with nothing to
# merge. Retry the whole presign+PUT a few times with linear backoff. Presigned
# URLs are one-shot, so each retry re-requests a fresh URL.
MAX_UPLOAD_ATTEMPTS = 3
UPLOAD_RETRY_BACKOFF_SECONDS = 0.5


def _downmix_to_mono(pcm: bytes, num_channels: int) -> bytes:
    """Average interleaved 16-bit signed-LE channels down to a single mono lane.

    LiveKit audio frames are usually mono, but a device or track can deliver
    multiple interleaved channels. The mixer works in mono per side, so collapse
    anything wider before it reaches ``StereoMixer.add_mono``.
    """
    if num_channels <= 1 or not pcm:
        return pcm
    samples = struct.unpack(f"<{len(pcm) // PCM_BYTES_PER_SAMPLE}h", pcm)
    frame_count = len(samples) // num_channels
    out = [
        sum(samples[i * num_channels : (i + 1) * num_channels]) // num_channels
        for i in range(frame_count)
    ]
    return struct.pack(f"<{len(out)}h", *out)


def _resample_linear(pcm: bytes, src_rate: int, dst_rate: int) -> bytes:
    """Naive linear interpolation resampler for 16-bit signed-LE mono PCM.

    livekit-rtc ships its own ``AudioResampler``, but it isn't always importable
    in test environments — this is a pure-Python fallback. Linear interpolation
    is good enough for voice-quality observability audio; if we ever ship audio
    back through the pipeline, swap in livekit-rtc's resampler.
    """
    if src_rate == dst_rate or not pcm:
        return pcm
    samples = struct.unpack(f"<{len(pcm) // PCM_BYTES_PER_SAMPLE}h", pcm)
    if len(samples) <= 1:
        return pcm
    ratio = dst_rate / src_rate
    out_count = max(1, int(len(samples) * ratio))
    out = []
    for i in range(out_count):
        src_pos = i / ratio
        i0 = int(src_pos)
        i1 = min(i0 + 1, len(samples) - 1)
        frac = src_pos - i0
        out.append(int(samples[i0] * (1 - frac) + samples[i1] * frac))
    return struct.pack(f"<{len(out)}h", *out)


class StereoMixer:
    """Maintain a stereo PCM buffer mixing user (L) and agent (R) frames.

    Frames arrive at independent wall-clock times. We anchor the buffer to the
    timestamp of the first frame (from either side), then each incoming chunk
    is placed at its arrival time relative to that anchor — gaps become
    silence, overlaps are summed (saturating to int16 range).

    Note: this is a best-effort observability mixer, not a sample-accurate
    timeline. It assumes near-realtime delivery (no out-of-order chunks from
    livekit-rtc, which is true for AudioStream). Per-track jitter buffers
    inside livekit-rtc smooth out RTP jitter before frames reach us.
    """

    def __init__(self, *, sample_rate: int = DEFAULT_SAMPLE_RATE) -> None:
        self.sample_rate = sample_rate
        # Single growable stereo PCM buffer. Indexed in frames (one frame = NUM_CHANNELS
        # samples = 4 bytes). Grows as new audio arrives; the chunk uploader drains
        # whole chunks off the head.
        self._buffer = bytearray()
        # Total stereo frames already drained out via take_chunk (so callers see one
        # continuous timeline across chunks).
        self._drained_frames = 0
        self._anchor_monotonic: float | None = None

    def _now_frames(self) -> int:
        """Convert the current monotonic clock into a stereo-frame offset."""
        now = time.monotonic()
        if self._anchor_monotonic is None:
            self._anchor_monotonic = now
            return 0
        return int((now - self._anchor_monotonic) * self.sample_rate)

    def _ensure_capacity_frames(self, end_frame: int) -> None:
        """Extend the in-memory buffer with silence up to ``end_frame``."""
        needed_bytes = (end_frame - self._drained_frames) * BYTES_PER_STEREO_FRAME
        if needed_bytes > len(self._buffer):
            self._buffer.extend(b"\x00" * (needed_bytes - len(self._buffer)))

    def add_mono(
        self,
        *,
        channel: int,
        pcm: bytes,
        sample_rate: int,
    ) -> None:
        """Append mono PCM into the L (0) or R (1) lane.

        Args:
            channel: 0 = user / left, 1 = agent / right.
            pcm: 16-bit signed little-endian mono PCM samples.
            sample_rate: Source sample rate; resampled to ``self.sample_rate`` if
                they differ.
        """
        if channel not in (0, 1) or not pcm:
            return
        resampled = _resample_linear(pcm, sample_rate, self.sample_rate)
        samples = len(resampled) // PCM_BYTES_PER_SAMPLE
        if samples == 0:
            return

        # Anchor the timeline to whichever side speaks first. Subsequent frames are
        # placed at their wall-clock arrival time, so silence is inserted between
        # frames on the slower side rather than concatenating them.
        start_frame = self._now_frames()
        end_frame = start_frame + samples
        self._ensure_capacity_frames(end_frame)

        # Write samples into the chosen channel. Each stereo frame is L,R int16 LE
        # → 4 bytes, channel offset is 0 or 2 bytes from the frame start.
        new = struct.unpack(f"<{samples}h", resampled)
        buf = self._buffer
        for i, sample in enumerate(new):
            byte_idx = (start_frame - self._drained_frames + i) * BYTES_PER_STEREO_FRAME + (
                channel * PCM_BYTES_PER_SAMPLE
            )
            if byte_idx < 0 or byte_idx + PCM_BYTES_PER_SAMPLE > len(buf):
                continue
            # Sum into the existing channel sample (saturating add) so overlapping
            # frames don't clobber each other. In practice user/agent don't write
            # the same channel concurrently — this is a guard, not a feature.
            existing = struct.unpack_from("<h", buf, byte_idx)[0]
            mixed = max(-32768, min(32767, existing + sample))
            struct.pack_into("<h", buf, byte_idx, mixed)

    def take_chunk(self, *, chunk_bytes: int) -> bytes | None:
        """Drain one ``chunk_bytes``-sized chunk off the head, or return ``None``."""
        if len(self._buffer) < chunk_bytes:
            return None
        out = bytes(self._buffer[:chunk_bytes])
        del self._buffer[:chunk_bytes]
        self._drained_frames += chunk_bytes // BYTES_PER_STEREO_FRAME
        return out

    def take_tail(self) -> bytes:
        """Drain whatever's left in the buffer (called at call-ended)."""
        out = bytes(self._buffer)
        self._buffer.clear()
        self._drained_frames += len(out) // BYTES_PER_STEREO_FRAME
        return out


class AudioCapture:
    """Drive a ``StereoMixer`` + push chunks to Roark as they accrue.

    Concrete livekit-rtc subscription wiring lives in ``session.py`` — that
    module handles ``ctx.room.on("track_subscribed", ...)`` and feeds frames
    into ``add_user_frame`` / ``add_agent_frame``. This class owns the upload
    loop and idempotent flush.
    """

    def __init__(
        self,
        *,
        client: RoarkClient,
        livekit_call_id: str,
        sample_rate: int = DEFAULT_SAMPLE_RATE,
        chunk_bytes: int = DEFAULT_CHUNK_BYTES,
    ) -> None:
        self._client = client
        self._livekit_call_id = livekit_call_id
        self._chunk_bytes = chunk_bytes
        self.sample_rate = sample_rate
        self.num_channels = NUM_CHANNELS

        self._mixer = StereoMixer(sample_rate=sample_rate)
        self._chunk_index = 0
        self._uploaded_count = 0
        self._inflight: set[asyncio.Task[None]] = set()
        self._closed = False
        self._first_audio_observed = False

    @property
    def chunk_index(self) -> int:
        """Number of chunks queued for upload (informational for call-ended)."""
        return self._chunk_index

    @property
    def uploaded_count(self) -> int:
        """Number of chunks that actually landed in S3 (after retries).

        Distinct from ``chunk_index`` (chunks *enqueued*): if every upload fails,
        this stays 0. call-ended uses it to decide whether to advertise a
        recording — advertising one that never uploaded just makes the server-side
        merge hunt for chunks that aren't there.
        """
        return self._uploaded_count

    @property
    def first_audio_observed(self) -> bool:
        """True once at least one audio frame has been added (any channel)."""
        return self._first_audio_observed

    def add_user_frame(self, pcm: bytes, sample_rate: int) -> None:
        """Append user-side mono PCM (resampled + placed on the L channel)."""
        if self._closed:
            return
        self._first_audio_observed = True
        self._mixer.add_mono(channel=0, pcm=pcm, sample_rate=sample_rate)
        self._drain_ready_chunks()

    def add_agent_frame(self, pcm: bytes, sample_rate: int) -> None:
        """Append agent-side mono PCM (resampled + placed on the R channel)."""
        if self._closed:
            return
        self._first_audio_observed = True
        self._mixer.add_mono(channel=1, pcm=pcm, sample_rate=sample_rate)
        self._drain_ready_chunks()

    def _drain_ready_chunks(self) -> None:
        """Upload as many whole chunks as the mixer has ready."""
        while True:
            chunk = self._mixer.take_chunk(chunk_bytes=self._chunk_bytes)
            if chunk is None:
                return
            self._enqueue_upload(chunk)

    def _enqueue_upload(self, chunk: bytes) -> None:
        idx = self._chunk_index
        self._chunk_index = idx + 1
        task = asyncio.create_task(self._do_upload(idx, chunk))
        self._inflight.add(task)
        task.add_done_callback(self._inflight.discard)

    async def _do_upload(self, idx: int, pcm: bytes) -> None:
        for attempt in range(1, MAX_UPLOAD_ATTEMPTS + 1):
            # Presigned URLs are single-use, so re-request a fresh one each attempt.
            upload = await self._client.request_chunk_upload_url(
                livekit_call_id=self._livekit_call_id, chunk_index=idx
            )
            if upload and await self._client.upload_chunk(
                upload_url=upload["uploadUrl"], body=pcm
            ):
                self._uploaded_count += 1
                return
            if attempt < MAX_UPLOAD_ATTEMPTS:
                await asyncio.sleep(UPLOAD_RETRY_BACKOFF_SECONDS * attempt)
        log.warning(
            "chunk %d upload failed after %d attempts; dropping %d bytes",
            idx,
            MAX_UPLOAD_ATTEMPTS,
            len(pcm),
        )

    async def aflush(self) -> None:
        """Drain the tail + await every in-flight upload. Idempotent."""
        if self._closed:
            return
        self._closed = True
        tail = self._mixer.take_tail()
        if tail:
            self._enqueue_upload(tail)
        if self._inflight:
            await asyncio.gather(*list(self._inflight), return_exceptions=True)

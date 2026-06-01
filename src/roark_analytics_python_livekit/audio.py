"""Chunked upload of an already-aligned stereo PCM stream to Roark.

The hard part of a stereo call recording — placing each turn at the right point
on the timeline, inserting real silence between turns, and not collapsing a
faster-than-real-time TTS burst — is handled by livekit-agents' own
``RecorderIO`` (see ``_recorder.py``), not by anything here. This module only
takes the interleaved 16-bit stereo PCM that the recorder emits and ships it to
Roark as ~256 KB chunks via presigned S3 PUTs.

The chunk-upload contract matches ``pipecat-roark``: concatenated chunks form
one continuous interleaved-stereo PCM stream that the Roark merge wraps in a WAV
header.

Failures are logged and swallowed — the upload never raises into the
surrounding session.
"""

from __future__ import annotations

import asyncio
import logging

from .client import RoarkClient

log = logging.getLogger("roark_analytics_python_livekit.audio")


# Defaults --------------------------------------------------------------------

# 48 kHz stereo 16-bit ⇒ ~188 KB/s per channel; a 256 KB chunk flushes ~0.7s of
# audio. That's a good trade-off between upload frequency (latency on call-ended)
# and the per-chunk presigned-URL round trip cost. Matches pipecat-roark's default.
DEFAULT_CHUNK_BYTES = 256 * 1024

# livekit-agents' RecorderIO resamples both channels to a single rate; this is the
# default it uses, so the recording matches what LiveKit would write to disk. The
# session passes the resolved rate through to ``recordingSampleRate`` on call-ended.
DEFAULT_SAMPLE_RATE = 48_000

# 16-bit signed little-endian PCM. Two channels ⇒ 4 bytes per sample frame.
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

# Circuit breaker. Per-chunk retries (above) cope with transient blips, but when
# the upload endpoint is *persistently* down or unreachable (e.g. the chunk-upload
# service isn't running, or a presign host that hangs until timeout), every chunk
# would otherwise fail its full retry budget for the entire call — flooding the
# log and, if the endpoint hangs rather than refuses, piling up dozens of
# concurrent 30s+ upload tasks. After this many chunks fail in a row, the capture
# trips the breaker: it stops attempting uploads for the rest of the call, logs
# once, and drops further audio so memory stays bounded.
MAX_CONSECUTIVE_UPLOAD_FAILURES = 5


class AudioCapture:
    """Buffer an interleaved stereo PCM stream and push it to Roark in chunks.

    The stream is produced by livekit-agents' ``RecorderIO`` (wrapped by
    ``_recorder.RoarkRecorderIO``), which has already aligned the two channels
    and spliced the inter-turn silence. This class is purely the upload side: it
    appends incoming PCM into a single byte buffer and flushes whole
    ``chunk_bytes``-sized chunks as they accrue, plus the tail on ``aflush``.

    ``add_stereo_pcm`` must be called on the event loop (the recorder hands bytes
    over via ``loop.call_soon_threadsafe`` from its encode thread).
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
        self._sample_rate = sample_rate
        self.num_channels = NUM_CHANNELS

        self._buffer = bytearray()
        self._chunk_index = 0
        self._uploaded_count = 0
        self._inflight: set[asyncio.Task[None]] = set()
        self._closed = False
        self._first_audio_observed = False
        # Circuit breaker state. ``_consecutive_failures`` counts chunks that
        # exhausted their retry budget back-to-back; once it crosses the
        # threshold, ``_uploads_disabled`` latches and no further chunks are
        # uploaded for the rest of the call (see MAX_CONSECUTIVE_UPLOAD_FAILURES).
        self._consecutive_failures = 0
        self._uploads_disabled = False

    @property
    def sample_rate(self) -> int:
        """Recording sample rate — the rate RecorderIO resampled both channels to."""
        return self._sample_rate

    def set_sample_rate(self, sample_rate: int) -> None:
        """Adopt the negotiated recording rate (reported on call-ended).

        Only affects the advertised ``recordingSampleRate`` — the recorder owns
        the actual resampling. Call before any audio flows.
        """
        if sample_rate > 0:
            self._sample_rate = sample_rate

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
        """True once at least one PCM frame has been buffered."""
        return self._first_audio_observed

    @property
    def uploads_disabled(self) -> bool:
        """True once the circuit breaker has tripped on persistent upload failures."""
        return self._uploads_disabled

    def add_stereo_pcm(self, pcm: bytes) -> None:
        """Append interleaved 16-bit stereo PCM and drain whole chunks.

        Called on the event loop. ``pcm`` is L,R-interleaved signed little-endian
        produced by the recorder; this just accumulates and flushes.
        """
        if self._closed or not pcm:
            return
        self._first_audio_observed = True
        self._buffer.extend(pcm)
        while len(self._buffer) >= self._chunk_bytes:
            chunk = bytes(self._buffer[: self._chunk_bytes])
            del self._buffer[: self._chunk_bytes]
            self._enqueue_upload(chunk)

    def _enqueue_upload(self, chunk: bytes) -> None:
        # Breaker tripped: drop the chunk so memory stays bounded without
        # re-attempting a known-dead endpoint.
        if self._uploads_disabled or not chunk:
            return
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
            if upload and await self._client.upload_chunk(upload_url=upload["uploadUrl"], body=pcm):
                self._uploaded_count += 1
                self._consecutive_failures = 0
                return
            if attempt < MAX_UPLOAD_ATTEMPTS:
                await asyncio.sleep(UPLOAD_RETRY_BACKOFF_SECONDS * attempt)
        log.warning(
            "chunk %d upload failed after %d attempts; dropping %d bytes",
            idx,
            MAX_UPLOAD_ATTEMPTS,
            len(pcm),
        )
        self._note_upload_failure()

    def _note_upload_failure(self) -> None:
        """Advance the consecutive-failure count and trip the breaker if needed."""
        self._consecutive_failures += 1
        if (
            not self._uploads_disabled
            and self._consecutive_failures >= MAX_CONSECUTIVE_UPLOAD_FAILURES
        ):
            self._uploads_disabled = True
            log.error(
                "chunk upload disabled after %d consecutive failures; "
                "dropping further audio for this call",
                self._consecutive_failures,
            )

    async def aflush(self) -> None:
        """Flush the buffered tail + await every in-flight upload. Idempotent."""
        if self._closed:
            return
        self._closed = True
        if self._buffer:
            self._enqueue_upload(bytes(self._buffer))
            self._buffer = bytearray()
        if self._inflight:
            await asyncio.gather(*list(self._inflight), return_exceptions=True)

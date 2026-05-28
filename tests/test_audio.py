"""Tests for the stereo mixer + chunked upload pipeline.

Exercises the pure-Python mixer (no livekit-rtc required) and the AudioCapture
upload loop with a stubbed RoarkClient.
"""

from __future__ import annotations

import asyncio
import struct
from typing import Any

import pytest

from roark_analytics_python_livekit.audio import (
    BYTES_PER_STEREO_FRAME,
    DEFAULT_CHUNK_BYTES,
    AudioCapture,
    StereoMixer,
    _resample_linear,
)


def _make_pcm(samples: list[int]) -> bytes:
    return struct.pack(f"<{len(samples)}h", *samples)


def test_resample_linear_passthrough_when_rates_match() -> None:
    pcm = _make_pcm([100, 200, 300])
    assert _resample_linear(pcm, 16_000, 16_000) == pcm


def test_resample_linear_changes_sample_count() -> None:
    pcm = _make_pcm([0, 1000, 2000, 3000])
    # 16 → 24 kHz: 50% more samples.
    resampled = _resample_linear(pcm, 16_000, 24_000)
    assert len(resampled) > len(pcm)


def test_mixer_writes_user_into_left_channel() -> None:
    mixer = StereoMixer(sample_rate=8_000)
    mixer.add_mono(channel=0, pcm=_make_pcm([1000] * 100), sample_rate=8_000)
    # Drain the buffer — the L channel of every stereo frame must be non-zero,
    # the R channel must remain silent until the agent side writes.
    tail = mixer.take_tail()
    assert len(tail) > 0
    # First stereo frame: L=non-zero, R=0.
    first_l, first_r = struct.unpack_from("<hh", tail, 0)
    assert first_l != 0
    assert first_r == 0


@pytest.mark.asyncio
async def test_audio_capture_uploads_chunk_when_buffer_fills() -> None:
    uploaded: list[tuple[int, int]] = []

    class _StubClient:
        async def request_chunk_upload_url(self, *, livekit_call_id: str, chunk_index: int) -> Any:
            return {"uploadUrl": f"https://s3/{chunk_index}"}

        async def upload_chunk(self, *, upload_url: str, body: bytes) -> bool:
            uploaded.append((int(upload_url.rsplit("/", 1)[1]), len(body)))
            return True

    cap = AudioCapture(
        client=_StubClient(),  # type: ignore[arg-type]
        livekit_call_id="test",
        sample_rate=8_000,
        chunk_bytes=DEFAULT_CHUNK_BYTES,
    )

    # Push enough audio that at least one chunk has to flush. One DEFAULT_CHUNK_BYTES
    # of stereo 16-bit PCM = DEFAULT_CHUNK_BYTES / BYTES_PER_STEREO_FRAME stereo frames.
    frames_per_chunk = DEFAULT_CHUNK_BYTES // BYTES_PER_STEREO_FRAME
    cap.add_user_frame(_make_pcm([1000] * frames_per_chunk * 2), sample_rate=8_000)

    # Let the create_task fire.
    await asyncio.sleep(0)
    await cap.aflush()

    assert cap.chunk_index >= 1
    assert uploaded[0][0] == 0
    assert uploaded[0][1] == DEFAULT_CHUNK_BYTES

"""Tests for the chunked-upload side of audio capture.

``AudioCapture`` only buffers and uploads the already-aligned interleaved stereo
PCM produced by the recorder, so these tests need no livekit — they feed PCM
directly and stub the ``RoarkClient``. The alignment/silence logic lives in
``RoarkRecorderIO`` and is covered in ``test_recorder.py``.
"""

from __future__ import annotations

import asyncio
import struct
from typing import Any

import pytest

import roark_analytics_python_livekit.audio as audio_mod
from roark_analytics_python_livekit.audio import (
    BYTES_PER_STEREO_FRAME,
    DEFAULT_CHUNK_BYTES,
    AudioCapture,
)


def _stereo_pcm(left: list[int], right: list[int]) -> bytes:
    """Interleave equal-length L,R sample lists into 16-bit stereo PCM."""
    assert len(left) == len(right)
    out: list[int] = []
    for lo, ro in zip(left, right, strict=True):
        out.append(lo)
        out.append(ro)
    return struct.pack(f"<{len(out)}h", *out)


class _StubClient:
    def __init__(self) -> None:
        self.uploaded: list[tuple[int, int]] = []

    async def request_chunk_upload_url(self, *, livekit_call_id: str, chunk_index: int) -> Any:
        return {"uploadUrl": f"https://s3/{chunk_index}"}

    async def upload_chunk(self, *, upload_url: str, body: bytes) -> bool:
        self.uploaded.append((int(upload_url.rsplit("/", 1)[1]), len(body)))
        return True


@pytest.mark.asyncio
async def test_add_stereo_pcm_drains_full_chunks() -> None:
    client = _StubClient()
    cap = AudioCapture(
        client=client,  # type: ignore[arg-type]
        livekit_call_id="test",
        sample_rate=8_000,
        chunk_bytes=DEFAULT_CHUNK_BYTES,
    )

    # One whole chunk's worth of stereo frames (chunk_bytes / 4 frames per side).
    frames = DEFAULT_CHUNK_BYTES // BYTES_PER_STEREO_FRAME
    cap.add_stereo_pcm(_stereo_pcm([1000] * frames, [2000] * frames))

    await asyncio.sleep(0)  # let the create_task fire
    await cap.aflush()

    assert cap.first_audio_observed is True
    assert cap.chunk_index == 1
    assert cap.uploaded_count == 1
    assert client.uploaded[0] == (0, DEFAULT_CHUNK_BYTES)
    assert client.uploaded[0][1] % BYTES_PER_STEREO_FRAME == 0


@pytest.mark.asyncio
async def test_partial_remainder_uploaded_as_tail_on_flush() -> None:
    """A sub-chunk remainder isn't uploaded until aflush drains the tail."""
    client = _StubClient()
    cap = AudioCapture(client=client, livekit_call_id="t", chunk_bytes=DEFAULT_CHUNK_BYTES)  # type: ignore[arg-type]

    # Half a chunk: nothing should upload yet.
    half_frames = (DEFAULT_CHUNK_BYTES // BYTES_PER_STEREO_FRAME) // 2
    cap.add_stereo_pcm(_stereo_pcm([5] * half_frames, [6] * half_frames))
    await asyncio.sleep(0)
    assert cap.chunk_index == 0  # below the chunk threshold

    await cap.aflush()
    assert cap.chunk_index == 1  # tail flushed
    assert cap.uploaded_count == 1
    assert client.uploaded[0][1] == half_frames * BYTES_PER_STEREO_FRAME


@pytest.mark.asyncio
async def test_multiple_chunks_drain_in_order() -> None:
    client = _StubClient()
    cap = AudioCapture(client=client, livekit_call_id="t", chunk_bytes=2_000)  # type: ignore[arg-type]
    # 5_000 bytes of stereo PCM (1_250 frames) → two 2_000-byte chunks + 1_000 tail.
    frames = 5_000 // BYTES_PER_STEREO_FRAME
    cap.add_stereo_pcm(_stereo_pcm([1] * frames, [2] * frames))
    await asyncio.sleep(0)
    assert cap.chunk_index == 2  # two whole chunks drained immediately

    await cap.aflush()
    assert cap.chunk_index == 3  # plus the tail
    assert [idx for idx, _ in client.uploaded] == [0, 1, 2]  # in order


def test_set_sample_rate_updates_reported_rate() -> None:
    """The recording rate is adopted from the negotiated rate (dynamic rate) and
    reported as-is; a non-positive value is ignored."""
    client = _StubClient()
    cap = AudioCapture(client=client, livekit_call_id="t", sample_rate=48_000)  # type: ignore[arg-type]
    cap.set_sample_rate(8_000)
    assert cap.sample_rate == 8_000
    cap.set_sample_rate(0)  # ignored
    assert cap.sample_rate == 8_000


@pytest.mark.asyncio
async def test_no_upload_when_no_audio() -> None:
    client = _StubClient()
    cap = AudioCapture(client=client, livekit_call_id="t")  # type: ignore[arg-type]
    await cap.aflush()
    assert cap.first_audio_observed is False
    assert cap.chunk_index == 0
    assert cap.uploaded_count == 0


@pytest.mark.asyncio
async def test_add_after_flush_is_ignored() -> None:
    client = _StubClient()
    cap = AudioCapture(client=client, livekit_call_id="t", chunk_bytes=8)  # type: ignore[arg-type]
    await cap.aflush()
    cap.add_stereo_pcm(_stereo_pcm([1] * 100, [2] * 100))
    await asyncio.sleep(0)
    assert cap.chunk_index == 0  # closed → dropped


@pytest.mark.asyncio
async def test_do_upload_retries_transient_presign_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A transient presign failure (e.g. a 502) is retried, not dropped."""
    monkeypatch.setattr(audio_mod, "UPLOAD_RETRY_BACKOFF_SECONDS", 0)
    calls = {"presign": 0, "put": 0}

    class _FlakyClient:
        async def request_chunk_upload_url(self, *, livekit_call_id: str, chunk_index: int) -> Any:
            calls["presign"] += 1
            # First attempt 502s (None); second succeeds.
            return None if calls["presign"] == 1 else {"uploadUrl": "https://s3/0"}

        async def upload_chunk(self, *, upload_url: str, body: bytes) -> bool:
            calls["put"] += 1
            return True

    cap = AudioCapture(client=_FlakyClient(), livekit_call_id="t")  # type: ignore[arg-type]
    await cap._do_upload(0, b"\x00\x00\x00\x00")

    assert calls["presign"] == 2  # retried once
    assert calls["put"] == 1  # only the successful attempt PUTs
    assert cap.uploaded_count == 1


@pytest.mark.asyncio
async def test_do_upload_gives_up_after_max_attempts(monkeypatch: pytest.MonkeyPatch) -> None:
    """When every attempt fails, the chunk is dropped and uploaded_count stays 0."""
    monkeypatch.setattr(audio_mod, "UPLOAD_RETRY_BACKOFF_SECONDS", 0)
    presigns = 0

    class _DeadClient:
        async def request_chunk_upload_url(self, *, livekit_call_id: str, chunk_index: int) -> Any:
            nonlocal presigns
            presigns += 1
            return None  # always 502

        async def upload_chunk(self, *, upload_url: str, body: bytes) -> bool:  # pragma: no cover
            raise AssertionError("should never PUT when presign fails")

    cap = AudioCapture(client=_DeadClient(), livekit_call_id="t")  # type: ignore[arg-type]
    await cap._do_upload(0, b"\x00\x00\x00\x00")

    assert presigns == audio_mod.MAX_UPLOAD_ATTEMPTS
    assert cap.uploaded_count == 0


@pytest.mark.asyncio
async def test_circuit_breaker_trips_after_consecutive_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A persistently-dead endpoint trips the breaker and stops further uploads."""
    monkeypatch.setattr(audio_mod, "UPLOAD_RETRY_BACKOFF_SECONDS", 0)
    presigns = 0

    class _DeadClient:
        async def request_chunk_upload_url(self, *, livekit_call_id: str, chunk_index: int) -> Any:
            nonlocal presigns
            presigns += 1
            return None  # always fails

        async def upload_chunk(self, *, upload_url: str, body: bytes) -> bool:  # pragma: no cover
            raise AssertionError("should never PUT when presign fails")

    cap = AudioCapture(client=_DeadClient(), livekit_call_id="t")  # type: ignore[arg-type]

    # Fail exactly the threshold number of chunks back-to-back → breaker trips.
    for idx in range(audio_mod.MAX_CONSECUTIVE_UPLOAD_FAILURES):
        await cap._do_upload(idx, b"\x00\x00\x00\x00")
    assert cap.uploads_disabled is True

    presigns_at_trip = presigns
    # Once tripped, enqueuing further chunks is a no-op — no new task, no presign.
    cap._enqueue_upload(b"\x00\x00\x00\x00")
    assert cap.uploaded_count == 0
    assert presigns == presigns_at_trip  # no further presign attempts
    assert not cap._inflight


@pytest.mark.asyncio
async def test_circuit_breaker_resets_on_success(monkeypatch: pytest.MonkeyPatch) -> None:
    """A successful upload clears the consecutive-failure count, so blips don't trip."""
    monkeypatch.setattr(audio_mod, "UPLOAD_RETRY_BACKOFF_SECONDS", 0)
    # Per-chunk plan: chunk 2 succeeds, the rest fail every attempt. A failing
    # chunk presigns None (so upload_chunk is never reached and it fails outright).
    succeeds = {2}

    class _BlipClient:
        async def request_chunk_upload_url(self, *, livekit_call_id: str, chunk_index: int) -> Any:
            return {"uploadUrl": "https://s3/0"} if chunk_index in succeeds else None

        async def upload_chunk(self, *, upload_url: str, body: bytes) -> bool:
            return True

    cap = AudioCapture(client=_BlipClient(), livekit_call_id="t")  # type: ignore[arg-type]
    # fail, fail, SUCCESS(reset), fail, fail → longest streak is 2, never hits 5.
    for idx in range(5):
        await cap._do_upload(idx, b"\x00\x00\x00\x00")

    assert cap.uploads_disabled is False
    assert cap.uploaded_count == 1

"""Tests for RoarkRecorderIO — the livekit RecorderIO subclass that emits PCM.

Requires livekit-rtc + numpy (and, transitively, ``av``), so the whole module is
skipped when they aren't importable. The channel-alignment/silence logic itself
is livekit's; these tests cover the part we add — downmix, the leading-silence
alignment, and that the encode thread turns matched frame pairs into interleaved
stereo PCM at the sink.
"""

from __future__ import annotations

import asyncio
import struct

import pytest

pytest.importorskip("numpy")
pytest.importorskip("livekit.rtc")
pytest.importorskip("livekit.agents.voice.recorder_io")

import numpy as np  # noqa: E402
from livekit import rtc  # noqa: E402

from roark_analytics_python_livekit._recorder import (  # noqa: E402
    RoarkRecorderIO,
    _frames_to_mono_i16,
    _interleave_aligned,
)


def _split_stereo(pcm: bytes) -> tuple[list[int], list[int]]:
    samples = struct.unpack(f"<{len(pcm) // 2}h", pcm)
    return list(samples[0::2]), list(samples[1::2])


def _mono_frame(value: int, n: int, rate: int) -> rtc.AudioFrame:
    return rtc.AudioFrame(
        data=struct.pack(f"<{n}h", *([value] * n)),
        num_channels=1,
        samples_per_channel=n,
        sample_rate=rate,
    )


def test_frames_to_mono_downmixes_and_concatenates() -> None:
    # Two interleaved-stereo frames: averaging [100,300]→200 and [400,600]→500.
    f = rtc.AudioFrame(
        data=struct.pack("<4h", 100, 300, 400, 600),
        num_channels=2,
        samples_per_channel=2,
        sample_rate=16_000,
    )
    out = _frames_to_mono_i16([f, f])
    assert out.tolist() == [200, 500, 200, 500]


def test_frames_to_mono_empty() -> None:
    assert _frames_to_mono_i16([]).tolist() == []


def test_interleave_pads_shorter_right_with_leading_silence() -> None:
    """A short agent (right) segment is end-aligned against the longer user
    (left) window — the silence lands *before* the agent audio, which is the
    inter-turn silence that must be present."""
    left = np.array([1000] * 5, dtype=np.int16)
    right = np.array([2000] * 2, dtype=np.int16)
    lo, ro = _split_stereo(_interleave_aligned(left, right))
    assert lo == [1000] * 5
    assert ro == [0, 0, 0, 2000, 2000]  # leading silence, then the agent audio


def test_interleave_pads_shorter_left() -> None:
    left = np.array([1000] * 2, dtype=np.int16)
    right = np.array([2000] * 4, dtype=np.int16)
    lo, ro = _split_stereo(_interleave_aligned(left, right))
    assert lo == [0, 0, 1000, 1000]
    assert ro == [2000] * 4


def test_interleave_empty() -> None:
    assert _interleave_aligned(np.zeros(0, np.int16), np.zeros(0, np.int16)) == b""


class _Stub:
    """Minimal stand-in so start()'s "both sides wrapped" guard passes."""

    _last_speech_end_time = None


@pytest.mark.asyncio
async def test_encode_thread_emits_interleaved_pcm_to_sink() -> None:
    """End-to-end of the encode step: a matched (user, agent) frame pair pushed
    onto the recorder's queues comes back out of the sink as interleaved stereo
    PCM, with the shorter agent side padded so silence sits between turns."""
    received: list[bytes] = []
    loop = asyncio.get_running_loop()
    rate = 16_000
    recorder = RoarkRecorderIO(
        agent_session=object(),
        sink=received.append,
        sample_rate=rate,
        loop=loop,
    )
    # Bypass record_input/record_output wiring: we drive the queues directly to
    # test our encode thread (the alignment that feeds the queues is livekit's).
    recorder._in_record = _Stub()  # type: ignore[assignment]
    recorder._out_record = _Stub()  # type: ignore[assignment]
    await recorder.start()

    # 0.5s of user audio, 0.25s of agent audio → agent is the shorter side.
    user = _mono_frame(1000, rate // 2, rate)
    agent = _mono_frame(3000, rate // 4, rate)
    recorder._in_q.put_nowait([user])
    recorder._out_q.put_nowait([agent])

    await recorder.aclose()
    await asyncio.sleep(0)

    assert len(received) == 1
    left, right = _split_stereo(received[0])
    # Same-rate resampling can trim a handful of samples at the edges; assert on
    # structure, not exact counts.
    assert len(left) == len(right)
    assert sum(1 for s in left if s != 0) > rate // 3  # user fills most of the window
    leading_silence = next((i for i, s in enumerate(right) if s != 0), len(right))
    assert leading_silence > 0  # agent is preceded by real silence (the gap)
    assert any(s != 0 for s in right)  # the agent audio is present


@pytest.mark.asyncio
async def test_start_requires_both_records() -> None:
    recorder = RoarkRecorderIO(
        agent_session=object(), sink=lambda _b: None, loop=asyncio.get_running_loop()
    )
    with pytest.raises(RuntimeError):
        await recorder.start()


@pytest.mark.asyncio
async def test_set_target_sample_rate_changes_resample_target() -> None:
    """The session adopts the negotiated rate before start(); the encode thread
    resamples to it. A non-positive value is ignored."""
    recorder = RoarkRecorderIO(
        agent_session=object(),
        sink=lambda _b: None,
        sample_rate=48_000,
        loop=asyncio.get_running_loop(),
    )
    recorder.set_target_sample_rate(8_000)
    assert recorder._sample_rate == 8_000
    recorder.set_target_sample_rate(0)  # ignored
    assert recorder._sample_rate == 8_000

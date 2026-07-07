# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 RevoRacer
"""Round-trip tests: Python↔Python and verifying zero-copy memoryview."""
import struct
import pytest
import revoq

np = pytest.importorskip("numpy", reason="numpy not installed")


MSG_TYPE = 100


def write_frames(location, dest_id, n):
    w = revoq.Appender(location, dest_id)
    payloads = []
    for i in range(n):
        # Pack (uint64 seq, float64 value) — 16 bytes
        p = struct.pack("<Qd", i, float(i) * 1.5)
        w.write(msg_type=MSG_TYPE, gen_time=i + 1, data=p)
        payloads.append(p)
    del w
    return payloads


def read_frames(location, dest_id):
    r = revoq.MergingCursor(prefault=True, background_threads=False)
    r.join(location, dest_id, 0)
    return [f for f in r if f.msg_type == MSG_TYPE]


# ── Python → Python ───────────────────────────────────────────────────────────

def test_python_to_python_roundtrip(tmp_journal):
    _, location, dest_id = tmp_journal
    n = 50
    payloads = write_frames(location, dest_id, n)
    frames = read_frames(location, dest_id)

    assert len(frames) == n
    for i, (frame, expected) in enumerate(zip(frames, payloads)):
        assert frame.sequence == i
        assert frame.gen_time == i + 1
        assert frame.data_length == len(expected)
        assert bytes(frame.data) == expected


def test_memoryview_is_zero_copy(tmp_journal):
    """frame.data is a memoryview into the mmap, not a copy."""
    _, location, dest_id = tmp_journal
    payload = struct.pack("<Qd", 7, 2.718)
    w = revoq.Appender(location, dest_id)
    w.write(msg_type=MSG_TYPE, gen_time=1, data=payload)
    del w

    r = revoq.MergingCursor(prefault=True, background_threads=False)
    r.join(location, dest_id, 0)
    frame = next(iter(r))

    mv = frame.data
    assert isinstance(mv, memoryview)
    assert bytes(mv) == payload


def test_numpy_from_memoryview(tmp_journal):
    """numpy can read the payload without a copy via frombuffer."""
    _, location, dest_id = tmp_journal

    # Write 10 frames each carrying a float64
    import numpy as np
    values = [float(i * 3) for i in range(10)]
    w = revoq.Appender(location, dest_id)
    for v in values:
        w.write(msg_type=MSG_TYPE, gen_time=1, data=struct.pack("<d", v))
    del w

    r = revoq.MergingCursor(prefault=True, background_threads=False)
    r.join(location, dest_id, 0)
    for i, frame in enumerate(r):
        if frame.msg_type != MSG_TYPE:
            continue
        arr = np.frombuffer(frame.data, dtype=np.float64)
        assert arr[0] == pytest.approx(values[i])


def test_sequence_contiguous_across_segments(tmp_journal):
    """Sequences are contiguous even across segment rotations."""
    _, location, dest_id = tmp_journal
    # DEFAULT segment (executiondata + dest_id=1 → 4MB) with 64B frames:
    # frames/segment ≈ 65,534.  Write 70,000 to force 1 rotation.
    n = 70_000
    w = revoq.Appender(location, dest_id, background_threads=False)
    payload = struct.pack("<Q", 0)
    for i in range(n):
        w.write(msg_type=MSG_TYPE, gen_time=i + 1, data=payload)
    del w

    r = revoq.MergingCursor(prefault=True, background_threads=False)
    r.join(location, dest_id, 0)

    prev_seq = -1
    data_count = 0
    for frame in r:
        seq = frame.sequence
        assert seq == prev_seq + 1
        prev_seq = seq
        if frame.msg_type == MSG_TYPE:
            data_count += 1

    assert data_count == n

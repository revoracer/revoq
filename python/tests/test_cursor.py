# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 RevoRacer
"""MergingCursor Python binding tests."""
import struct
import revoq


def test_cursor_constructs():
    r = revoq.MergingCursor()
    assert r.current_frame() is None


def test_cursor_empty_returns_none_immediately(tmp_journal):
    _, location, dest_id = tmp_journal
    # Write one frame so the stream segment exists, then read with a fresh cursor.
    w = revoq.Appender(location, dest_id)
    w.write(msg_type=100, gen_time=1, data=b"\x01\x02")
    del w

    r = revoq.MergingCursor(prefault=True, background_threads=False)
    r.join(location, dest_id, 0)

    # Non-blocking read with no timeout returns None when no more data.
    frames = list(r)          # drain via iterator
    assert len(frames) == 1
    assert r.read(timeout_ms=0) is None


def test_cursor_frame_fields(tmp_journal):
    _, location, dest_id = tmp_journal
    payload = struct.pack("<qd", 99, 1.23)
    w = revoq.Appender(location, dest_id)
    w.write(msg_type=100, gen_time=1_000_000, data=payload)
    del w

    r = revoq.MergingCursor(prefault=True, background_threads=False)
    r.join(location, dest_id, 0)
    frame = r.read()
    assert frame is not None
    assert frame.msg_type == 100
    assert frame.gen_time == 1_000_000
    assert frame.sequence == 0
    assert frame.data_length == len(payload)


def test_cursor_iterator_exhausts(tmp_journal):
    _, location, dest_id = tmp_journal
    n = 20
    w = revoq.Appender(location, dest_id)
    for i in range(n):
        w.write(msg_type=100, gen_time=i + 1, data=struct.pack("<q", i))
    del w

    r = revoq.MergingCursor(prefault=True, background_threads=False)
    r.join(location, dest_id, 0)
    frames = list(r)
    # Iterator stops at first unavailable frame; SegmentEnd frames may appear
    # at segment boundaries but not within a single small segment.
    data_frames = [f for f in frames if f.msg_type == 100]
    assert len(data_frames) == n


def test_cursor_stop_iteration(tmp_journal):
    _, location, dest_id = tmp_journal
    w = revoq.Appender(location, dest_id)
    w.write(msg_type=100, gen_time=1, data=b"x")
    del w

    r = revoq.MergingCursor(prefault=True, background_threads=False)
    r.join(location, dest_id, 0)
    count = 0
    for _ in r:
        count += 1
    assert count >= 1
    # Second iteration immediately raises StopIteration
    try:
        next(iter(r))
        assert False, "expected StopIteration"
    except StopIteration:
        pass

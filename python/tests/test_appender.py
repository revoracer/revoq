// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 RevoRacer
"""Appender Python binding tests."""
import struct
import pytest
import revoq


def test_appender_constructs(tmp_journal):
    _, location, dest_id = tmp_journal
    w = revoq.Appender(location, dest_id)
    assert w.current_sequence() == 0
    assert w.current_segment_id() == 1


def test_appender_sequence_increments(tmp_journal):
    _, location, dest_id = tmp_journal
    w = revoq.Appender(location, dest_id)
    for i in range(10):
        w.write(msg_type=100, gen_time=i + 1, data=b"\x00" * 8)
    assert w.current_sequence() == 10


def test_appender_accepts_bytes(tmp_journal):
    _, location, dest_id = tmp_journal
    w = revoq.Appender(location, dest_id)
    payload = struct.pack("<qd", 42, 3.14)
    w.write(msg_type=100, gen_time=1_000_000, data=payload)
    assert w.current_sequence() == 1


def test_appender_accepts_bytearray(tmp_journal):
    _, location, dest_id = tmp_journal
    w = revoq.Appender(location, dest_id)
    w.write(msg_type=100, gen_time=1, data=bytearray(b"hello"))
    assert w.current_sequence() == 1


def test_appender_accepts_empty_payload(tmp_journal):
    _, location, dest_id = tmp_journal
    w = revoq.Appender(location, dest_id)
    w.write(msg_type=100, gen_time=1, data=b"")
    assert w.current_sequence() == 1


def test_appender_exposes_warmer_and_cpu_options(tmp_journal):
    _, location, dest_id = tmp_journal
    w = revoq.Appender(
        location,
        dest_id,
        populate=revoq.SegmentPopulate.Background,
        background_threads=True,
        warmup_segments=1,
        warmer_cpu_core=-1,
        retire_cpu=-1,
        retained_segment_limit=1,
    )
    w.write(msg_type=100, gen_time=1, data=b"warm")
    assert w.current_sequence() == 1


def test_appender_rejects_too_many_warmup_segments(tmp_journal):
    _, location, dest_id = tmp_journal
    with pytest.raises(ValueError, match="warmup_segments must be within"):
        revoq.Appender(location, dest_id, warmup_segments=17)


def test_appender_background_requires_background_threads(tmp_journal):
    _, location, dest_id = tmp_journal
    with pytest.raises(ValueError, match="requires background_threads"):
        revoq.Appender(location, dest_id, background_threads=False)


def test_make_location_unknown_mode(tmp_path):
    with pytest.raises(ValueError, match="Unknown mode"):
        revoq.make_location(str(tmp_path), mode="bad", category="marketdata",
                            group="g", name="n")


def test_make_location_unknown_category(tmp_path):
    with pytest.raises(ValueError, match="Unknown category"):
        revoq.make_location(str(tmp_path), mode="live", category="bad",
                            group="g", name="n")

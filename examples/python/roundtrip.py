#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 RevoRacer
import struct
import tempfile

import revoq


def main() -> int:
    msg_type = 100
    payloads = [struct.pack("<Qd", i, 100.0 + i) for i in range(3)]

    with tempfile.TemporaryDirectory(prefix="revoq-python-roundtrip-") as path:
        location = revoq.make_location(
            path,
            mode="live",
            category="executiondata",
            group="examples",
            name="python-roundtrip",
        )

        appender = revoq.Appender(
            location, 0, populate=revoq.SegmentPopulate.Lazy, background_threads=False
        )
        for i, payload in enumerate(payloads):
            appender.write(msg_type=msg_type, gen_time=i + 1, data=payload)
        del appender

        cursor = revoq.MergingCursor(
            populate=revoq.SegmentPopulate.Lazy, background_threads=False
        )
        cursor.join(location, 0, 0)
        frames = [frame for frame in cursor if frame.msg_type == msg_type]

        assert len(frames) == len(payloads)
        for frame, expected in zip(frames, payloads):
            assert bytes(frame.data) == expected

    print(f"ok python_roundtrip messages={len(payloads)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

# File Format

Stream files are segment files under a location directory. Each segment contains:

- `SegmentHeader`: version, header length, segment size, frame header length, and last-frame offset.
- Zero or more aligned frames.
- A `SegmentEnd` sentinel when the appender rotates to the next segment.

Each frame contains:

- `length`: logical frame length, published last.
- `msg_type`: application message type.
- `flags`: frame flags.
- `sequence`: appender sequence number.
- `gen_time`: producer timestamp.
- `event_time`: optional application timestamp.
- payload bytes immediately after the header.

Physical frame stride is aligned to `FRAME_ALIGNMENT`.

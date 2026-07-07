# Architecture

Revoq is a file-backed stream built around memory-mapped segments.

Core pieces:

- `Appender`: appends frames to a destination stream.
- `MergingCursor`: joins one or more streams and reads committed frames.
- `FrameHeader`: fixed-size metadata published with release semantics.
- `Segment`: memory-mapped stream segment with a fixed header and aligned frames.
- `AppenderWarmer`: optional segment preparation ahead of the appender.
- `CursorWarmer`: optional read-side segment preparation ahead of the cursor.

Append operations publish a frame by filling payload bytes first and storing
frame length last. Cursors use acquire loads of the frame length before reading
metadata and payload.

## Tuned benchmark topology

The tuned latency benchmark uses four explicit isolated roles:

| role | responsibility |
| --- | --- |
| appender | opens frames, writes payloads, publishes frame lengths |
| cursor | polls committed frames and dispatches handlers |
| appender warmer | prepares upcoming writable segment mappings |
| cursor warmer | prepares upcoming read-only segment mappings |

Retire/cleanup threads are background maintenance and should stay on
housekeeping CPUs, not on the isolated benchmark CPUs.

The benchmark exercises the application path from appender timestamp to cursor
handler endpoint using a direct static dispatch strategy.

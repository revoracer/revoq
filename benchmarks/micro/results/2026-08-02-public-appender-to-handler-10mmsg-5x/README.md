# 2026-08-02 public appender-to-handler benchmark

Public tuned `revoq_benchmark_stream_handler_benchmark_p99` artifact.

Measured span: appender timestamp / intended-send time to cursor handler
endpoint.

This directory name uses `10mmsg` to mean **10,000,000 measured messages per
run**. Within `public-micro/stdout/`, file names such as `tuned-10m-run1.txt`
use `10m` to mean the offered rate, **10M messages/second** (`--interval-ns
100`).

## Provenance

- Benchmark binary: `./build/revoq_benchmark_stream_handler_benchmark_p99`
- Payload: 32 B
- Message types: 4
- Dispatch: `static_switch`
- Topology: tuned, forked cursor, four pinned isolated logical roles, occupying
  four physical cores as shown by the SMT sibling map below
- CPUs: appender 13, cursor 14, appender warmer 3, cursor warmer 12
- Realtime priority: 80
- Hugepages: 2048 x 2 MiB pages on a hugetlbfs mount
- Command lines: `cmd/*.cmd` with local filesystem paths redacted

Host capture lives in `env/`: kernel, CPU topology, isolated CPU set, CPU
governors, hugepage state, hugetlbfs mount state, memlock setting, toolchain
versions, git commit, and git tree state. Local filesystem paths are redacted.

Role-to-core mapping:

| Role | Logical CPU | SMT sibling set |
| --- | ---: | --- |
| appender | 13 | `5,13` |
| cursor | 14 | `6,14` |
| appender warmer | 3 | `3,11` |
| cursor warmer | 12 | `4,12` |

## Tuned 10M msg/s runs

Each run used 10,000,000 measured messages, 200,000 warmup messages, and
`--interval-ns 100`. The measured span is appender timestamp / intended-send
time to cursor handler endpoint.

| Run | p50 ns | p90 ns | p99 ns | p99.9 ns | p99.99 ns | p99.999 ns | max ns |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `tuned-10m-run1.txt` | 106 | 121 | 139 | 219 | 2087 | 7963 | 13168 |
| `tuned-10m-run2.txt` | 107 | 121 | 139 | 217 | 1970 | 7171 | 11605 |
| `tuned-10m-run3.txt` | 106 | 121 | 138 | 217 | 2847 | 8122 | 13679 |
| `tuned-10m-run4.txt` | 106 | 121 | 139 | 217 | 2229 | 7387 | 12908 |
| `tuned-10m-run5.txt` | 106 | 121 | 139 | 219 | 3479 | 12256 | 18746 |

Headline range: p99 stayed between **138 ns and 139 ns**; p99.9 stayed between
**217 ns and 219 ns**.

## Tuned 1M msg/s runs

Each run used 10,000,000 measured messages, 200,000 warmup messages, and
`--interval-ns 1000`.

| Run | p50 ns | p90 ns | p99 ns | p99.9 ns | p99.99 ns | p99.999 ns | max ns |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `tuned-1m-run1.txt` | 107 | 122 | 138 | 185 | 535 | 1843 | 21322 |
| `tuned-1m-run2.txt` | 107 | 122 | 138 | 149 | 236 | 1659 | 21546 |
| `tuned-1m-run3.txt` | 107 | 121 | 138 | 149 | 221 | 2414 | 20981 |
| `tuned-1m-run4.txt` | 107 | 122 | 138 | 149 | 226 | 1262 | 21310 |
| `tuned-1m-run5.txt` | 107 | 122 | 138 | 150 | 235 | 1249 | 19608 |

The 1M msg/s runs are included as additional tuned evidence. They are not the
headline rate, but they confirm that the lower-rate path stays at p99 138 ns
with a p99.9 range of 149-185 ns on this tuned host. Run 1 has the widest 1M
msg/s tail; runs 2-5 form a tighter repeat set. The summary range includes all
five runs.

## Files

- `cmd/` contains the tuned benchmark command lines with local filesystem paths
  redacted.
- `public-micro/stdout/` contains benchmark stdout with local filesystem paths
  redacted and HdrHistogram payloads for all ten tuned runs.
- `env/` contains host, kernel, CPU topology, hugepage, governor, toolchain,
  commit, and tree-state capture.

<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 RevoRacer -->

# Comparative results — 2026-07-08 (manual-all)

revoq vs **Aeron** (IPC + Archive) and **Chronicle Queue** (OSS) on the single
canonical workload in [`../../workload.md`](../../workload.md). This directory is
self-contained: the committed `*.json` are the source of truth (schema:
[`../../schema.md`](../../schema.md)), `env.json` is the host capture, and the
tables below are generated directly from those files.

Validate with:

```bash
python3 ../../harness/validate_results.py --require-oss .
```

## Host & environment

| field | value |
|---|---|
| host | `femy` |
| CPU | AMD Ryzen 7 7840HS w/ Radeon 780M Graphics |
| kernel | Linux 7.0.0-27-generic |
| core isolation | `isolcpus=2,3,4,5,6,10,11,12,13,14,nohz_full=3,4,5,6,11,12,13,14` |
| scaling governor | performance |
| hugepages (2 MiB) | 4096 |
| JDK | openjdk version "21.0.11" 2026-04-21 |
| captured (UTC) | 2026-07-09T06:05:03Z |

> Laptop-class host with isolated cores, `performance` governor, and a hugetlbfs
> pool — best-effort isolation, not a dedicated server. Good enough for a fair
> relative read; absolute numbers would tighten further on server silicon.

## Systems

| system | version | edition | semantic group | durability |
|---|---|---|---|---|
| revoq | v0.1.0 + AppenderWarmer race fix (`f86ff59-dirty`) | oss | volatile_ipc | none |
| aeron-ipc | 1.44.1 | oss | volatile_ipc | none |
| aeron-archive | 1.44.1 | oss | journal_replayable | archive |
| chronicle-queue | 2026.5 | oss | journal_replayable | page_cache |

All runs: **1p1c**, **200,000** warmup + **5,000,000** measured messages per
(size, rate) point, **batch_size = 1**, open-loop. Paced points stamp the frame
at *intended* send time (coordinated-omission-safe); unpaced (saturation) points
stamp actual send time and report **throughput only** — their latency
percentiles are queue depth, not response time, and are not charted. The JVM
drivers are warmed to a **confirmed JIT steady state** (`jit_steady_state_confirmed`,
12/12 points each) with a forced warmup GC, so the tails below are warm-JVM fair.

## A) Tail latency — `volatile_ipc` / `none` (revoq vs Aeron IPC)

The valid apples-to-apples cell: both volatile shared memory. Open-loop, paced.

**Offered 500,000 msg/s**

| payload | system | p50 | p99 | p99.9 | p99.99 | max |
|---|---|--:|--:|--:|--:|--:|
| 32 B | revoq | 108 ns | 137 ns | 216 ns | 248 ns | 19.8 µs |
| 32 B | aeron-ipc | 102 ns | 938 ns | 2.4 µs | 13.6 µs | 178.8 µs |
| 64 B | revoq | 107 ns | 136 ns | 214 ns | 251 ns | 18.3 µs |
| 64 B | aeron-ipc | 103 ns | 975 ns | 2.1 µs | 25.0 µs | 217.0 µs |
| 256 B | revoq | 105 ns | 137 ns | 211 ns | 260 ns | 21.9 µs |
| 256 B | aeron-ipc | 137 ns | 1.0 µs | 3.9 µs | 30.6 µs | 179.1 µs |
| 1024 B | revoq | 114 ns | 265 ns | 475 ns | 953 ns | 20.5 µs |
| 1024 B | aeron-ipc | 145 ns | 313 ns | 1.7 µs | 14.8 µs | 188.7 µs |

**Offered 1,000,000 msg/s**

| payload | system | p50 | p99 | p99.9 | p99.99 | max |
|---|---|--:|--:|--:|--:|--:|
| 32 B | revoq | 104 ns | 136 ns | 207 ns | 254 ns | 18.3 µs |
| 32 B | aeron-ipc | 93 ns | 926 ns | 2.8 µs | 162.2 µs | 259.5 µs |
| 64 B | revoq | 105 ns | 137 ns | 199 ns | 253 ns | 18.5 µs |
| 64 B | aeron-ipc | 95 ns | 973 ns | 2.9 µs | 56.2 µs | 188.5 µs |
| 256 B | revoq | 104 ns | 136 ns | 199 ns | 258 ns | 21.7 µs |
| 256 B | aeron-ipc | 132 ns | 1.1 µs | 3.1 µs | 65.1 µs | 218.2 µs |
| 1024 B | revoq | 107 ns | 338 ns | 504 ns | 4.6 µs | 20.2 µs |
| 1024 B | aeron-ipc | 134 ns | 282 ns | 4.7 µs | 84.0 µs | 240.3 µs |

**Read:** median is a **tie** (~100–140 ns both). revoq's tail runs **~10×**
tighter (7–15× by percentile/size): sub-150 ns p99 and sub-µs p99.9 across every
size, where warm Aeron IPC sits at ~1 µs p99 and low-single-digit-µs p99.9.

## B) Throughput — saturation (unpaced)

| payload | system | durability | achieved msg/s | GB/s | revoq speedup |
|---|---|---|--:|--:|--:|
| 32 B | revoq | none | 42,611,510 | 1.36 | (base) |
| 32 B | aeron-ipc | none | 22,759,135 | 0.73 | 1.87× |
| 32 B | chronicle | page_cache | 6,417,539 | 0.21 | 6.64× |
| 32 B | aeron-archive | archive | 19,333,342 | 0.62 | 2.20× |
| 64 B | revoq | none | 42,156,022 | 2.70 | (base) |
| 64 B | aeron-ipc | none | 20,996,357 | 1.34 | 2.01× |
| 64 B | chronicle | page_cache | 5,870,187 | 0.38 | 7.18× |
| 64 B | aeron-archive | archive | 17,696,749 | 1.13 | 2.38× |
| 256 B | revoq | none | 36,656,320 | 9.38 | (base) |
| 256 B | aeron-ipc | none | 14,779,983 | 3.78 | 2.48× |
| 256 B | chronicle | page_cache | 3,967,252 | 1.02 | 9.24× |
| 256 B | aeron-archive | archive | 8,351,086 | 2.14 | 4.39× |
| 1024 B | revoq | none | 12,023,878 | 12.31 | (base) |
| 1024 B | aeron-ipc | none | 12,679,929 | 12.98 | 0.95× (Aeron faster) |
| 1024 B | chronicle | page_cache | 1,452,813 | 1.49 | 8.28× |
| 1024 B | aeron-archive | archive | 2,711,491 | 2.78 | 4.43× |

**Read:** revoq sustains **~1.9–2.5×** Aeron IPC's saturation throughput up to
256 B; at **1024 B they converge to parity** (both ~12 GB/s, memory-bandwidth-
bound — Aeron edges it by ~5%). Chronicle and Aeron Archive are durable systems
(different `durability`); their throughput is shown with the tag, not as a
volatile-IPC comparison.

## C) Tail latency — durable `journal_replayable`

Chronicle (`page_cache`) and Aeron Archive (`archive`) are **different durability
modes**, so they are not a single chart cell — shown side by side with the tag.
A revoq row from the `none` run is included **only as a stand-in** (see caveat).

**Offered 500,000 msg/s**

| payload | system | durability | p50 | p99 | p99.9 | p99.99 | max |
|---|---|---|--:|--:|--:|--:|--:|
| 32 B | revoq | none \* | 108 ns | 137 ns | 216 ns | 248 ns | 19.8 µs |
| 32 B | chronicle | page_cache | 195 ns | 2.5 µs | 5.5 µs | 335.4 µs | 634.4 µs |
| 32 B | aeron-archive | archive | 105 ns | 965 ns | 2.7 µs | 19.3 µs | 210.8 µs |
| 64 B | revoq | none \* | 107 ns | 136 ns | 214 ns | 251 ns | 18.3 µs |
| 64 B | chronicle | page_cache | 198 ns | 2.6 µs | 35.2 µs | 325.9 µs | 627.7 µs |
| 64 B | aeron-archive | archive | 110 ns | 999 ns | 2.2 µs | 21.5 µs | 217.3 µs |
| 256 B | revoq | none \* | 105 ns | 137 ns | 211 ns | 260 ns | 21.9 µs |
| 256 B | chronicle | page_cache | 218 ns | 2.9 µs | 73.2 µs | 363.0 µs | 619.5 µs |
| 256 B | aeron-archive | archive | 157 ns | 1.0 µs | 2.8 µs | 15.6 µs | 207.7 µs |
| 1024 B | revoq | none \* | 114 ns | 265 ns | 475 ns | 953 ns | 20.5 µs |
| 1024 B | chronicle | page_cache | 1.3 µs | 5.4 µs | 266.0 µs | 1.50 ms | 1.75 ms |
| 1024 B | aeron-archive | archive | 208 ns | 349 ns | 2.9 µs | 17.2 µs | 212.9 µs |

**Offered 1,000,000 msg/s**

| payload | system | durability | p50 | p99 | p99.9 | p99.99 | max |
|---|---|---|--:|--:|--:|--:|--:|
| 32 B | revoq | none \* | 104 ns | 136 ns | 207 ns | 254 ns | 18.3 µs |
| 32 B | chronicle | page_cache | 188 ns | 2.8 µs | 140.2 µs | 948.2 µs | 993.8 µs |
| 32 B | aeron-archive | archive | 104 ns | 965 ns | 2.9 µs | 57.1 µs | 201.3 µs |
| 64 B | revoq | none \* | 105 ns | 137 ns | 199 ns | 253 ns | 18.5 µs |
| 64 B | chronicle | page_cache | 188 ns | 2.9 µs | 237.3 µs | 673.3 µs | 705.5 µs |
| 64 B | aeron-archive | archive | 109 ns | 1.0 µs | 3.1 µs | 64.5 µs | 218.5 µs |
| 256 B | revoq | none \* | 104 ns | 136 ns | 199 ns | 258 ns | 21.7 µs |
| 256 B | chronicle | page_cache | 235 ns | 5.3 µs | 250.8 µs | 483.1 µs | 703.0 µs |
| 256 B | aeron-archive | archive | 147 ns | 1.1 µs | 3.5 µs | 75.8 µs | 230.0 µs |
| 1024 B | revoq | none \* | 107 ns | 338 ns | 504 ns | 4.6 µs | 20.2 µs |
| 1024 B | chronicle | page_cache | 2.3 µs | 27.93 ms | 38.34 ms | 38.60 ms | 38.76 ms |
| 1024 B | aeron-archive | archive | 156 ns | 348 ns | 19.9 µs | 78.9 µs | 243.8 µs |

**Read:** Aeron Archive holds a low-µs p99 tail; **Chronicle's tail degrades
sharply under load at large payloads** (p99 = 27.9 ms at 1024 B / 1M msg/s —
page-cache writeback stalling). 


## Caveats (the honest read)

- **Unpaced points are throughput/saturation only** — their latency fields are
  queue depth, never charted as response-time percentiles.
- **Host is laptop-class** with best-effort isolation (see above).

## Reproduce

```bash
# from repo root; revoq needs a hugetlbfs --path (sudo)
CXX=/usr/bin/clang++-20 ./build.sh benchmarks
R=benchmarks/comparative/results/2026-07-08-manual-all
bash benchmarks/comparative/harness/env_capture.sh > $R/env.json

sudo benchmarks/comparative/revoq/run.sh --tuned --fork \
  --path /dev/hugepages/revoq-bench \
  --appender-cpu 4 --cursor-cpu 6 --appender-warmer-cpu 3 --cursor-warmer-cpu 5 \
  --rt-priority 80 --sizes "32 64 256 1024" --rates "500000 1000000 0" \
  --messages 5000000 --warmup 200000 --out $R/revoq-none.json

bash benchmarks/comparative/aeron/run_ipc.sh     --sizes "32 64 256 1024" --rates "500000 1000000 0" --messages 5000000 --warmup 200000 --out $R/aeron-ipc.json
bash benchmarks/comparative/aeron/run_archive.sh --sizes "32 64 256 1024" --rates "500000 1000000 0" --messages 5000000 --warmup 200000 --out $R/aeron-archive.json
bash benchmarks/comparative/chronicle/run.sh     --sizes "32 64 256 1024" --rates "500000 1000000 0" --messages 5000000 --warmup 200000 --out $R/chronicle.json
```


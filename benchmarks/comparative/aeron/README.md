<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 RevoRacer -->

# Aeron comparative drivers

Two drivers, split by **semantics** so a chart can never mix incompatible cells:

| Driver | Script | `semantic_group` | `durability_mode` | Compare against |
|--------|--------|------------------|-------------------|-----------------|
| Aeron IPC | `run_ipc.sh` | `volatile_ipc` | `none` | revoq run in an explicitly non-durable mode (tmpfs/hugetlbfs) |
| Aeron Archive | `run_archive.sh` | `journal_replayable` | `archive` | revoq journal, Chronicle Queue (archive vs page_cache/fsync are **different cells**) |

**Aeron OSS only** (`io.aeron`), never Premium/Cluster. Results carry
`system_edition: "oss"`.

## Status: implemented (`run_ipc.sh` / `run_archive.sh` + Gradle/Java)

```sh
# volatile_ipc / durability none
./run_ipc.sh     --sizes "32 64 256 1024" --rates "100000 500000 1000000 0" --out aeron-ipc.json
# journal_replayable / durability archive
./run_archive.sh --sizes "32 64 256 1024" --rates "100000 500000 1000000 0" --out aeron-archive.json
# both honor: --path DIR --messages N --warmup N --producer-core N --consumer-core N
#             AERON_VERSION, AERON_JVM_FLAGS, GRADLE env overrides
```

Each emits a JSON array of [`../schema.md`](../schema.md) points, one per
(size, offered-rate). Validate with `../harness/validate_results.py`.

## Requirements

- **JDK 17+** and **Gradle 8+** on PATH (or `GRADLE=/path/to/gradle`).
- Network access on the first build to fetch `io.aeron:aeron-all` +
  HdrHistogram from Maven Central.

## How it maps to the workload

Implements [`../workload.md`](../workload.md) point for point:

- **1P1C, separate OS processes** over a standalone `MediaDriver`
  (`ArchivingMediaDriver` for the archive) using the native `aeron:ipc` channel —
  no TCP loopback. The run script launches the driver, then the consumer
  (subscription), then the producer (publication), and tears the driver down per
  point.
- **Cross-process clock.** Both processes timestamp with `System.nanoTime()`. On
  Linux/HotSpot that is `CLOCK_MONOTONIC` read directly (no per-process offset),
  so it is comparable across processes on the same host and gives true
  sub-microsecond resolution. An epoch-calibrated clock (agrona's
  `OffsetEpochNanoClock`, Chronicle's `SystemTimeProvider`) was tried first but
  each process calibrates its epoch→nanos offset independently, injecting a large
  **constant skew** (~100–200 µs here) that swamped the real IPC latency —
  `p50==p90` sitting on a fixed pedestal was the tell. `System.nanoTime()` removes
  it. (This holds on Linux; on macOS/Windows `nanoTime()` may carry a per-process
  offset and a host-wide monotonic clock via JNI would be needed instead.)
- **Coordinated omission fixed.** For paced runs the record carries its
  **intended** (scheduled) send time; transport back-pressure is retried without
  restamping, so a slow consumer or a busy archive lands in the tail. →
  `timestamp_point: "intended_send"`. Unpaced (rate `0`) runs stamp actual send
  and are throughput/saturation only.
- **HdrHistogram**, 1 ns–1 s, 3 sig figs, emitted as the base64 "V2" string via
  `encodeIntoCompressedByteBuffer` — byte-compatible with the HdrHistogram-C
  output the revoq driver produces, so all three systems are directly chartable.
  → `latency_ns.hdr_histogram_b64`.
- **Payload sweep** 32/64/256/1024 B (16-byte seq+timestamp header + zero pad).
- **Saturation.** The producer reports `achieved_mps`; the driver fills
  `achieved_rate_mps` and flags `saturated` when a paced point falls >2% short.
- **JVM honesty.** The `jvm` block records JDK vendor/version and GC name; GC
  pause count/max are captured over the **measured window only** (via
  `GarbageCollectionNotificationInfo`) so pauses land in the reported tail.
  `jit_steady_state_confirmed` is the workload's proxy — at least one GC observed
  before the measured window — not asserted blindly.

## Pinning & isolation (read before comparing tails)

`p50`/`p90` are stable on almost any box. The **deep tail (`p99`+) is not** — it
is dominated by scheduling, and getting it right is the whole game. Flags:

| flag | pins | notes |
|------|------|-------|
| `--producer-core N` | producer (1 busy-spin thread) | |
| `--consumer-core N` | consumer (1 busy-spin thread) | |
| `--driver-cores "a,b,c"` | the whole MediaDriver process | DEDICATED = 3 busy-spin threads → give ≥3 cores |
| `--driver-threading SHARED` | folds the driver to 1 thread | use when you can only spare 1 core for it |
| `--rt-priority N` | `chrt -f N` on every process | needs `sudo`/CAP_SYS_NICE |

**Hard-won lessons (measured on a non-isolated box):**

1. **Partial pinning is worse than none.** Pinning producer/consumer to cores 8/9
   but leaving the driver's 3 busy-spin threads floating drove `p99.9` from ~5 µs
   (unpinned) to **2.6 ms** — a floating driver thread periodically lands on a
   pinned busy-spin core and the two fight. Pin *all* threads to *disjoint* cores
   or none.
2. **Pinning to non-isolated cores doesn't help either.** Even with driver/
   producer/consumer on disjoint cores, `p99` stayed in the milliseconds because
   those cores still take timer ticks/IRQs and, without RT priority, the busy-spin
   threads are `SCHED_OTHER` and get preempted.
3. **Clean tails need box-level isolation:** `isolcpus` + `nohz_full` +
   `rcu_nocbs` on the benchmark cores, `irqaffinity` moved off them, turbo and
   deep C-states disabled, then `--rt-priority` under sudo. This is the same
   treatment revoq's `--tuned` mode applies to itself. Do it for every system or
   compare only `p50`/`p90` across systems and state that the tail is un-isolated.

## Archive semantics — stated honestly

Aeron Archive is **not** a drop-in equal of Chronicle/revoq random replay, and
the driver does not pretend otherwise:

- The producer starts a **LOCAL recording** of the stream (`SourceLocation.LOCAL`)
  before publishing — that recording is the durability guarantee, so the run is
  tagged `durability_mode: archive`.
- The consumer subscribes to the **live** stream while recording is active; it
  measures live-subscriber latency and does **not** replay from disk. Chronicle
  and revoq consumers, by contrast, read the persisted log directly.
- The recording subscriber's back-pressure onto the producer *is* felt (open-loop
  retry keeps the intended timestamp), so the cost of durability is not hidden —
  but the read path differs, and charts that place `aeron-archive` beside
  `chronicle-queue`/`revoq` must say so. They are the same `semantic_group`
  (`journal_replayable`) but only ever the **same `durability_mode`** charts
  together, and `archive` is its own durability mode — it never shares a chart
  with `page_cache` or `fsync`.

## Group-2 asymmetry (IPC)

Aeron IPC is a transient shared-memory ring buffer. revoq, even on tmpfs, still
maintains a replayable, queryable journal — strictly more work. The chart must
say so (this is rule 2/`README.md` in the parent directory).

## Typed dispatch

`typed_dispatch` is wired via `run_typed.sh` → `AeronTypedBench`: it encodes a
mixed 3-type stream into a fixed-layout buffer, then times decoding+dispatching
every record through a type switch into a typed handler — the FragmentHandler/SBE
decode shape. It measures the decode+dispatch path over an in-memory buffer (no
live poll), matching the read-back nature of the revoq/Chronicle typed
measurements so all three isolate the same cost. Reports decode throughput
(ns/message) at `durability: none`, charting against revoq's tmpfs (`none`) typed
run. See [`../workload.md`](../workload.md) section 11.

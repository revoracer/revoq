<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 RevoRacer -->

# Chronicle Queue comparative driver

Headline comparison for the `journal_replayable` group: Chronicle Queue is, like
revoq, a persisted, mmap-backed, replayable queue — the fairest apples-to-apples
match. **Chronicle Queue OSS only** (`net.openhft:chronicle-queue`); never
Enterprise. Results carry `system_edition: "oss"`.

## Status: implemented (`run.sh` + Gradle/Java)

```sh
./run.sh --sizes "32 64 256 1024" --rates "100000 500000 1000000 0" --out chronicle.json
# honors: --path DIR --durability page_cache|none|fsync --messages N --warmup N
#         --producer-core N --consumer-core N
#         CHRONICLE_VERSION, CHRONICLE_JVM_FLAGS, GRADLE env overrides
```

Emits a JSON array of [`../schema.md`](../schema.md) points, one per
(size, offered-rate). Validate with `../harness/validate_results.py`.

## Requirements

- **JDK 17+** and **Gradle 8+** on PATH (or `GRADLE=/path/to/gradle`).
- Network access on the first build to fetch Chronicle Queue + HdrHistogram from
  Maven Central.


## How it maps to the workload

Implements [`../workload.md`](../workload.md) point for point:

- **1P1C, two OS processes** over Chronicle's native memory-mapped queue file
  (no TCP). `run.sh` launches `ChronicleBench consumer` then `producer`.
- **Cross-process clock.** Both processes timestamp with `System.nanoTime()` —
  on Linux/HotSpot this is `CLOCK_MONOTONIC` read directly (no per-process
  offset), so it is comparable across processes on the same host at true
  sub-microsecond resolution. Chronicle's epoch-calibrated
  `SystemTimeProvider.currentTimeNanos()` was tried first but injected a large
  constant cross-process skew that swamped the real latency; see the clock note
  in `../aeron/README.md`.
- **Coordinated omission fixed.** For paced runs the record carries its
  **intended** (scheduled) send time; a late producer lands in the tail. →
  `timestamp_point: "intended_send"`. Unpaced (rate `0`) runs stamp actual send
  and are throughput/saturation only.
- **HdrHistogram**, 1 ns–1 s, 3 sig figs, emitted as the base64 "V2" string via
  `encodeIntoCompressedByteBuffer` — byte-compatible with the HdrHistogram-C
  `hdr_log_encode` output the revoq driver produces, so the two are directly
  chartable. → `latency_ns.hdr_histogram_b64`.
- **Payload sweep** 32/64/256/1024 B (16-byte seq+timestamp header + pad).
- **Saturation.** The producer reports `achieved_mps`; the driver fills
  `achieved_rate_mps` and flags `saturated` when a paced point falls >2% short.
- **JVM honesty.** The `jvm` block records JDK vendor/version and GC name;
  GC pause count/max are captured over the **measured window only** (via
  `GarbageCollectionNotificationInfo`) so pauses land in the reported tail.
  `jit_steady_state_confirmed` is set from the workload's proxy — at least one GC
  observed during warmup — not asserted blindly.

## Durability

`durability_mode` defaults to `page_cache` (Chronicle's mmap-to-page-cache
path, no explicit fsync). `--durability fsync` calls `ExcerptAppender.sync()`
per write (requires a Chronicle version exposing it). For the volatile case put
`--path` on tmpfs/hugetlbfs **and** pass `--durability none` (tagged
`volatile_ipc`). Charts may only combine equal durability modes.

## Typed dispatch

`typed_dispatch` is wired via `run_typed.sh` → `ChronicleTypedBench`: it writes a
mixed 3-type stream with a method-writer proxy, then times a `MethodReader`
draining it into typed handler methods — Chronicle's idiomatic typed
decode+dispatch. Reports decode throughput (ns/message) at `durability: page_cache`,
charting against revoq's `page_cache` typed run. See [`../workload.md`](../workload.md)
section 11.

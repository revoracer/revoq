<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 RevoRacer -->

# Canonical comparative workload

This is the **anchor** for every comparative benchmark. All drivers (revoq,
Chronicle, Aeron) implement *this* workload so the only variable is the system
under test. If a driver cannot express something here, that limitation is
recorded in its result rather than silently changing the workload.

## 1. Topology

- Single producer, single consumer (1P1C), pinned to separate physical cores on
  the same NUMA node.
- One logical stream/queue/channel.
- Producer and consumer are separate OS processes communicating via the system's
  native shared-memory / mmap path (no TCP loopback).

## 2. Message schema and payload sizes

- Fixed-size binary records. Each record carries an 8-byte producer timestamp
  plus payload.
- Payload sizes swept: **32, 64, 256, 1024 bytes** (one run per size; size is a
  schema field, never mixed within a chart).
- No compression, no serialization framework beyond each system's native
  fixed-record encoding.

## 3. Counts

- Warmup: **≥ 200,000** messages, discarded, and long enough that JVM JIT has
  reached steady state and at least one GC cycle has occurred (Java drivers must
  confirm this in the result).
- Measured: **≥ 5,000,000** messages per (size, offered_rate) point, or 60 s,
  whichever is larger.

## 4. Latency timestamp location — READ THIS

- The producer timestamp is stamped at the **intended send time**
  (`next_write_time`), **not** the actual send time.
- Latency = `consumer_handler_receive_time − intended_send_time`.
- Rationale: measuring from actual send time hides queueing delay caused by a
  late producer — **coordinated omission**. Response time, not service time, is
  what we report.


> Unpaced (rate 0) runs stamp actual send time and
> report saturation/throughput only — their latency percentiles are queue depth,
> not response time, and are not charted.

## 5. Batching policy

- Producer submits **one record per intended tick** (no application-level
  batching) for the latency runs.
- Any transport-internal batching (e.g. Chronicle write batching, Aeron MTU
  aggregation) is left at each system's documented low-latency default and that
  configuration is recorded verbatim in the result.
- Throughput runs may batch; if so, `batch_size` is recorded and latency is not
  reported from a batched run.

## 6. Durability modes (chart axis, not a footnote)

`durability_mode` is a required schema field. Charts may only combine results
with the **same** durability mode.

| `durability_mode` | Meaning |
|-------------------|---------|
| `none` | Volatile shared memory, no persistence (tmpfs/hugetlbfs, ring buffer) |
| `page_cache` | mmap-persisted; durable to page cache, no explicit fsync |
| `fsync` | Explicit fsync/fdatasync forced on the write path |
| `archive` | Recorded to an archive/replay log with that system's record semantics |

## 7. Load model — open loop

- Latency runs are **open-loop**: the producer targets a fixed **offered rate**
  and does not wait for the consumer. Latency is measured against the intended
  schedule (see §4).
- Offered rates swept per size: **100k, 500k, 1M messages/s, and saturation**
  (unpaced). Latency is reported **as a curve across these rates**, never as a
  single detached percentile.
- `achieved_rate` is recorded alongside `offered_rate`; if `achieved < offered`
  the point is flagged as saturated.

## 8. CPU pinning model

- Producer core, consumer core, and (for JVM) GC/JIT-helper affinity recorded in
  `env.json`.
- Recommended: isolate the producer/consumer cores (`isolcpus` / `nohz_full`),
  disable turbo and deep C-states, and record whether that was done. Results from
  a non-isolated box are still valid but must not be presented as best-case.

## 9. What is reported

Each run reports **both** where applicable, but never conflates them:

- **Throughput**: sustained messages/s and bytes/s (unpaced, may be batched).
- **Latency**: full HdrHistogram (encoded) plus p50/p90/p99/p99.9/p99.99/max, per
  offered rate (open-loop, unbatched).

## 10. Output

Every driver emits one JSON object per run conforming to [`schema.md`](schema.md),
plus GC/safepoint pause stats for JVM systems included in the tail (never warmed
past).

## 11. Typed-dispatch micro (semantic group `typed_dispatch`)

A separate, smaller workload that isolates **decode + dispatch overhead** — the
CPU cost of turning bytes back into typed messages and routing them to a typed
handler. It is a throughput micro (ns/message), **not** a latency curve, so its
results carry a `throughput` block, `load_model: unpaced`, and no
`offered_rate_mps`.

- **Schema:** three message types in an equal round-robin mix, so dispatch must
  branch on type and decode different field sets:

  | type | fields |
  |------|--------|
  | `Trade` | `int64 ts`, `uint32 symbol_id`, `double price` |
  | `Bbo` | `int64 ts`, `uint32 bid_size`, `uint32 ask_size` |
  | `BookReset` | *(none)* |

- **Measurement:** write **N** messages to the system's native medium (untimed),
  then time the consumer-side decode+dispatch of all N through that system's
  idiomatic typed API — revoq `TypedDispatcher`, Chronicle `MethodReader`, Aeron
  fragment decode + type switch. Report `messages_per_s` and derive `ns/message`.
- **Warmup:** JVM systems run a warmup pass to reach JIT steady state and confirm
  it (`jit_steady_state_confirmed`); native revoq needs none (`warmup_count: 0`).
- **Byte encoding differs by system** (Chronicle's self-describing Wire vs the
  fixed structs of revoq/Aeron), so an identical byte layout is neither possible
  nor the point. `message_bytes` records a **nominal** max record size (24) for
  cell-matching only; the headline metric is ns/message. This asymmetry is stated
  on the chart.
- **Durability** tags the write medium. Decode overhead is largely
  medium-independent once the data is hot, so revoq is run on **both**
  `page_cache` and tmpfs `none` to partner Chronicle (`page_cache`, its native
  queue) and Aeron (`none`, its volatile ring). Charts still only combine equal
  durability modes, exactly as for the transport groups.

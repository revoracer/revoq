<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 RevoRacer -->

# Result JSON schema

Every driver emits one JSON object per run. The validator
(`harness/validate_results.py`) checks required fields and enforces that any
results charted together share the same comparison **cell** (see below).

## Schema (version 1)

```jsonc
{
  "schema_version": 1,

  // ---- identity ----
  "system": "revoq",                 // revoq | chronicle-queue | aeron-ipc | aeron-archive
  "system_version": "0.1.0",         // exact version/artifact of the system under test
  "system_edition": "oss",           // oss | enterprise  (must be "oss" for published charts)

  // ---- comparison cell (charts may only combine identical cells) ----
  "semantic_group": "journal_replayable", // journal_replayable | volatile_ipc | typed_dispatch
  "durability_mode": "page_cache",         // none | page_cache | fsync | archive
  "load_model": "open_loop",               // open_loop | closed_loop | unpaced
  "message_bytes": 64,
  "offered_rate_mps": 1000000,             // messages/s targeted; null for unpaced throughput runs

  // ---- workload params (from workload.md) ----
  "topology": "1p1c",
  "warmup_count": 200000,
  "measured_count": 5000000,
  "batch_size": 1,                         // 1 = no app batching (required for latency runs)
  "timestamp_point": "intended_send",      // intended_send | actual_send  (must be intended_send for latency)

  // ---- results ----
  "achieved_rate_mps": 998500,             // measured; if < offered_rate_mps the point is saturated
  "saturated": false,

  "latency_ns": {                          // present for latency runs
    "p50": 420, "p90": 610, "p99": 1180, "p99_9": 3400, "p99_99": 9800, "p99_999": 15200, "max": 21000,
    "hdr_histogram_b64": "…"               // base64 HdrHistogram encoding; source of truth
  },

  "throughput": {                          // present for throughput runs
    "messages_per_s": 12500000,
    "bytes_per_s": 800000000
  },

  "jvm": {                                 // present only for JVM systems; null otherwise
    "jdk_vendor": "temurin",
    "jdk_version": "21.0.3",
    "gc": "ZGC",
    "gc_pause_count": 3,
    "gc_pause_max_ns": 480000,
    "jit_steady_state_confirmed": true
  },

  // ---- provenance ----
  "env_ref": "env.json",                   // env capture this run belongs to
  "started_utc": "2026-07-07T14:03:11Z",
  "notes": ""
}
```

## Required fields

`schema_version`, `system`, `system_version`, `system_edition`, `semantic_group`,
`durability_mode`, `load_model`, `message_bytes`, `topology`, `warmup_count`,
`measured_count`, `timestamp_point`, `env_ref`, and at least one of `latency_ns` /
`throughput`.

## Comparison-cell rule

Two results are **chart-compatible** iff they match on:

```
semantic_group, durability_mode, load_model, message_bytes, offered_rate_mps
```

The validator groups results into cells on these keys and refuses to emit a
combined chart across cells. Latency runs additionally require
`timestamp_point == "intended_send"` and `batch_size == 1`, and published charts
require `system_edition == "oss"`.

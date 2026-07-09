<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 RevoRacer -->

# Comparative benchmarks

This directory holds **comparative** benchmarks that place revoq next to other
low-latency messaging/journal systems on an identical, language-neutral
workload. It is deliberately kept out of the core CMake build: the drivers here
pull in JVM toolchains (Gradle/Maven) and long-running load generators that have
no place in the normal `revoq` build or CI smoke path.

For revoq-only regression benchmarks, see [`../micro/`](../) (CI/smoke safe).

## Design rules (non-negotiable)

These exist so the repo cannot accidentally publish a misleading chart. They are
enforced mechanically by `harness/validate_results.py` where possible.

1. **Compare by semantics, not by product name.** Every result declares a
   `semantic_group` and a `durability_mode`. A chart may only combine results
   that share the same `{semantic_group, durability_mode, load_model,
   message_bytes, offered_rate}` cell. See [`workload.md`](workload.md).

2. **No cross-durability headline charts.** Persisted revoq must never sit beside
   non-persisted Aeron IPC in the same chart. The validator rejects it.

3. **Open-loop latency, measured against intended send time.** Avoids
   coordinated omission. Latency numbers without an attached offered load are not
   published.

4. **Publish raw artifacts, not just charts.** Commit the per-system JSON and the
   `env.json` for every run under a dated results folder. Charts are regenerated
   from these; the artifacts are the source of truth.

5. **OSS editions only, versions pinned.** Chronicle Queue OSS (not Enterprise),
   Aeron OSS (not Premium/Cluster). The exact artifact + version is recorded in
   each result so a reader can obtain and re-run it.

## Semantic groups

| Group | Meaning | Systems |
|-------|---------|---------|
| `journal_replayable` | Persisted, replayable, mmap-backed log | revoq journal, Chronicle Queue, Aeron Archive (archive/replay semantics stated) |
| `volatile_ipc` | Non-durable shared-memory transport | Aeron IPC, revoq only in an explicitly non-durable mode (tmpfs/hugetlbfs), labeled |
| `typed_dispatch` | Typed decode/dispatch overhead | revoq typed dispatch, Chronicle method-reader, Aeron fragment-handler decode |

> **Group 2 asymmetry, stated honestly:** revoq on tmpfs still maintains a
> replayable, queryable journal; Aeron IPC is a transient ring buffer. revoq is
> doing strictly more work in that comparison. We say so on the chart.

## Latest results

- **[`results/2026-07-08-manual-all/`](results/2026-07-08-manual-all/)** —
  all four payload sizes × {500k, 1M, saturation}, on an isolated-core host, with
  the JVM drivers warmed to a **confirmed JIT steady state** (`jit_steady_state_confirmed`).
  `volatile_ipc` (durability `none`), **revoq IPC vs Aeron IPC**: `p50` a tie
  (~100–140 ns both); saturation throughput **~1.9–2.5×** revoq up to 256 B,
  **parity at 1024 B** (both ~12 GB/s, bandwidth-bound); deep tail `p99.9`+
  **~10× tighter** on revoq (revoq sub-µs, warm Aeron low-single-digit µs). Earlier drafts cited a
  millisecond Aeron tail — that was an un-warmed-JIT artifact and has been
  corrected here. The durable `journal_replayable` group (Chronicle `page_cache`,
  Aeron Archive `archive`) is charted separately; Chronicle's tail degrades
  sharply under load at large payloads (p99 into the tens of ms at 1024 B / 1M).

## Layout

```
comparative/
  README.md          this file
  workload.md        the single canonical workload spec (the anchor)
  schema.md          the result JSON schema every driver emits
  revoq/             revoq driver (wraps ../micro p99/throughput binaries)
  chronicle/         Chronicle Queue OSS driver (Gradle) — phase 2
  aeron/             Aeron IPC + Aeron Archive drivers — phase 3
  harness/
    env_capture.sh   captures env.json (CPU, kernel, governor, hugepages, JDK)
    run_all.sh       runs each driver, collects JSON into a dated results dir
    generate_readme_charts.py   regenerates README SVG charts from committed JSON
    validate_results.py   refuses to combine results across incompatible cells
  results/
    YYYY-MM-DD-<host>-<kernel>/
      env.json
      revoq.json
      chronicle.json
      aeron-ipc.json
      aeron-archive.json
```

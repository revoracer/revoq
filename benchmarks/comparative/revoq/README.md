<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 RevoRacer -->

# revoq comparative driver

Wraps the revoq benchmark binaries (built from `../../micro/` with
`-DREVOQ_BUILD_BENCHMARKS=ON`) and re-emits their output as
[`../schema.md`](../schema.md)-conformant JSON for the shared harness.

## Status: working (`run.sh` implemented)

```sh
./run.sh --rates "100000 500000 1000000 0" --out revoq.json
# honors: --path DIR --durability MODE --messages N --warmup N
#         REVOQ_P99_BIN, REVOQ_VERSION env overrides
```

Runs the p99 benchmark once per offered rate and writes a JSON array of
[`../schema.md`](../schema.md) points. Validated by `../harness/validate_results.py`.


### Known limitations

- **Durability labeling is the caller's responsibility.** `--durability` defaults
  to `page_cache` but MUST match the filesystem of `--path`. On tmpfs/hugetlbfs
  pass `--durability none` (the driver then tags the run `volatile_ipc`).
- **Payload sizes are a fixed set** (32/64/256/1024) — each is a compile-time
  instantiation. Add to `kSupportedPayloadBytes` in the benchmark to extend.

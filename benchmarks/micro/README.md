<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 RevoRacer -->

# Micro / regression benchmarks

revoq-only benchmarks that exercise internal paths (journal write/read
throughput, typed dispatch overhead, application-path p99 latency). These are the
CI/smoke-safe benchmarks: single self-contained binaries, no external toolchains.

Built as part of the core CMake project:

```sh
./build.sh benchmarks        # or: cmake -DREVOQ_BUILD_BENCHMARKS=ON ...
```

Each `*.cpp` here becomes a `revoq_benchmark_<name>` executable (see the
`REVOQ_BUILD_BENCHMARKS` block in the top-level `CMakeLists.txt`).

For cross-system comparisons against other messaging/journal systems (Chronicle
Queue, Aeron), see [`../comparative/`](../comparative/) — those live outside the
core build on purpose.

## Notes

- `stream_handler_benchmark_p99.cpp` measures latency from a message's **intended
  send time** (when paced) to the consumer handler, so producer stalls show up in
  the tail rather than being erased (free of coordinated omission). Use
  `--interval-ns 0` only for throughput/saturation, not latency percentiles.

# AGENTS.md

Instructions for coding agents working in `revoq`.

## Project role

`revoq` is an open-source, file-backed, memory-mapped IPC stream for
low-latency same-host messaging and replay. It is a transport/journal primitive,
not a network transport, broker, database, or AI-agent framework.

Use it for local event movement between processes when process isolation,
replayability, and low tail latency matter.

## Build and test

Initialize dependencies before building:

```bash
git submodule update --init --recursive
```

Common commands:

```bash
CXX=/usr/bin/clang++-20 ./build.sh doctor
CXX=/usr/bin/clang++-20 ./build.sh test
CXX=/usr/bin/clang++-20 ./build.sh examples
CXX=/usr/bin/clang++-20 ./build.sh benchmarks
CXX=/usr/bin/clang++-20 ./build.sh python
```

CI validates builds, tests, Python bindings, and committed comparative result
JSON. CI never produces publishable latency numbers.

## Library shape

The public `revoq` API is header-only under `include/revoq/`, but the build
still links compiled support code from `revobase` for mmap, time, hashing, CPU
affinity, realtime scheduling, and file utilities.

For CMake consumers:

```cmake
add_subdirectory(revoq)
target_link_libraries(my_app PRIVATE revoq::revoq)
```

## Core API contract

- `Appender` appends aligned frames to a destination stream.
- `MergingCursor` joins one or more streams and reads committed frames ordered
  by `gen_time`.
- One hot-path writer owns a stream. Multiple independent readers may consume
  without stalling the writer.
- Multi-writer use is serialized by policy; do not describe it as lock-free
  MPSC.
- revoq is same-host IPC over mapped files; do not describe it as a network
  transport.

Publication order matters:

1. Reserve a slot.
2. Fill metadata and payload.
3. Release-store the frame `length` last.
4. Readers acquire-load `length` before reading metadata and payload.

Do not change publication, cursor advancement, frame layout, or recovery
behavior without reading the memory-model and file-format docs first.

## Benchmark rules

- Portable benchmark mode is for functional checks and rough local comparisons.
  Do not use it for host-to-host p99 claims.
- Tuned benchmark mode requires Linux, explicit appender/cursor/appender-warmer/
  cursor-warmer CPUs, isolated cores, realtime priority, and a `hugetlbfs` path.
- Publish tuned results only with the exact command, benchmark output, and host
  capture from `scripts/check_system.sh` and `scripts/collect_benchmark_env.sh`.
- Do not publish latency claims from CI, virtualized runners, dirty source trees,
  missing environment capture, or older topologies that do not match the current
  four-role tuned benchmark.
- Comparative charts must be generated from committed JSON and must not mix
  incompatible semantic or durability groups.

## Files worth reading

- `README.md` - overview, quick start, examples, and benchmark summary.
- `docs/architecture.md` - components and tuned topology.
- `docs/memory_model.md` - publication and visibility rules.
- `docs/file_format.md` - segment and frame layout.
- `docs/benchmarking.md` - benchmark commands and output contract.
- `docs/tuning_overview.md` - tuned host preparation.
- `benchmarks/comparative/README.md` - cross-system benchmark rules.
- `examples/` - runnable C++ examples.
- `python/tests/` - Python binding behavior.

## Editing guidance

- Keep public claims tied to code, tests, docs, or committed benchmark artifacts.
- Add or update tests when behavior changes.
- Preserve the public include layout under `include/revoq/` unless an API change
  is deliberate.
- Keep benchmark output machine-readable and schema-compatible.
- Keep Python bindings aligned with the C++ API contract.

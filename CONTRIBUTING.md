# Contributing

Thanks for improving `revoq`.

## Development Setup

Initialize dependencies and run the test suite before opening a pull request:

```bash
git submodule update --init --recursive
CXX=/usr/bin/clang++-20 ./build.sh test
```

Useful local checks:

```bash
CXX=/usr/bin/clang++-20 ./build.sh doctor
CXX=/usr/bin/clang++-20 ./build.sh examples
CXX=/usr/bin/clang++-20 ./build.sh benchmarks
CXX=/usr/bin/clang++-20 ./build.sh python
```

CI builds and tests correctness only. CI is not a latency environment.

## Pull Requests

- Keep changes focused on one behavior, API, benchmark, or documentation
  concern.
- Add or update tests when behavior changes.
- Preserve the public include layout under `include/revoq/` unless an API change
  is intentional.
- Read `docs/memory_model.md` before changing publication, cursor, or frame
  visibility behavior.
- Read `docs/file_format.md` before changing segment or frame layout.
- Read `docs/benchmarking.md` and `benchmarks/comparative/README.md` before
  changing benchmark output, result schemas, or README benchmark claims.
- Run the relevant build command and include failures if tests cannot be run.

The API is still early and may change, but changes should remain explicit,
reviewable, and backed by tests or benchmark artifacts where appropriate.

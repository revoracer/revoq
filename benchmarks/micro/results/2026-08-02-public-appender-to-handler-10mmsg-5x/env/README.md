# Environment capture

Host capture for `2026-08-02-public-appender-to-handler-10mmsg-5x`.

This directory records:

- local checkout path, hostname, and filesystem paths redacted
- exact CPU model/SKU redacted
- tuned host state for the four-role topology
- hugepage mount and reservation
- isolated CPU set and CPU governors
- memlock setting, compiler, and CMake versions
- git commit and git tree status at capture time

Local host identity, chassis/DMI product identifiers, exact CPU model/SKU, and
filesystem paths are intentionally not published. CPU vendor/class, topology,
kernel, hugepage reservation, and governor state are kept because they are
material to benchmark reproducibility.

## Files

- `benchmark-env.txt` - consolidated host/toolchain capture from
  `scripts/collect_benchmark_env.sh`.
- `check_system.txt` - output from `scripts/check_system.sh`.
- `setup_benchmark_session.txt` - benchmark session tuning command output.
- `cpu-isolated.txt`, `cpu-online.txt`, `cpu-nohz-full.txt` - CPU topology and
  isolation capture.
- `thread-siblings.txt` - SMT sibling sets for the benchmark role CPUs and
  their sibling logical CPUs.
- `meminfo-hugepages.txt` - hugepage reservation state.
- `lscpu.txt` - redacted CPU class and topology.

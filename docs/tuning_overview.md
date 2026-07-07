# Tuning Overview

Low-latency benchmark numbers depend on the host. Revoq's tuned benchmark
exposes the knobs, but it does not silently reconfigure your machine.

## Recommended host properties

- Linux kernel with stable CPU frequency.
- Dedicated appender, cursor, appender-warmer, and cursor-warmer CPUs.
- Minimal interrupt and scheduler noise on benchmark CPUs.
- `hugetlbfs` mounted for the benchmark path.
- Sufficient locked-memory limit.
- Real-time scheduling permission for the benchmark process.

## CPU roles

| role | CLI flag | notes |
| --- | --- | --- |
| appender | `--appender-cpu` | hot append/publish path |
| cursor | `--cursor-cpu` | hot poll/dispatch path |
| appender warmer | `--appender-warmer-cpu` | prepares writable segments |
| cursor warmer | `--cursor-warmer-cpu` | prepares read-only cursor segments |

Use four distinct CPUs. Physical cores are preferred for publishable numbers.
Do not place both warmers on one shared CPU for tuned reference results.

## Session setup

`setup_benchmark_session.sh` applies the session-scoped tuning in one step.
Changes are **not persistent** across reboots.

```bash
sudo ./scripts/setup_benchmark_session.sh \
  --appender-cpu 2 \
  --cursor-cpu 4 \
  --appender-warmer-cpu 6 \
  --cursor-warmer-cpu 8
```

This sets the CPU frequency governor to `performance` for the specified cores,
allocates 2MB hugepages (default: 4096 = 8GB), and mounts `hugetlbfs` at
`/dev/hugepages` if not already mounted.

The setup script accepts only the four explicit role flags shown above.

## Verify

```bash
./scripts/check_system.sh
```

Checks CPU governors, hugepage allocation, hugetlbfs mount, memlock limit, and
whether `chrt` is available for real-time scheduling.

Run this after session setup and include the output when publishing benchmark
results.

## Boot-time tuning

For full scheduler isolation, add the following kernel parameters to your
bootloader (requires reboot):

```
isolcpus=<cpus> nohz_full=<cpus> rcu_nocbs=<cpus>
```

`setup_benchmark_session.sh` prints the exact parameters with your CPU numbers
filled in at the end of its run.

On Ubuntu/Debian: edit `GRUB_CMDLINE_LINUX_DEFAULT` in `/etc/default/grub`,
then run `sudo update-grub` and reboot.

## Capture the environment

Before publishing benchmark results, capture the host configuration:

```bash
./scripts/collect_benchmark_env.sh > env.txt
```

Publish `env.txt` alongside the benchmark output so results are auditable.

## What not to do

Do not run tuned benchmarks on shared production hosts unless you understand
the scheduler and hugepage impact.

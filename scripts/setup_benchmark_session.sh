#!/usr/bin/env bash
set -euo pipefail

appender_cpu=""
cursor_cpu=""
appender_warmer_cpu=""
cursor_warmer_cpu=""
hugepages="4096"
mountpoint="/dev/hugepages"
dry_run="OFF"

usage() {
  cat <<EOF
Usage: sudo ./scripts/setup_benchmark_session.sh --appender-cpu N --cursor-cpu N --appender-warmer-cpu N --cursor-warmer-cpu N [options]

Options:
  --hugepages N          Number of 2MB hugepages to allocate (default: 4096)
  --hugepage-mount DIR   hugetlbfs mount point (default: /dev/hugepages)
  --dry-run              Print actions without applying them
  -h, --help             Show this help
EOF
}

require_value() {
  local name="$1"
  local value="${2:-}"
  if [[ -z "${value}" ]]; then
    echo "${name} requires a value" >&2
    exit 2
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --appender-cpu)
      require_value "$1" "${2:-}"
      appender_cpu="$2"
      shift 2
      ;;
    --appender-cpu=*)
      appender_cpu="${1#*=}"
      shift
      ;;
    --cursor-cpu)
      require_value "$1" "${2:-}"
      cursor_cpu="$2"
      shift 2
      ;;
    --cursor-cpu=*)
      cursor_cpu="${1#*=}"
      shift
      ;;
    --appender-warmer-cpu)
      require_value "$1" "${2:-}"
      appender_warmer_cpu="$2"
      shift 2
      ;;
    --appender-warmer-cpu=*)
      appender_warmer_cpu="${1#*=}"
      shift
      ;;
    --cursor-warmer-cpu)
      require_value "$1" "${2:-}"
      cursor_warmer_cpu="$2"
      shift 2
      ;;
    --cursor-warmer-cpu=*)
      cursor_warmer_cpu="${1#*=}"
      shift
      ;;
    --hugepages)
      require_value "$1" "${2:-}"
      hugepages="$2"
      shift 2
      ;;
    --hugepages=*)
      hugepages="${1#*=}"
      shift
      ;;
    --hugepage-mount)
      require_value "$1" "${2:-}"
      mountpoint="$2"
      shift 2
      ;;
    --hugepage-mount=*)
      mountpoint="${1#*=}"
      shift
      ;;
    --dry-run)
      dry_run="ON"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ -z "${appender_cpu}" || -z "${cursor_cpu}" || -z "${appender_warmer_cpu}" || -z "${cursor_warmer_cpu}" ]]; then
  echo "--appender-cpu, --cursor-cpu, --appender-warmer-cpu, and --cursor-warmer-cpu are required" >&2
  usage >&2
  exit 2
fi

if [[ "${appender_cpu}" == "${cursor_cpu}" || "${appender_cpu}" == "${appender_warmer_cpu}" || "${appender_cpu}" == "${cursor_warmer_cpu}" || "${cursor_cpu}" == "${appender_warmer_cpu}" || "${cursor_cpu}" == "${cursor_warmer_cpu}" || "${appender_warmer_cpu}" == "${cursor_warmer_cpu}" ]]; then
  echo "appender, cursor, appender warmer, and cursor warmer CPUs must be distinct" >&2
  exit 2
fi

if [[ "${dry_run}" != "ON" && "$(id -u)" != "0" ]]; then
  echo "this script changes session-scoped kernel settings; run with sudo or use --dry-run" >&2
  exit 1
fi

run() {
  if [[ "${dry_run}" == "ON" ]]; then
    printf '+'
    printf ' %q' "$@"
    printf '\n'
  else
    "$@"
  fi
}

write_file() {
  local path="$1"
  local value="$2"
  if [[ ! -e "${path}" ]]; then
    echo "skip missing ${path}"
    return
  fi
  if [[ "${dry_run}" == "ON" ]]; then
    printf '+ printf %%s\\\\n %q ' "${value}"
    printf '> %q\n' "${path}"
  else
    printf '%s\n' "${value}" > "${path}"
  fi
}

set_governor() {
  local cpu="$1"
  local path="/sys/devices/system/cpu/cpu${cpu}/cpufreq/scaling_governor"
  if [[ -e "${path}" ]]; then
    echo "cpu${cpu}: scaling_governor=performance"
    write_file "${path}" performance
  else
    echo "cpu${cpu}: no scaling_governor file; skipping"
  fi
}

mounted_hugetlbfs() {
  awk -v mountpoint="${mountpoint}" '$2 == mountpoint && $3 == "hugetlbfs" { found = 1 } END { exit !found }' /proc/mounts
}

echo "setting CPU governors"
set_governor "${appender_cpu}"
set_governor "${cursor_cpu}"
set_governor "${appender_warmer_cpu}"
set_governor "${cursor_warmer_cpu}"

echo "allocating 2MB hugepages: ${hugepages}"
write_file /proc/sys/vm/nr_hugepages "${hugepages}"

echo "ensuring hugetlbfs mount: ${mountpoint}"
run mkdir -p "${mountpoint}"
if mounted_hugetlbfs; then
  echo "${mountpoint} is already hugetlbfs"
else
  run mount -t hugetlbfs none "${mountpoint}" -o pagesize=2M
fi

cpus="${appender_cpu},${cursor_cpu},${appender_warmer_cpu},${cursor_warmer_cpu}"
cat <<EOF

session tuning complete
hugepage_path=${mountpoint}/revoq-bench
boot_parameters=isolcpus=${cpus} nohz_full=${cpus} rcu_nocbs=${cpus}

Run the tuned benchmark with sudo, for example:
sudo ./build/revoq_benchmark_stream_handler_benchmark_p99 --tuned --fork \\
  --path ${mountpoint}/revoq-bench \\
  --appender-cpu ${appender_cpu} --cursor-cpu ${cursor_cpu} \\
  --appender-warmer-cpu ${appender_warmer_cpu} --cursor-warmer-cpu ${cursor_warmer_cpu} \\
  --rt-priority 80 --messages 100000000 --warmup 50000 --interval-ns 100
EOF

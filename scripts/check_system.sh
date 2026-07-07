#!/usr/bin/env bash
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

say() {
  printf '%s\n' "$*"
}

warn() {
  say "warn: $*"
}

value_or_unknown() {
  local path="$1"
  if [[ -r "${path}" ]]; then
    cat "${path}"
  else
    printf 'unknown'
  fi
}

hugetlbfs_mounted() {
  awk '$3 == "hugetlbfs" { found = 1 } END { exit !found }' /proc/mounts
}

say "revoq system check"
say "repo=${ROOT_DIR}"
say "timestamp_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
say "kernel=$(uname -srvmo)"

if [[ -r /proc/cpuinfo ]]; then
  cpu_model="$(awk -F: '/model name/ { sub(/^[ \t]+/, "", $2); print $2; exit }' /proc/cpuinfo)"
  say "cpu_model=${cpu_model:-unknown}"
fi

if command -v nproc >/dev/null 2>&1; then
  say "logical_cpus=$(nproc)"
fi

isolated="$(value_or_unknown /sys/devices/system/cpu/isolated)"
say "isolated_cpus=${isolated:-none}"
if [[ -z "${isolated}" || "${isolated}" == "unknown" ]]; then
  warn "no isolated CPUs reported; tuned latency runs may see scheduler noise"
fi

if compgen -G "/sys/devices/system/cpu/cpu*/cpufreq/scaling_governor" >/dev/null; then
  say "cpu_governors:"
  for path in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do
    cpu="${path%/cpufreq/scaling_governor}"
    cpu="${cpu##*/cpu}"
    if governor="$(cat "${path}" 2>/dev/null)"; then
      say "  cpu${cpu}=${governor}"
    else
      say "  cpu${cpu}=unavailable"
    fi
  done
else
  warn "CPU governor files not available"
fi

if [[ -r /proc/meminfo ]]; then
  awk '
    /HugePages_Total|HugePages_Free|Hugepagesize/ {
      key = tolower($1)
      gsub(":", "", key)
      if (NF >= 3) {
        printf "%s=%s %s\n", key, $2, $3
      } else {
        printf "%s=%s\n", key, $2
      }
    }
  ' /proc/meminfo
fi

if hugetlbfs_mounted; then
  say "hugetlbfs_mounts:"
  awk '$3 == "hugetlbfs" { print "  " $2 }' /proc/mounts
else
  warn "no hugetlbfs mount found; tuned mode needs a hugetlbfs --path"
fi

say "memlock_kb=$(ulimit -l)"

if command -v chrt >/dev/null 2>&1; then
  say "chrt=available"
else
  warn "chrt not found; install util-linux for real-time scheduling checks"
fi

compiler="${CXX:-/usr/bin/clang++-20}"
if command -v "${compiler}" >/dev/null 2>&1; then
  say "compiler=$("${compiler}" --version | head -n 1)"
else
  warn "compiler not found: ${compiler}; set CXX=/path/to/clang++"
fi

if command -v cmake >/dev/null 2>&1; then
  say "cmake=$(cmake --version | head -n 1)"
else
  warn "cmake not found"
fi

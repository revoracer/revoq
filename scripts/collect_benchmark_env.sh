#!/usr/bin/env bash
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

kv() {
  printf '%s=%s\n' "$1" "$2"
}

read_file() {
  local path="$1"
  if [[ -r "${path}" ]]; then
    cat "${path}"
  else
    printf 'unknown'
  fi
}

kv timestamp_utc "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
kv repo "${ROOT_DIR}"

if command -v git >/dev/null 2>&1 && git -C "${ROOT_DIR}" rev-parse --git-dir >/dev/null 2>&1; then
  kv git_commit "$(git -C "${ROOT_DIR}" rev-parse HEAD 2>/dev/null || printf unknown)"
  if [[ -n "$(git -C "${ROOT_DIR}" status --short 2>/dev/null)" ]]; then
    kv git_tree dirty
  else
    kv git_tree clean
  fi
fi

kv kernel "$(uname -srvmo)"
kv hostname "$(hostname 2>/dev/null || printf unknown)"

if [[ -r /proc/cpuinfo ]]; then
  kv cpu_model "$(awk -F: '/model name/ { sub(/^[ \t]+/, "", $2); print $2; exit }' /proc/cpuinfo)"
fi

if command -v nproc >/dev/null 2>&1; then
  kv logical_cpus "$(nproc)"
fi

kv isolated_cpus "$(read_file /sys/devices/system/cpu/isolated)"
kv nohz_full_cpus "$(read_file /sys/devices/system/cpu/nohz_full)"
kv memlock_kb "$(ulimit -l)"

compiler="${CXX:-/usr/bin/clang++-20}"
if command -v "${compiler}" >/dev/null 2>&1; then
  kv compiler "$("${compiler}" --version | head -n 1)"
else
  kv compiler "not-found:${compiler}"
fi

if command -v cmake >/dev/null 2>&1; then
  kv cmake "$(cmake --version | head -n 1)"
fi

if [[ -r /proc/meminfo ]]; then
  awk '
    /MemTotal|HugePages_Total|HugePages_Free|HugePages_Rsvd|HugePages_Surp|Hugepagesize/ {
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

if compgen -G "/sys/devices/system/cpu/cpu*/cpufreq/scaling_governor" >/dev/null; then
  for path in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do
    cpu="${path%/cpufreq/scaling_governor}"
    cpu="${cpu##*/cpu}"
    if governor="$(cat "${path}" 2>/dev/null)"; then
      kv "cpu${cpu}_governor" "${governor}"
    else
      kv "cpu${cpu}_governor" unavailable
    fi
  done
fi

awk '$3 == "hugetlbfs" { printf "hugetlbfs_mount=%s options=%s\n", $2, $4 }' /proc/mounts

if command -v lscpu >/dev/null 2>&1; then
  echo "lscpu_begin"
  lscpu
  echo "lscpu_end"
fi

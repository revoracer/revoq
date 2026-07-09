#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 RevoRacer
#
# Capture the machine environment for a comparative benchmark run into env.json.
# Best-effort: fields that cannot be read are emitted as null with a note.
#
# Usage: env_capture.sh > results/<dated-dir>/env.json

set -euo pipefail

j() { printf '%s' "$1" | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()))'; }
read_or_null() { [ -r "$1" ] && cat "$1" 2>/dev/null | head -1 || echo ""; }

cpu_model=$(grep -m1 'model name' /proc/cpuinfo 2>/dev/null | cut -d: -f2- | sed 's/^ *//' || echo "")
kernel=$(uname -sr)
governor=$(read_or_null /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor)
turbo_no_turbo=$(read_or_null /sys/devices/system/cpu/intel_pstate/no_turbo)
hugepages=$(grep -m1 HugePages_Total /proc/meminfo 2>/dev/null | awk '{print $2}' || echo "")
isolcpus=$(cat /proc/cmdline 2>/dev/null | tr ' ' '\n' | grep -E '^(isolcpus|nohz_full)=' | paste -sd, - || echo "")
jdk=$( (java -version 2>&1 | head -1) 2>/dev/null || echo "")

cat <<EOF
{
  "schema_version": 1,
  "captured_utc": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "host": $(j "$(hostname)"),
  "cpu_model": $(j "$cpu_model"),
  "cpu_count": $(nproc),
  "kernel": $(j "$kernel"),
  "scaling_governor": $(j "$governor"),
  "intel_pstate_no_turbo": $(j "$turbo_no_turbo"),
  "hugepages_total": $(j "$hugepages"),
  "isolation_cmdline": $(j "$isolcpus"),
  "jdk": $(j "$jdk"),
  "notes": "best-effort capture; empty strings mean the source was unreadable"
}
EOF

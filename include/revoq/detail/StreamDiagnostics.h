#pragma once

#include <atomic>
#include <cstdint>

#include <revobase/TimeUtils.h>

namespace revoq {

// X-macro over the cursor-warmer counters that get identical treatment at every
// site (default 0, snapshot = load, reset = store 0). It drives the atomic
// members, CursorWarmerStats, its getStats()/resetStats(), and the Stream copy
// from one list, so adding a counter is a single-line edit instead of five
// hand-synchronized ones. The two non-uniform members
// (warmer_ready_ahead_min_ns, which uses a max() "no data" sentinel, and
// warmer_last_cpu, an int defaulting to -1) are handled explicitly at each
// site.
// clang-format off
#define REVOQ_WARMER_UNIFORM_COUNTERS(X)                                       \
  X(warmer_hits) X(warmer_misses) X(warmer_invalid) X(warmer_signals)          \
  X(warmer_ready) X(warmer_file_timeouts) X(warmer_header_timeouts)            \
  X(warmer_superseded) X(warmer_map_errors) X(warmer_populate_errors)          \
  X(warmer_signal_overwrites) X(warmer_miss_pending) X(warmer_miss_working)    \
  X(warmer_miss_wrong_ready) X(warmer_miss_wrong_working)                      \
  X(warmer_miss_no_request) X(warmer_ready_after_need) X(warmer_cpu_changes)   \
  X(warmer_wake_max_ns) X(warmer_file_wait_max_ns) X(warmer_map_max_ns)        \
  X(warmer_header_wait_max_ns) X(warmer_populate_max_ns)                       \
  X(warmer_signal_to_ready_max_ns) X(warmer_need_to_ready_max_ns)
// clang-format on

struct StreamRuntimeStats {
  uint64_t load_segments{};
  uint64_t slow_loads{};
  uint64_t warmer_hits{};
  uint64_t warmer_misses{};
  uint64_t warmer_invalid{};
  uint64_t warmer_signals{};
  uint64_t warmer_ready{};
  uint64_t warmer_file_timeouts{};
  uint64_t warmer_header_timeouts{};
  uint64_t warmer_superseded{};
  uint64_t warmer_map_errors{};
  uint64_t warmer_populate_errors{};
  uint64_t warmer_signal_overwrites{};
  uint64_t warmer_miss_pending{};
  uint64_t warmer_miss_working{};
  uint64_t warmer_miss_wrong_ready{};
  uint64_t warmer_miss_wrong_working{};
  uint64_t warmer_miss_no_request{};
  uint64_t warmer_ready_after_need{};
  uint64_t warmer_cpu_changes{};
  uint64_t retire_enqueues{};
  uint64_t retire_segments{};
  uint64_t load_max_ns{};
  uint64_t slow_load_max_ns{};
  uint64_t warmer_wake_max_ns{};
  uint64_t warmer_file_wait_max_ns{};
  uint64_t warmer_map_max_ns{};
  uint64_t warmer_header_wait_max_ns{};
  uint64_t warmer_populate_max_ns{};
  uint64_t warmer_signal_to_ready_max_ns{};
  uint64_t warmer_ready_ahead_min_ns{};
  uint64_t warmer_need_to_ready_max_ns{};
  uint64_t retire_max_ns{};
  int warmer_last_cpu{-1};
};

struct AppenderStats {
  uint64_t rotations{};
  uint64_t warmer_hits{};
  uint64_t warmer_misses{};
  uint64_t retire_enqueues{};
  uint64_t retire_segments{};
  uint64_t rotate_max_ns{};
  uint64_t fallback_load_max_ns{};
  uint64_t retire_max_ns{};
};

namespace detail {

inline uint64_t revoqNowNs() {
  return static_cast<uint64_t>(revobase::Timestamp::now().nsec());
}

inline void recordMax(std::atomic<uint64_t> &target, uint64_t value) {
  uint64_t current = target.load(std::memory_order_relaxed);
  while (current < value && !target.compare_exchange_weak(
                                current, value, std::memory_order_relaxed)) {
  }
}

inline void recordMin(std::atomic<uint64_t> &target, uint64_t value) {
  uint64_t current = target.load(std::memory_order_relaxed);
  while (current > value && !target.compare_exchange_weak(
                                current, value, std::memory_order_relaxed)) {
  }
}

} // namespace detail

} // namespace revoq

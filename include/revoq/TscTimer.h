// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 RevoRacer
#pragma once
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <fstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "portable_intrinsics.h"

#ifdef __linux__
#include <pthread.h>
#include <sched.h>
#endif

#if defined(__APPLE__)
#include <TargetConditionals.h>
#include <mach/mach_time.h>
#endif

#if defined(__x86_64__) || defined(_M_X64)
#if defined(__GNUC__) || defined(__clang__)
#include <cpuid.h>
#include <x86intrin.h>
#endif
#endif

namespace racer {
class TscTimer {
public:
  static void calibrate() {
    if (!is_suitable_for_cross_core_timing()) {
      throw std::runtime_error(
          "TSC not suitable for cross-core timing on this CPU. "
          "Missing invariant_tsc, constant_tsc, or nonstop_tsc support.");
    }

#if defined(__x86_64__) || defined(_M_X64)

    const auto t0 = std::chrono::steady_clock::now();
    const uint64_t c0 = rdtscp();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    const uint64_t c1 = rdtscp();
    const auto t1 = std::chrono::steady_clock::now();

    const double ns = std::chrono::duration<double, std::nano>(t1 - t0).count();
    const double ticks_per_ns = (c1 - c0) / ns;

    if (ticks_per_ns < 0.5 || ticks_per_ns > 10.0) {
      throw std::runtime_error(
          "TSC calibration failed: unreasonable ticks_per_ns=" +
          std::to_string(ticks_per_ns));
    }

    ticks_per_ns_.store(ticks_per_ns, std::memory_order_relaxed);

    last_chk_cycles_.store(c1, std::memory_order_relaxed);
    last_chk_steady_.store(
        std::chrono::steady_clock::now().time_since_epoch().count(),
        std::memory_order_relaxed);

    detect_tsc_offset_issue();
#endif
  }

  static inline uint64_t now_cycles_begin() noexcept {
#if defined(__x86_64__) || defined(_M_X64)
    unsigned aux;
    PORTABLE_LFENCE();
    return __rdtscp(&aux);
#elif defined(__APPLE__) && defined(__aarch64__)
    __asm__ __volatile__("dsb sy" ::: "memory");
    return mach_absolute_time();
#else
    return now_ns();
#endif
  }

  static inline uint64_t now_cycles_end() noexcept {
#if defined(__x86_64__) || defined(_M_X64)
    unsigned aux;
    uint64_t tsc = __rdtscp(&aux);
    PORTABLE_LFENCE();
    return tsc;
#elif defined(__APPLE__) && defined(__aarch64__)
    uint64_t t = mach_absolute_time();
    __asm__ __volatile__("dsb sy" ::: "memory");
    return t;
#else
    return now_ns();
#endif
  }

  static inline double delta_ns(uint64_t start, uint64_t end) noexcept {
#if defined(__x86_64__) || defined(_M_X64)
    const double tpn = ticks_per_ns_.load(std::memory_order_relaxed);
    return (end - start) / (tpn > 0.0 ? tpn : 1.0);
#elif defined(__APPLE__) && defined(__aarch64__)
    return static_cast<double>(end - start) * mach_timebase();
#else
    return static_cast<double>(end - start);
#endif
  }

  static inline double delta_us(uint64_t start, uint64_t end) noexcept {
    return delta_ns(start, end) / 1000.0;
  }

  static bool is_suitable_for_cross_core_timing() noexcept {
#if defined(__x86_64__) || defined(_M_X64)
    if (!has_invariant_tsc()) {
      return false;
    }

#ifdef __linux__
    std::ifstream cpuinfo("/proc/cpuinfo");
    if (!cpuinfo.is_open()) {
      return false;
    }

    std::string line;
    bool has_constant_tsc = false;
    bool has_nonstop_tsc = false;

    while (std::getline(cpuinfo, line)) {
      if (line.find("flags") != std::string::npos) {
        has_constant_tsc = line.find("constant_tsc") != std::string::npos;
        has_nonstop_tsc = line.find("nonstop_tsc") != std::string::npos;
        if (has_constant_tsc && has_nonstop_tsc) {
          break;
        }
      }
    }

    return has_constant_tsc && has_nonstop_tsc;
#else
    return true;
#endif

#elif defined(__APPLE__) && defined(__aarch64__)
    return true;
#else
    return true;
#endif
  }

  static bool has_cross_core_offset_issue() noexcept {
#if defined(__x86_64__) || defined(_M_X64)
    return has_tsc_offset_issue_.load(std::memory_order_relaxed);
#else
    return false;
#endif
  }

  static void recalibrate_if_drift(
      double rel_threshold = 0.01,
      std::chrono::milliseconds interval = std::chrono::milliseconds(500)) {
#if defined(__x86_64__) || defined(_M_X64)
    const auto now_sc = std::chrono::steady_clock::now();
    const int64_t now_sc_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            now_sc.time_since_epoch())
            .count();
    const int64_t last_sc_ns = last_chk_steady_.load(std::memory_order_relaxed);
    if (last_sc_ns == 0 ||
        (now_sc_ns - last_sc_ns) < interval.count() * 1'000'000LL)
      return;

    const uint64_t now_cyc = rdtscp();
    const uint64_t last_cyc = last_chk_cycles_.load(std::memory_order_relaxed);

    const double sc_ns = static_cast<double>(now_sc_ns - last_sc_ns);
    const double cyc = static_cast<double>(now_cyc - last_cyc);
    if (sc_ns <= 0.0 || cyc <= 0.0)
      return;

    const double new_tpn = cyc / sc_ns;
    double old_tpn = ticks_per_ns_.load(std::memory_order_relaxed);
    if (old_tpn <= 0.0) {
      ticks_per_ns_.store(new_tpn, std::memory_order_relaxed);
    } else {
      const double drift = std::abs(new_tpn - old_tpn) / old_tpn;
      if (drift > rel_threshold) {
        const double blended = 0.85 * old_tpn + 0.15 * new_tpn;
        ticks_per_ns_.store(blended, std::memory_order_relaxed);
      }
    }
    last_chk_cycles_.store(now_cyc, std::memory_order_relaxed);
    last_chk_steady_.store(now_sc_ns, std::memory_order_relaxed);
#else
    (void)rel_threshold;
    (void)interval;
#endif
  }

private:
  static bool has_invariant_tsc() noexcept {
#if defined(__x86_64__) || defined(_M_X64)
    unsigned int eax = 0, ebx = 0, ecx = 0, edx = 0;
    if (!__get_cpuid_max(0x80000000, nullptr) ||
        __get_cpuid_max(0x80000000, nullptr) < 0x80000007)
      return false;
    __cpuid(0x80000007, eax, ebx, ecx, edx);
    return (edx & (1u << 8)) != 0;
#else
    return false;
#endif
  }

  static void detect_tsc_offset_issue() {
#if defined(__x86_64__) || defined(_M_X64)
#ifdef __linux__
    const int num_cores = std::thread::hardware_concurrency();
    if (num_cores < 2)
      return;

    std::vector<uint64_t> core_timestamps(std::min(num_cores, 4));
    std::vector<std::thread> threads;

    for (size_t i = 0; i < core_timestamps.size(); ++i) {
      threads.emplace_back([&, core = i]() {
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(core, &cpuset);
        pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        core_timestamps[core] = rdtscp();
      });
    }

    for (auto &t : threads) {
      t.join();
    }

    uint64_t min_ts =
        *std::min_element(core_timestamps.begin(), core_timestamps.end());
    uint64_t max_ts =
        *std::max_element(core_timestamps.begin(), core_timestamps.end());
    uint64_t spread = max_ts - min_ts;

    if (spread > 10'000'000'000ULL) {
      has_tsc_offset_issue_.store(true, std::memory_order_relaxed);
    }
#endif
#endif
  }

  // Fallback: high-resolution "now" in nanoseconds
  static inline uint64_t now_ns() noexcept {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
  }

#if defined(__x86_64__) || defined(_M_X64)
  static inline uint64_t rdtscp() noexcept {
    unsigned aux;
    return __rdtscp(&aux);
  }
  inline static std::atomic<double> ticks_per_ns_{1.0};
  inline static std::atomic<uint64_t> last_chk_cycles_{0};
  inline static std::atomic<int64_t> last_chk_steady_{0};
  inline static std::atomic<bool> has_tsc_offset_issue_{false};
#endif

#if defined(__APPLE__) && defined(__aarch64__)
  static inline double mach_timebase() noexcept {
    static std::atomic<double> scale{0.0};
    double s = scale.load(std::memory_order_acquire);
    if (s == 0.0) {
      mach_timebase_info_data_t info{};
      (void)mach_timebase_info(&info);
      s = (info.numer / static_cast<double>(info.denom));
      scale.store(s, std::memory_order_release);
    }
    return s;
  }
#endif
};

} // namespace racer

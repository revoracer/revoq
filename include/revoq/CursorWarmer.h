// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 RevoRacer
#pragma once

#include <atomic>
#include <cassert>
#include <condition_variable>
#include <cstdint>
#include <limits>
#include <mutex>
#include <thread>

#ifdef __linux__
#include <sched.h>
#include <sys/stat.h>
#endif

#include "Segment.h"
#include "StreamBase.h"
#include "detail/StreamPlatform.h"
#include <revobase/CpuAffinity.h>
#include <revobase/MmapBuffer.h>
#include <revobase/PortableIntrinsics.h>
#include <revobase/RealtimePriority.h>

#ifdef REVOQ_ENABLE_DIAGNOSTICS
#include "detail/StreamDiagnostics.h"
#endif

namespace revoq {

#ifdef REVOQ_ENABLE_DIAGNOSTICS
// Warmer-owned subset of the cursor diagnostics. Stream copies these into the
// public StreamRuntimeStats alongside its own load/retire counters.
struct CursorWarmerStats {
#define X(f) uint64_t f{};
  REVOQ_WARMER_UNIFORM_COUNTERS(X)
#undef X
  uint64_t warmer_ready_ahead_min_ns{};
  int warmer_last_cpu{-1};
};
#endif

// cursor-side segment warmer: pre-maps the cursor's next segment read-only with
// MADV_POPULATE_READ so the cursor hot path incurs zero page faults at rotation
// time.
//
// Distinct subsystem from AppenderWarmer (appender): single-slot (not a ring),
// read-only mmap, mlock only on request (lock_pages), and it waits on the
// appender to create the file and publish a valid segment header before
// mapping. Running the loop at SCHED_FIFO 20 ensures MADV_POPULATE_READ for a
// full segment completes well within the ~1.455s rotation period, so the warmed
// segment is always ready before the cursor needs it.
class CursorWarmer {
public:
  CursorWarmer(LocationPtr location, std::uint32_t dest_id,
               std::uint32_t segment_size, int warmer_timeout_ms,
               bool lock_pages = false)
      : location_(std::move(location)), dest_id_(dest_id),
        segment_size_(segment_size), warmer_timeout_ms_(warmer_timeout_ms),
        lock_pages_(lock_pages) {}

  // stop() already joins the thread and releases any unclaimed warmed segment.
  ~CursorWarmer() { stop(); }

  CursorWarmer(const CursorWarmer &) = delete;
  CursorWarmer &operator=(const CursorWarmer &) = delete;

  // Create the warmer thread and pin it from the CALLER thread (not self-pin).
  // Immediate pinning right after creation is critical for avoiding missed
  // rotations.
  void start(int background_worker_cpu) {
    // Idempotent: a second start() must not assign over a live thread_ (which
    // would std::terminate). Mirrors AppenderWarmer::start()'s exchange guard.
    if (running_.exchange(true, std::memory_order_relaxed))
      return;
    {
      // Reopen for work in case this is a restart after a prior stop(). The run
      // mode is resolved AFTER the pin below and published via config_ready_;
      // warmerLoop() blocks until then, so it never picks RT busy-poll before
      // we know whether the requested core actually took.
      std::lock_guard<std::mutex> lock(mutex_);
      stopped_ = false;
      dedicated_ = false;
      config_ready_ = false;
    }
    thread_ = std::thread(&CursorWarmer::warmerLoop, this);
    bool pinned_to_requested = false;
#ifdef __linux__
    const bool pinned = revobase::system::CpuAffinity::pinThreadOrHousekeeping(
        thread_.native_handle(), background_worker_cpu, "cursor warmer thread",
        &pinned_to_requested);
    assert(pinned && "cursor warmer thread affinity pin failed");
    (void)pinned;
#else
    (void)background_worker_cpu;
#endif
    {
      // A dedicated core opts this warmer into busy-poll + SCHED_FIFO. That
      // requires the REQUESTED core to have been pinned: if pinning fell back
      // to shared housekeeping CPUs (or none was requested), stay parked under
      // CFS so we never strand an RT spinner on a shared core. Publish under
      // mutex_ so signalNext() and warmerLoop() read it coherently.
      std::lock_guard<std::mutex> lock(mutex_);
      dedicated_ = pinned_to_requested;
      config_ready_ = true;
    }
    cv_.notify_one();
  }

  // Shutdown + join via the shared lost-notify-safe helper, then fully quiesce.
  //
  // Intended for a single start()/stop() lifecycle per instance. The quiesce
  // runs UNCONDITIONALLY — even when stopAndJoin() reports the instance was
  // never running (e.g. stop() called before start()) — so the contract holds
  // in every ordering: a late signalNext() becomes a no-op, unconsumed intent
  // is dropped, and an unclaimed warmed segment is released so a restart begins
  // genuinely clean. signalNext()/tryLoadWarmedSegment() are public and can
  // race a concurrent caller, so all of this is mutated under mutex_.
  void stop() {
    detail::stopAndJoin(running_, mutex_, cv_, thread_);
    std::lock_guard<std::mutex> lock(mutex_);
    stopped_ = true;
    warmer_pending_ = false;
    pending_flag_.store(false, std::memory_order_relaxed);
    warmer_next_segment_ = -1;
    releaseWarmedSegment();
#ifdef REVOQ_ENABLE_DIAGNOSTICS
    warmer_working_segment_ = -1;
#endif
  }

  // cursor hot-path claim. If the warmer has segment_id ready, claim it
  // (construct the Segment from the pre-populated mapping), clear slot state,
  // and return it. On a miss, return nullptr; the caller falls through to the
  // slow load. Claim, construct, release-on-throw, and hit/invalid/miss
  // accounting all happen under mutex_ so ownership never straddles the API
  // boundary.
  [[nodiscard]] SegmentPtr tryLoadWarmedSegment(int segment_id) {
#ifdef REVOQ_ENABLE_DIAGNOSTICS
    // Diagnostics-only, and intentionally captured here rather than at the
    // Stream loadSegment() entry (as pre-extraction). This shifts
    // warmer_ready_ahead_min_ns / warmer_need_to_ready_max_ns by the small
    // pre-lock interval, so those absolute counters are not comparable across
    // the extraction (e.g. in journal_benchmark_p99 output).
    const uint64_t need_ns = detail::revoqNowNs();
#endif
    SegmentPtr new_segment;
    std::lock_guard<std::mutex> lock(mutex_);
#ifdef REVOQ_ENABLE_DIAGNOSTICS
    warmer_cursor_need_segment_ = segment_id;
    warmer_cursor_need_ns_ = need_ns;
#endif
    if (warmer_ready_id_ == segment_id && warmer_addr_ != 0) {
      try {
        // ~Segment() munlocks only if the warmer locked the mapping.
        new_segment = Segment::loadFromAddress(location_, dest_id_, segment_id,
                                               warmer_addr_, lock_pages_);
#ifdef REVOQ_ENABLE_DIAGNOSTICS
        stat_warmer_hits_.fetch_add(1, std::memory_order_relaxed);
        if (warmer_ready_ns_ != 0 && need_ns >= warmer_ready_ns_)
          detail::recordMin(stat_warmer_ready_ahead_min_ns_,
                            need_ns - warmer_ready_ns_);
#endif
        warmer_addr_ = 0;
        warmer_ready_id_ = -1;
#ifdef REVOQ_ENABLE_DIAGNOSTICS
        warmer_ready_ns_ = 0;
#endif
      } catch (...) {
        // Header not valid yet; release and fall through to lazy load.
#ifdef REVOQ_ENABLE_DIAGNOSTICS
        stat_warmer_invalid_.fetch_add(1, std::memory_order_relaxed);
#endif
        revobase::os::MmapBuffer::releaseMmapBuffer(
            warmer_addr_, segment_size_, lock_pages_,
            revobase::os::MsyncMode::NONE);
        warmer_addr_ = 0;
        warmer_ready_id_ = -1;
#ifdef REVOQ_ENABLE_DIAGNOSTICS
        warmer_ready_ns_ = 0;
#endif
      }
    }
#ifdef REVOQ_ENABLE_DIAGNOSTICS
    if (!new_segment) {
      stat_warmer_misses_.fetch_add(1, std::memory_order_relaxed);
      if (warmer_pending_ && warmer_next_segment_ == segment_id) {
        stat_warmer_miss_pending_.fetch_add(1, std::memory_order_relaxed);
      } else if (warmer_working_segment_ == segment_id) {
        stat_warmer_miss_working_.fetch_add(1, std::memory_order_relaxed);
      } else if (warmer_ready_id_ >= 0 && warmer_ready_id_ != segment_id &&
                 warmer_addr_ != 0) {
        stat_warmer_miss_wrong_ready_.fetch_add(1, std::memory_order_relaxed);
      } else if (warmer_working_segment_ >= 0 &&
                 warmer_working_segment_ != segment_id) {
        stat_warmer_miss_wrong_working_.fetch_add(1, std::memory_order_relaxed);
      } else {
        stat_warmer_miss_no_request_.fetch_add(1, std::memory_order_relaxed);
      }
    }
#endif
    return new_segment;
  }

  // Tell the warmer to begin pre-mapping segment_id (the cursor's next
  // segment). A signal that arrives after stop() is dropped: there is no thread
  // to consume it. (Signalling before start() is fine and pre-queues one
  // segment.)
  void signalNext(int segment_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (stopped_)
      return;
#ifdef REVOQ_ENABLE_DIAGNOSTICS
    if (warmer_pending_)
      stat_warmer_signal_overwrites_.fetch_add(1, std::memory_order_relaxed);
#endif
    warmer_next_segment_ = segment_id;
#ifdef REVOQ_ENABLE_DIAGNOSTICS
    warmer_signal_ns_ = detail::revoqNowNs();
#endif
    warmer_pending_ = true;
#ifdef REVOQ_ENABLE_DIAGNOSTICS
    stat_warmer_signals_.fetch_add(1, std::memory_order_relaxed);
#endif
    // release pairs with the acquire load in warmerLoop's poll.
    pending_flag_.store(true, std::memory_order_release);
    // Dedicated core: no cv_.notify_one() — the warmer busy-polls pending_flag_
    // instead of parking, so the cursor hot thread issues no FUTEX_WAKE at
    // rotation. Parked (shared-core) mode: wake the CV. That costs one
    // FUTEX_WAKE per rotation, which is fine off the dedicated-core hot path
    // and far cheaper than spinning a core. notify under mutex_ (held here) so
    // the parked loop can't miss it.
    if (!dedicated_)
      cv_.notify_one();
  }

#ifdef REVOQ_ENABLE_DIAGNOSTICS
  [[nodiscard]] CursorWarmerStats getStats() const {
    CursorWarmerStats s;
#define X(f) s.f = stat_##f##_.load(std::memory_order_relaxed);
    REVOQ_WARMER_UNIFORM_COUNTERS(X)
#undef X
    const uint64_t ready_ahead_min =
        stat_warmer_ready_ahead_min_ns_.load(std::memory_order_relaxed);
    s.warmer_ready_ahead_min_ns =
        ready_ahead_min == std::numeric_limits<uint64_t>::max()
            ? 0
            : ready_ahead_min;
    s.warmer_last_cpu = stat_warmer_last_cpu_.load(std::memory_order_relaxed);
    return s;
  }

  void resetStats() {
#define X(f) stat_##f##_.store(0, std::memory_order_relaxed);
    REVOQ_WARMER_UNIFORM_COUNTERS(X)
#undef X
    stat_warmer_ready_ahead_min_ns_.store(std::numeric_limits<uint64_t>::max(),
                                          std::memory_order_relaxed);
    stat_warmer_last_cpu_.store(-1, std::memory_order_relaxed);
  }
#endif

private:
  const LocationPtr location_;
  const std::uint32_t dest_id_;
  const std::uint32_t segment_size_;
  const int warmer_timeout_ms_;
  // mlock warmed segments (in this thread, off the cursor hot path).
  const bool lock_pages_;

  // Own shutdown flag — the warmer no longer shares Stream's retire_running_.
  std::atomic<bool> running_{false};
  std::thread thread_;

  // Lock-free wakeup hint for the busy-poll loop. Mirrors warmer_pending_ (set
  // together under mutex_ in signalNext, cleared under mutex_ on consume) but
  // is readable without the lock so the poll never contends signalNext /
  // tryLoadWarmedSegment. cv_ is retained only for stopAndJoin()'s shutdown
  // notify_all (a no-op now that the loop never parks on it).
  std::atomic<bool> pending_flag_{false};

  // Single-slot state, all guarded by mutex_.
  std::mutex mutex_;
  std::condition_variable cv_;
  // Set by stop(), cleared by start(): a stopped instance drops new work
  // instead of queuing intent that no thread will consume.
  bool stopped_{false};
  // True only when the requested core was actually pinned (dedicated core):
  // busy-poll + RT. False (default -1, or a pin that fell back to housekeeping)
  // parks on cv_ under CFS. Resolved in start() after pinning and gated by
  // config_ready_; read in signalNext() and warmerLoop() under mutex_.
  bool dedicated_{false};
  // start() publishes the resolved run mode after pinning; warmerLoop() blocks
  // on cv_ until this is set so it never picks its mode before the pin result.
  bool config_ready_{false};
  int64_t warmer_next_segment_{-1};
  bool warmer_pending_{false};
  std::uintptr_t warmer_addr_{0};
  int64_t warmer_ready_id_{-1};

#ifdef REVOQ_ENABLE_DIAGNOSTICS
  uint64_t warmer_ready_ns_{0};
  uint64_t warmer_signal_ns_{0};
  int64_t warmer_working_segment_{-1};
  int64_t warmer_cursor_need_segment_{-1};
  uint64_t warmer_cursor_need_ns_{0};

#define X(f) std::atomic<uint64_t> stat_##f##_{0};
  REVOQ_WARMER_UNIFORM_COUNTERS(X)
#undef X
  std::atomic<uint64_t> stat_warmer_ready_ahead_min_ns_{
      std::numeric_limits<uint64_t>::max()};
  std::atomic<int> stat_warmer_last_cpu_{-1};

  void recordWarmerCpu() {
#ifdef __linux__
    const int cpu = sched_getcpu();
    if (cpu >= 0) {
      const int previous =
          stat_warmer_last_cpu_.exchange(cpu, std::memory_order_relaxed);
      if (previous >= 0 && previous != cpu)
        stat_warmer_cpu_changes_.fetch_add(1, std::memory_order_relaxed);
    }
#endif
  }
#endif

  // Pre-maps the next segment with MADV_POPULATE_READ (all PTEs established) so
  // that at rotation time the cursor hot path has zero page faults.
  void warmerLoop() {
    {
      // Wait for start() to resolve the run mode from the actual pin result
      // before choosing RT busy-poll vs parked, so a housekeeping-fallback pin
      // never lands us in RT on a shared core.
      std::unique_lock<std::mutex> lock(mutex_);
      cv_.wait(lock, [this] {
        return config_ready_ || !running_.load(std::memory_order_relaxed);
      });
      if (!running_.load(std::memory_order_relaxed))
        return;
    }
    // Elevate priority so MADV_POPULATE_READ always completes within the
    // rotation window (~1.455 s) with margin. ONLY on a dedicated core: on a
    // shared/housekeeping core SCHED_FIFO would starve normal work, and the
    // ~1.455 s of slack makes CFS more than adequate for a ~ms populate.
    if (dedicated_)
      revobase::system::RealtimePriority::setRealtimePriority(
          20, revobase::system::SchedulerPolicy::FIFO);

    while (true) {
      int64_t segment_to_warm;
#ifdef REVOQ_ENABLE_DIAGNOSTICS
      uint64_t signal_ns;
#endif
      {
        std::unique_lock<std::mutex> lock(mutex_, std::defer_lock);
        if (dedicated_) {
          // Dedicated core: busy-poll instead of parking, so signalNext()'s
          // store issues no FUTEX_WAKE. Spin on the lock-free flag without the
          // lock (so the spin never contends
          // signalNext()/tryLoadWarmedSegment()), then take the mutex only once
          // work (or shutdown) is visible.
          while (!pending_flag_.load(std::memory_order_acquire) &&
                 running_.load(std::memory_order_relaxed)) {
            PORTABLE_PAUSE();
          }
          lock.lock();
        } else {
          // Shared/housekeeping core: park on cv_ until signalNext() (or
          // shutdown) wakes us. Consumes no CPU while idle.
          lock.lock();
          cv_.wait(lock, [this] {
            return pending_flag_.load(std::memory_order_acquire) ||
                   !running_.load(std::memory_order_relaxed);
          });
        }
        if (!running_.load(std::memory_order_relaxed))
          break;
        pending_flag_.store(false, std::memory_order_relaxed);
        if (!warmer_pending_)
          // Flag set without live work (e.g. shutdown race). Re-spin.
          continue;
        warmer_pending_ = false;
        segment_to_warm = warmer_next_segment_;
#ifdef REVOQ_ENABLE_DIAGNOSTICS
        signal_ns = warmer_signal_ns_;
        warmer_working_segment_ = segment_to_warm;
#endif
      }

      if (segment_to_warm < 0)
        continue;

#ifdef REVOQ_ENABLE_DIAGNOSTICS
      const uint64_t wake_ns = detail::revoqNowNs();
      if (signal_ns != 0 && wake_ns >= signal_ns)
        detail::recordMax(stat_warmer_wake_max_ns_, wake_ns - signal_ns);
      recordWarmerCpu();
#endif

      auto path = Segment::segmentPath(location_, dest_id_,
                                       static_cast<int>(segment_to_warm));
      // Build the string form once; it is consumed by both the map and the
      // populate calls below.
      const std::string path_str = path.string();

      // Wait for the appender to create and fallocate the file (the appender's
      // AppenderWarmer does this 1-2 segments ahead, so typically exits on the
      // first or second iteration).
      bool file_ready = false;
#ifdef REVOQ_ENABLE_DIAGNOSTICS
      const uint64_t file_wait_t0 = detail::revoqNowNs();
#endif
      for (int ms = 0; ms < warmer_timeout_ms_; ++ms) {
        {
          std::lock_guard<std::mutex> lk(mutex_);
          if (warmer_pending_ || !running_.load(std::memory_order_relaxed)) {
#ifdef REVOQ_ENABLE_DIAGNOSTICS
            detail::recordMax(stat_warmer_file_wait_max_ns_,
                              detail::revoqNowNs() - file_wait_t0);
            if (warmer_pending_)
              stat_warmer_superseded_.fetch_add(1, std::memory_order_relaxed);
#endif
            goto next_signal;
          }
        }
        {
          struct stat st{};
          // stat == 0: the file exists and is reachable
          if (stat(path.c_str(), &st) == 0 &&
              st.st_size == static_cast<off_t>(segment_size_)) {
            file_ready = true;
            break;
          }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
      }
#ifdef REVOQ_ENABLE_DIAGNOSTICS
      detail::recordMax(stat_warmer_file_wait_max_ns_,
                        detail::revoqNowNs() - file_wait_t0);
#endif
      // this means the appender is not active enough to fallocate N+1 in
      // timeout seconds so the warmer essentially self-degrades gracefully
      // under low load
      if (!file_ready) {
#ifdef REVOQ_ENABLE_DIAGNOSTICS
        stat_warmer_file_timeouts_.fetch_add(1, std::memory_order_relaxed);
#endif
        goto next_signal;
      }

      {
        // final double check just in case of the tiny window between stat()
        // and here. always good to do so before expensive work
        std::lock_guard<std::mutex> lk(mutex_);
        if (warmer_pending_ || !running_.load(std::memory_order_relaxed)) {
#ifdef REVOQ_ENABLE_DIAGNOSTICS
          if (warmer_pending_)
            stat_warmer_superseded_.fetch_add(1, std::memory_order_relaxed);
#endif
          goto next_signal;
        }
      }

      {
#ifdef __linux__
        std::uintptr_t addr = 0;
#ifdef REVOQ_ENABLE_DIAGNOSTICS
        const uint64_t map_t0 = detail::revoqNowNs();
#endif
        try {
          addr = revobase::os::MmapBuffer::loadExistingReadOnlyMmapBuffer(path_str,
                                                                segment_size_);
        } catch (const revobase::os::MmapError &) {
#ifdef REVOQ_ENABLE_DIAGNOSTICS
          detail::recordMax(stat_warmer_map_max_ns_,
                            detail::revoqNowNs() - map_t0);
          stat_warmer_map_errors_.fetch_add(1, std::memory_order_relaxed);
#endif
          goto next_signal;
        }
#ifdef REVOQ_ENABLE_DIAGNOSTICS
        detail::recordMax(stat_warmer_map_max_ns_,
                          detail::revoqNowNs() - map_t0);
#endif

        // Wait for the appender's warmer to initialize the segment header.
        // The appender's warmer writes the header AFTER fallocate+mmap+memset,
        // so there is a brief window where the file is the right size but
        // version=0.
        {
          const auto *hdr = reinterpret_cast<const SegmentHeader *>(addr);
          bool header_ok = false;
#ifdef REVOQ_ENABLE_DIAGNOSTICS
          const uint64_t header_wait_t0 = detail::revoqNowNs();
#endif
          for (int ms = 0; ms < warmer_timeout_ms_; ++ms) {
            {
              std::lock_guard<std::mutex> lk(mutex_);
              if (warmer_pending_ ||
                  !running_.load(std::memory_order_relaxed)) {
#ifdef REVOQ_ENABLE_DIAGNOSTICS
                detail::recordMax(stat_warmer_header_wait_max_ns_,
                                  detail::revoqNowNs() - header_wait_t0);
                if (warmer_pending_)
                  stat_warmer_superseded_.fetch_add(1,
                                                    std::memory_order_relaxed);
#endif
                revobase::os::MmapBuffer::releaseMmapBuffer(addr, segment_size_, false,
                                                  revobase::os::MsyncMode::NONE);
                goto next_signal;
              }
            }
            std::atomic_thread_fence(std::memory_order_acquire);
            if (hdr->version == SEGMENT_VERSION &&
                hdr->segment_header_length == sizeof(SegmentHeader)) {
              header_ok = true;
              break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
          }
#ifdef REVOQ_ENABLE_DIAGNOSTICS
          detail::recordMax(stat_warmer_header_wait_max_ns_,
                            detail::revoqNowNs() - header_wait_t0);
#endif
          if (!header_ok) {
#ifdef REVOQ_ENABLE_DIAGNOSTICS
            stat_warmer_header_timeouts_.fetch_add(1,
                                                   std::memory_order_relaxed);
#endif
            revobase::os::MmapBuffer::releaseMmapBuffer(addr, segment_size_, false,
                                              revobase::os::MsyncMode::NONE);
            goto next_signal;
          }
        }
        // Note:
        // Page tables belong to the process, so PTEs the warmer thread builds are
        // used by the cursor thread too. The TLB, though, is per core, and the warmer
        // runs on a different core. So the cursor's core still takes a TLB miss on its
        // first touch of each page. That's a hardware walk(<100 ns),
        // much cheaper than a fault.

        // Unmapping can disturb other cores. When a mapping is removed
        // (the retire thread's munmap), other cores may still cache translations
        // for it in their TLBs. The kernel sends them an interrupt to flush those
        // entries, which is called a TLB shootdown. So even an munmap on a
        // housekeeping thread can briefly interrupt the cursor's core.
        // Doing retire off the hot thread avoids the cost of the munmap itself,
        // but not necessarily that interrupt.

        // mlock only on request (lock_pages): removing faults only needs the PTEs,
        // while mlock(128MB) costs ~20ms. That cost runs on this thread, but mlock
        // briefly takes the mmap lock, which can delay page faults on other threads.
        // HugeTLB segments are already unswappable.

        // Establish all PTEs now, ~1.455s before the cursor needs them. After
        // this call the cursor hot path incurs only a TLB miss (~100 ns) per
        // 4 KB region on first access, not a soft page fault (2-10 µs).
#ifdef REVOQ_ENABLE_DIAGNOSTICS
        const uint64_t populate_t0 = detail::revoqNowNs();
#endif
        try {
          revobase::os::MmapBuffer::prepareReadOnlyMmapBuffer(
              path_str, addr, segment_size_,
              {.populate_read = true,
               .advise_hugepage = true,
               .lock_pages = lock_pages_});
        } catch (const revobase::os::MmapError &) {
#ifdef REVOQ_ENABLE_DIAGNOSTICS
          stat_warmer_populate_errors_.fetch_add(1, std::memory_order_relaxed);
#endif
          revobase::os::MmapBuffer::releaseMmapBuffer(addr, segment_size_, false,
                                            revobase::os::MsyncMode::NONE);
          goto next_signal;
        }
#ifdef REVOQ_ENABLE_DIAGNOSTICS
        const uint64_t populate_dt = detail::revoqNowNs() - populate_t0;
        detail::recordMax(stat_warmer_populate_max_ns_, populate_dt);
        const uint64_t ready_ns = detail::revoqNowNs();
#endif

        {
          std::lock_guard<std::mutex> lk(mutex_);
          if (warmer_pending_ || !running_.load(std::memory_order_relaxed)) {
#ifdef REVOQ_ENABLE_DIAGNOSTICS
            if (warmer_pending_)
              stat_warmer_superseded_.fetch_add(1, std::memory_order_relaxed);
#endif
            revobase::os::MmapBuffer::releaseMmapBuffer(
                addr, segment_size_, lock_pages_,
                revobase::os::MsyncMode::NONE);
            goto next_signal;
          }
          if (warmer_addr_ != 0)
            revobase::os::MmapBuffer::releaseMmapBuffer(
                warmer_addr_, segment_size_, lock_pages_,
                revobase::os::MsyncMode::NONE);
          warmer_addr_ = addr;
          warmer_ready_id_ = segment_to_warm;
#ifdef REVOQ_ENABLE_DIAGNOSTICS
          warmer_ready_ns_ = ready_ns;
          if (signal_ns != 0 && ready_ns >= signal_ns)
            detail::recordMax(stat_warmer_signal_to_ready_max_ns_,
                              ready_ns - signal_ns);
          if (warmer_cursor_need_segment_ == segment_to_warm &&
              warmer_cursor_need_ns_ != 0 &&
              ready_ns >= warmer_cursor_need_ns_) {
            stat_warmer_ready_after_need_.fetch_add(1,
                                                    std::memory_order_relaxed);
            detail::recordMax(stat_warmer_need_to_ready_max_ns_,
                              ready_ns - warmer_cursor_need_ns_);
          }
          stat_warmer_ready_.fetch_add(1, std::memory_order_relaxed);
#endif
        }
#endif // __linux__
      }
    next_signal:;
#ifdef REVOQ_ENABLE_DIAGNOSTICS
      {
        std::lock_guard<std::mutex> lk(mutex_);
        if (warmer_working_segment_ == segment_to_warm) {
          warmer_working_segment_ = -1;
        }
      }
#endif
    }
  }

  void releaseWarmedSegment() {
    if (warmer_addr_ != 0) {
      revobase::os::MmapBuffer::releaseMmapBuffer(
          warmer_addr_, segment_size_, lock_pages_,
          revobase::os::MsyncMode::NONE);
      warmer_addr_ = 0;
      warmer_ready_id_ = -1;
#ifdef REVOQ_ENABLE_DIAGNOSTICS
      warmer_ready_ns_ = 0;
#endif
    }
  }
};

using CursorWarmerPtr = std::unique_ptr<CursorWarmer>;

} // namespace revoq

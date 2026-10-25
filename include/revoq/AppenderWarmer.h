// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 RevoRacer
#pragma once

#include <array>
#include <atomic>
#include <condition_variable>
#include <cstring>
#include <memory>
#include <mutex>
#include <pthread.h>
#include <sched.h>
#include <stdexcept>
#include <thread>
#include <unistd.h>

#include "Frame.h"
#include "Segment.h"
#include "StreamBase.h"
#include "detail/StreamPlatform.h"

#include <revobase/CpuAffinity.h>
#include <revobase/MmapBuffer.h>
#include <revobase/RealtimePriority.h>
#include <revobase/TimeUtils.h>
#include <revobase/PortableIntrinsics.h>

#include <spdlog/spdlog.h>

namespace revoq {

// Unified file-backed segment warmer for Appender
// Pre-warms actual stream file segments to eliminate page faults
template <size_t RingSize = 16> class AppenderWarmer {
public:
  static_assert((RingSize & (RingSize - 1)) == 0,
                "RingSize must be power of 2");

  struct SegmentSlot {
    std::atomic<int64_t> segment_id{-1}; // -1 = empty, >=0 = valid
    std::uintptr_t address{0};           // File-backed mmap address
    std::size_t size{0};                 // Segment size
    std::atomic<uint64_t> sequence{0};   // For ordering/versioning
    bool is_owned{false};                // Do we need to release this mmap?
    char padding[64 - sizeof(std::atomic<int64_t>) - sizeof(std::uintptr_t) -
                 sizeof(std::size_t) - sizeof(std::atomic<uint64_t>) -
                 sizeof(bool)]{};
  } __attribute__((aligned(64)));

  AppenderWarmer(LocationPtr location, uint32_t dest_id, size_t segment_size,
                 bool is_appender, int warmer_cpu_core = -1,
                 size_t warmup_distance = 1, bool lock_pages = false)
      : location_(std::move(location)), dest_id_(dest_id),
        segment_size_(segment_size), is_appender_(is_appender),
        lock_pages_(lock_pages), warmer_cpu_core_(warmer_cpu_core),
        warmup_distance_(std::min(warmup_distance, RingSize / 2)),
        running_(false), write_index_(0), read_index_(0), current_segment_(-1) {
    SPDLOG_INFO("AppenderWarmer created for {}/{:08x} ({}) - ring size: {}",
                location_->uname, dest_id_,
                is_appender_ ? "APPENDER" : "CURSOR", RingSize);
  }

  ~AppenderWarmer() {
    stop();
    releaseAllSegments();
  }

  void start() {
    if (running_.exchange(true, std::memory_order_acquire)) {
      return;
    }
    {
      std::lock_guard<std::mutex> lock(wake_mutex_);
      config_ready_ = false;
    }
    warmer_thread_ = std::thread(&AppenderWarmer::warmupLoop, this);
    // Block until warmupLoop() has self-pinned and published its run mode, so
    // the appender hot path's updateCurrentSegment() reads a stable dedicated_
    // (and never skips the wake for a warmer that actually parked).
    {
      std::unique_lock<std::mutex> lock(wake_mutex_);
      wake_cv_.wait(lock, [this] { return config_ready_; });
    }
    SPDLOG_INFO("AppenderWarmer started");
  }

  void stop() {
    // Lost-notify-safe shutdown shared with the cursor warmer: flip running_
    // under wake_mutex_ so a concurrent worker can't miss the wake and park
    // forever.
    if (detail::stopAndJoin(running_, wake_mutex_, wake_cv_, warmer_thread_)) {
      SPDLOG_INFO("AppenderWarmer stopped");
    }
  }

  // LOCK-FREE HOT PATH: Get pre-warmed file-backed segment
  // Returns file-backed mmap address if available, 0 if not ready
  __attribute__((always_inline)) inline std::uintptr_t
  getWarmedSegment(int64_t segment_id, std::size_t &size_out) {
    const uint64_t start_read = read_index_.load(std::memory_order_acquire);
    for (size_t i = 0; i < RingSize; ++i) {
      const size_t idx = (start_read + i) & (RingSize - 1);
      SegmentSlot &slot = ring_[idx];
      int64_t slot_segment_id = slot.segment_id.load(std::memory_order_acquire);
      if (slot_segment_id == segment_id) {
        if (slot.segment_id.compare_exchange_strong(
                slot_segment_id, -1, std::memory_order_acq_rel)) {
          size_out = slot.size;
          std::uintptr_t addr = slot.address;
          // Mark as not owned by warmer anymore (caller owns it now)
          // the full chain: warmer allocates -> appender claims ->
          // Segment owns -> retire thread releases
          slot.is_owned = false;
          // Move read index forward
          read_index_.store((idx + 1) & (RingSize - 1),
                            std::memory_order_release);
          return addr;
        }
      }
    }
    return 0;
  }

  // Update current segment (called by appender, per segment-rotation).
  inline void updateCurrentSegment(int64_t segment_id) {
    // Dedicated core: a plain release store — no lock, no notify — so the
    // appender hot thread issues no FUTEX_WAKE at rotation. The warmer observes
    // this on its next poll (acquire load of current_segment_).
    current_segment_.store(segment_id, std::memory_order_release);
    // Parked (shared-core) warmer: wake it. Costs one FUTEX_WAKE per rotation,
    // acceptable off the dedicated-core hot path. notify under wake_mutex_ so
    // the parked loop (which re-checks current_segment_ under the lock) can't
    // miss it. The store above is release-ordered before the mutex, so the
    // waiter sees the new segment when it re-evaluates its predicate.
    if (!dedicated_.load(std::memory_order_acquire)) {
      std::lock_guard<std::mutex> lock(wake_mutex_);
      wake_cv_.notify_one();
    }
  }

  struct Stats {
    size_t segments_in_ring;
    uint64_t write_index;
    uint64_t read_index;
    int64_t current_segment;
  };

  Stats getStats() const {
    Stats stats;
    stats.write_index = write_index_.load(std::memory_order_relaxed);
    stats.read_index = read_index_.load(std::memory_order_relaxed);
    stats.current_segment = current_segment_.load(std::memory_order_relaxed);
    stats.segments_in_ring = 0;
    for (size_t i = 0; i < RingSize; ++i) {
      if (ring_[i].segment_id.load(std::memory_order_relaxed) >= 0) {
        ++stats.segments_in_ring;
      }
    }
    return stats;
  }

private:
  const LocationPtr location_;
  const uint32_t dest_id_;
  const size_t segment_size_;
  const bool is_appender_;
  // mlock warmed segments (in this thread, off the appender hot path).
  const bool lock_pages_;
  const int warmer_cpu_core_;
  const size_t warmup_distance_;
  // A dedicated core opts this warmer into the busy-poll + SCHED_FIFO hot path.
  // Resolved in warmupLoop() AFTER the self-pin: true only when an explicit
  // core was requested AND actually pinned. A failed pin
  // (bad/offline/disallowed CPU) stays parked on wake_cv_ under CFS so we never
  // strand an RT spinner on a shared core. Read by updateCurrentSegment() on
  // the appender hot thread, so atomic; publication is gated by config_ready_
  // (start() blocks on it).
  std::atomic<bool> dedicated_{false};

  std::atomic<bool> running_;

  alignas(64) std::atomic<uint64_t> write_index_{};
  alignas(64) std::atomic<uint64_t> read_index_{};
  alignas(64) std::atomic<int64_t> current_segment_{};

  alignas(64) std::array<SegmentSlot, RingSize> ring_;

  std::thread warmer_thread_;
  std::mutex wake_mutex_;
  std::condition_variable wake_cv_;
  // warmupLoop() publishes the resolved run mode (dedicated_) after
  // self-pinning and sets this; start() blocks until it is true so the appender
  // hot path's updateCurrentSegment() always reads a stable dedicated_. Guarded
  // by wake_mutex_.
  bool config_ready_{false};

  void releaseAllSegments() {
    for (size_t i = 0; i < RingSize; ++i) {
      SegmentSlot &slot = ring_[i];
      if (slot.is_owned && slot.address != 0) {
        revobase::os::MmapBuffer::releaseMmapBuffer(
            slot.address, slot.size, lock_pages_,
            revobase::os::MsyncMode::NONE);
        slot.address = 0;
        slot.is_owned = false;
      }
    }
  }

  // Wait until pred() holds or shutdown. On a dedicated core, busy-poll (no
  // futex, needs 100% of the core). Otherwise park on wake_cv_ (idle-friendly;
  // updateCurrentSegment()/stop() notify). Returns with running_ observed;
  // callers still re-check it.
  template <class Pred> void warmerWait(Pred pred) {
    if (dedicated_.load(std::memory_order_acquire)) {
      while (running_.load(std::memory_order_acquire) && !pred())
        PORTABLE_PAUSE();
    } else {
      std::unique_lock<std::mutex> lock(wake_mutex_);
      wake_cv_.wait(lock, [&] {
        return !running_.load(std::memory_order_acquire) || pred();
      });
    }
  }

  void warmupLoop() {
    // Resolve the run mode from the ACTUAL self-pin result: dedicated
    // (busy-poll
    // + RT) only when an explicit core was requested and actually took. A
    // failed pin (bad/offline/disallowed CPU) landed us on some shared core, so
    // stay parked — an RT spinner there would starve normal work (the misconfig
    // footgun). RT is best-effort on top of a good pin (unprivileged still
    // busy-polls the dedicated core under CFS).
    bool dedicated = false;
    if (warmer_cpu_core_ >= 0) {
      if (revobase::system::CpuAffinity::pinToCore(warmer_cpu_core_)) {
        dedicated = true;
        if (!revobase::system::RealtimePriority::setRealtimePriority(50))
          SPDLOG_ERROR("warmer: failed to set RT priority");
        else
          SPDLOG_DEBUG("Warmer thread pinned to CPU {} with RT priority {}",
                       warmer_cpu_core_, 50);
      } else {
        SPDLOG_ERROR("warmer: failed to pin to core {}; running parked "
                     "(no RT/busy-poll)",
                     warmer_cpu_core_);
      }
    }
    {
      // Publish the mode and release start()'s handshake.
      std::lock_guard<std::mutex> lock(wake_mutex_);
      dedicated_.store(dedicated, std::memory_order_release);
      config_ready_ = true;
    }
    wake_cv_.notify_one();
    SPDLOG_DEBUG("Warmer loop started");
    // Dedicated core: busy-poll, never park — updateCurrentSegment()/stop() are
    // plain atomic stores with no notify, so the appender hot thread pays no
    // FUTEX_WAKE at rotation. A spinning thread also sidesteps the nohz_full +
    // idle=poll hazard that ruled out sleep_for here (a sleeper's wake hrtimer
    // may not be serviced on a quiet isolated core; a spinner observes the
    // store immediately). Without a dedicated core (warmer_cpu_core=-1)
    // warmerWait() parks on wake_cv_ instead of spinning a shared core at 100%.
    warmerWait([this] {
      return current_segment_.load(std::memory_order_acquire) >= 0;
    });
    uint64_t local_seq = 1;
    while (running_.load(std::memory_order_acquire)) {
      int64_t current_segment =
          current_segment_.load(std::memory_order_acquire);
      if (current_segment < 0) {
        warmerWait([this] {
          return current_segment_.load(std::memory_order_acquire) >= 0;
        });
        continue;
      }
      bool did_work = false;
      for (size_t i = 1; i <= warmup_distance_; ++i) {
        if (!running_.load(std::memory_order_acquire)) {
          break;
        }
        int64_t next_segment_id = current_segment + static_cast<int64_t>(i);
        if (isSegmentInRing(next_segment_id)) {
          continue;
        }
        uint64_t write_idx = write_index_.load(std::memory_order_acquire);
        size_t slot_idx = write_idx & (RingSize - 1);
        SegmentSlot &slot = ring_[slot_idx];
        int64_t expected = -1;
        // sentinel states: -1 = empty, -2 = being warmed, >=0 = ready
        if (!slot.segment_id.compare_exchange_strong(
                expected, -2, std::memory_order_acq_rel)) {
          continue;
        }
        try {
          if (warmSegmentIntoSlot(next_segment_id, slot, local_seq++)) {
            did_work = true;
            write_index_.fetch_add(1, std::memory_order_release);
          } else {
            // Benign: the appender caught up and now owns this segment while
            // the warm was in flight (saturation). Release the reserved slot
            // quietly — warmSegmentIntoSlot already unmapped. Not an error.
            slot.segment_id.store(-1, std::memory_order_release);
            SPDLOG_DEBUG("Skipped stale warm of segment {} (appender already "
                         "owns it)",
                         next_segment_id);
          }
        } catch (const std::exception &e) {
          SPDLOG_ERROR("Failed to warm segment {}: {}", next_segment_id,
                       e.what());
          slot.segment_id.store(-1, std::memory_order_release);
        }
      }
      cleanupOldSegments(current_segment);
      if (!did_work) {
        // Warmup distance filled for this current_segment; wait until the
        // appender rotates (current_segment_ changes) or shutdown.
        warmerWait([this, current_segment] {
          return current_segment_.load(std::memory_order_acquire) !=
                 current_segment;
        });
      }
    }
    SPDLOG_DEBUG("Warmer loop exited");
  }

  [[nodiscard]] bool isSegmentInRing(int64_t segment_id) const {
    for (size_t i = 0; i < RingSize; ++i) {
      if (ring_[i].segment_id.load(std::memory_order_acquire) == segment_id) {
        return true;
      }
    }
    return false;
  }

  // Returns true if the segment was warmed and published into the slot; false
  // if the warm was abandoned because the appender already owns the segment
  // (a benign saturation outcome — see the ownership gate below). Throws only
  // on genuine failures (mmap error, corrupt existing header).
  bool warmSegmentIntoSlot(int64_t segment_id, SegmentSlot &slot,
                           uint64_t seq) {
    std::string segment_file =
        Segment::makeSegmentPath(location_, dest_id_,
                                 static_cast<int>(segment_id))
            .string();
    assert(!segment_file.empty());
    const auto t0 = revobase::Timestamp::now();
    const bool file_existed = std::filesystem::exists(segment_file);
    SPDLOG_DEBUG("AppenderWarmer warming segment {} ({})", segment_id,
                 segment_file);
    // Load ACTUAL FILE-BACKED mmap (not anonymous!)
    // This is the key difference - we're warming the real stream file
    std::uintptr_t addr = 0;
    try {
      addr = revobase::os::MmapBuffer::loadMmapBuffer(
          segment_file, segment_size_,
          is_appender_,      // true for appender, false for cursor
          {.populate = true, // fault the whole segment in NOW, off the hot path
           .lock_pages = lock_pages_,
           // zero_new_file=false: the warmer does NOT own this segment yet. We
           // only settle ownership below via the current_segment_ re-check,
           // and do our own (guarded) zeroing after it. Letting loadMmapBuffer
           // bulk-memset a just-created file here would run BEFORE that check
           // and could clobber a writer that fell back onto this same segment.
           .zero_new_file = false});
    } catch (...) {
      if (is_appender_) {
        std::error_code ec;
        if (std::filesystem::exists(segment_file, ec) &&
            std::filesystem::file_size(segment_file, ec) == 0) {
          // we don't want to pollute the file system so let's clean up.
          std::filesystem::remove(segment_file, ec);
        }
      }
      throw;
    }

    // MmapBuffer::loadMmapBuffer already does:
    // - MAP_POPULATE: pre-faults all segments
    // - MADV_HUGEPAGE: requests transparent huge page promotion
    // - hugetlbfs-backed files: use huge pages from the filesystem
    // - mlock: locks segments in RAM, when lock_pages_
    // - Segment touching: reads (and writes for new files)
    //
    // For appender: Opens file with O_RDWR, mmaps with PROT_READ|PROT_WRITE
    // For cursor: Opens file with O_RDONLY, mmaps with PROT_READ
    if (is_appender_) {
      // Ownership gate: never (re)initialize or zero a segment the appender has
      // already reached. Under saturation the warmer can fall behind and the
      // writer can claim this segment (via rotateSegment's synchronous
      // fallback) while this warm is still in flight; the header init + memset
      // below would clobber frames the appender is concurrently committing.
      // current_segment_ is advanced before the writer touches a segment, and
      // loadMmapBuffer was told NOT to zero new files (zero_new_file=false), so
      // this check gates the ONLY zeroing path — nothing has written to the
      // mapping before here.
      //
      // This is an expected saturation outcome, not a failure: unmap and return
      // false so the caller drops the slot quietly (no error log).
      if (segment_id <= current_segment_.load(std::memory_order_acquire)) {
        revobase::os::MmapBuffer::releaseMmapBuffer(
            addr, segment_size_, lock_pages_, revobase::os::MsyncMode::NONE);
        return false;
      }
      auto *header = reinterpret_cast<SegmentHeader *>(addr);
      if (!file_existed || header->last_frame_position == 0) {
        header->version = SEGMENT_VERSION;
        header->segment_header_length = sizeof(SegmentHeader);
        header->segment_size = static_cast<uint32_t>(segment_size_);
        header->frame_header_length = sizeof(FrameHeader);
        header->last_frame_position = sizeof(SegmentHeader);
        // zero out rest of segment
        std::memset(reinterpret_cast<void *>(addr + sizeof(SegmentHeader)), 0,
                    segment_size_ - sizeof(SegmentHeader));
        // ensure writes are visible
        std::atomic_thread_fence(std::memory_order_release);
      } else if (header->version != SEGMENT_VERSION ||
                 header->segment_header_length != sizeof(SegmentHeader) ||
                 header->segment_size != static_cast<uint32_t>(segment_size_)) {
        revobase::os::MmapBuffer::releaseMmapBuffer(
            addr, segment_size_, lock_pages_, revobase::os::MsyncMode::NONE);
        throw std::runtime_error(
            "existing warmed segment has invalid header: " + segment_file);
      }
    }
    slot.address = addr;
    slot.size = segment_size_;
    slot.is_owned = true; // Warmer owns this mmap
    slot.sequence.store(seq, std::memory_order_release);
    slot.segment_id.store(segment_id, std::memory_order_release);
    const auto t1 = revobase::Timestamp::now();
    SPDLOG_INFO("AppenderWarmer warmed segment {} in {:.3f} ms", segment_id,
                std::chrono::duration<double, std::milli>(t1 - t0).count());
    SPDLOG_TRACE("Warmed file-backed segment {} in {} ns", segment_id,
                 (t1 - t0).count());
    return true;
  }
  /*
    * Ring buffer with 16 slots:
    Appender is currently on segment 50

    Ring contains:
    Slot 0: segment 35  ← Very old!
    Slot 1: segment 36  ← Old
    Slot 2: segment 48  ← Recent
    Slot 3: segment 49  ← Recent
    Slot 4: segment 50  ← Current (appender is here)
    Slot 5: segment 51  ← Pre-warmed (next)
    Slot 6: segment 52  ← Pre-warmed
    ...

    Problem: Segments 35, 36 are too far behind!
    - Appender will never go back to segment 35
    - Slot is wasted holding old segment
    - Memory/file descriptor leak if not cleaned up
   */
  void cleanupOldSegments(int64_t current_segment) {
    // this is for the case when appender advances warmer many segments
    const int64_t cleanup_threshold =
        current_segment - static_cast<int64_t>(RingSize);
    for (size_t i = 0; i < RingSize; ++i) {
      SegmentSlot &slot = ring_[i];
      int64_t slot_segment_id = slot.segment_id.load(std::memory_order_acquire);
      if (slot_segment_id >= 0 && slot_segment_id < cleanup_threshold) {
        /*
         * Why CAS here?
        * // Thread A (warmupLoop calling cleanup):
        slot_segment_id = 33  // Load from slot
        // Check: 33 < 34? Yes, should clean up
        // Try to set slot to -1 (empty)
        // Thread B (cursor calling getWarmedSegment):
        // At the SAME time, tries to claim segment 33:
        slot.segment_id.compare_exchange_strong(33, -1);  // Claiming it
        // Only ONE thread wins!
        // Winner gets to act, loser moves on
        */
        if (slot.segment_id.compare_exchange_strong(
                slot_segment_id, -1, std::memory_order_acq_rel)) {
          // Release the file-backed mmap
          if (slot.is_owned && slot.address != 0) {
            revobase::os::MmapBuffer::releaseMmapBuffer(
                slot.address, slot.size, lock_pages_,
                revobase::os::MsyncMode::NONE);
            slot.address = 0;
            slot.is_owned = false;
          }
          SPDLOG_TRACE("Cleaned up old segment {} from ring", slot_segment_id);
        }
      }
    }
  }
};

using AppenderWarmerPtr = std::shared_ptr<AppenderWarmer<>>;

} // namespace revoq

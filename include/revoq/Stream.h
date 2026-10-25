// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 RevoRacer
#pragma once

#include <cassert>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <filesystem>
#include <memory>
#include <mutex>
#include <thread>
#include <unistd.h>
#include <utility>

#include "CursorWarmer.h"
#include "Frame.h"
#include "Segment.h"
#include "StreamBase.h"
#include "detail/StreamPlatform.h"

#include <revobase/CpuAffinity.h>
#include <revobase/TimeUtils.h>

#ifdef REVOQ_ENABLE_DIAGNOSTICS
#include "detail/StreamDiagnostics.h"
#endif

namespace revoq {

struct StreamOptions {
  bool is_writing;
  SegmentPopulate populate = SegmentPopulate::Background;
  // mlock segments while mapped. Not with Lazy.
  bool lock_pages = false;
  bool background_threads = true;
  size_t retained_segment_limit = 0;
  int warmer_timeout_ms = 5000;
  // Warmer busy-polls (dedicated core); retire timed-polls (housekeeping core,
  // -1 => housekeeping fallback). See CursorWarmer / retireLoop.
  int warmer_cpu = -1;
  int retire_cpu = -1;
};

class Stream {
public:
  Stream(revoq::LocationPtr location, std::uint32_t dest_id,
         StreamOptions options)
      : location_(std::move(location)), dest_id_(dest_id),
        is_writing_(options.is_writing), populate_(options.populate),
        lock_pages_(options.lock_pages),
        background_threads_(options.background_threads),
        max_retained_segments_(options.retained_segment_limit),
        warmer_timeout_ms_(options.warmer_timeout_ms),
        warmer_cpu_(options.warmer_cpu), retire_cpu_(options.retire_cpu),
        stream_dir_(location_->locator->layoutDir(location_, Layout::STREAM)),
        segment_size_(findSegmentSize(location_, dest_id_)),
        frame_(std::shared_ptr<Frame>(new Frame())), segment_frame_nb_(0) {
    validateSegmentPopulate(populate_, lock_pages_, background_threads_);
    if (!background_threads_)
      return;

    retire_running_.store(true, std::memory_order_relaxed);
    retire_thread_ = std::thread(&Stream::retireLoop, this);

#ifdef __linux__
    const bool retire_pinned =
        revobase::system::CpuAffinity::pinThreadOrHousekeeping(
            retire_thread_.native_handle(), retire_cpu_,
            "cursor retire thread");
    assert(retire_pinned && "cursor retire thread affinity pin failed");
#endif

    // Cursor warmer: pre-maps the next segment with MADV_POPULATE_READ so the
    // cursor hot path incurs zero page faults at rotation time. start() creates
    // the thread and pins it immediately from this thread, which is critical
    // for avoiding missed rotations.
    if (populate_ == SegmentPopulate::Background) {
      warmer_ = std::make_unique<CursorWarmer>(
          location_, dest_id_, segment_size_, warmer_timeout_ms_, lock_pages_);
      warmer_->start(warmer_cpu_);
    }
  }

  ~Stream() {
    // Stop the warmer first: it owns and releases its warmed mapping in its own
    // destructor. stop() is lost-notify-safe internally.
    warmer_.reset();
    {
      std::lock_guard<std::mutex> lock(retire_mutex_);
      if (current_segment_)
        retire_queue_.push_back(std::move(current_segment_));
      // let's not stall the current thread.
      // let the retire thread own the actual munmap work
      while (!retired_segments_.empty()) {
        retire_queue_.push_back(std::move(retired_segments_.front()));
        retired_segments_.pop_front();
      }
      retire_running_.store(false, std::memory_order_relaxed);
    }
    retire_cv_.notify_all();
    if (retire_thread_.joinable())
      retire_thread_.join();
  }

  Frame *getCurrentFrameRaw() noexcept {
    return isDataAvailable() ? frame_.get() : nullptr;
  }

  [[nodiscard]] bool isDataAvailable() const {
    return current_segment_ && frame_->hasData();
  }

#ifdef REVOQ_ENABLE_DIAGNOSTICS
  [[nodiscard]] StreamRuntimeStats getStats() const {
    StreamRuntimeStats s;
    s.load_segments = stat_load_segments_.load(std::memory_order_relaxed);
    s.slow_loads = stat_slow_loads_.load(std::memory_order_relaxed);
    s.retire_enqueues = stat_retire_enqueues_.load(std::memory_order_relaxed);
    s.retire_segments = stat_retire_segments_.load(std::memory_order_relaxed);
    s.load_max_ns = stat_load_max_ns_.load(std::memory_order_relaxed);
    s.slow_load_max_ns = stat_slow_load_max_ns_.load(std::memory_order_relaxed);
    s.retire_max_ns = stat_retire_max_ns_.load(std::memory_order_relaxed);

    // Warmer-owned counters (empty when no warmer, i.e. populate is not
    // Background — matching the pre-extraction all-zero state).
    if (warmer_) {
      const CursorWarmerStats w = warmer_->getStats();
#define X(f) s.f = w.f;
      REVOQ_WARMER_UNIFORM_COUNTERS(X)
#undef X
      s.warmer_ready_ahead_min_ns = w.warmer_ready_ahead_min_ns;
      s.warmer_last_cpu = w.warmer_last_cpu;
    }
    return s;
  }

  void resetStats() {
    stat_load_segments_.store(0, std::memory_order_relaxed);
    stat_slow_loads_.store(0, std::memory_order_relaxed);
    stat_retire_enqueues_.store(0, std::memory_order_relaxed);
    stat_retire_segments_.store(0, std::memory_order_relaxed);
    stat_load_max_ns_.store(0, std::memory_order_relaxed);
    stat_slow_load_max_ns_.store(0, std::memory_order_relaxed);
    stat_retire_max_ns_.store(0, std::memory_order_relaxed);
    if (warmer_)
      warmer_->resetStats();
  }
#endif

  void next() {
    if (!current_segment_)
      return;
    next(frame_->getFrameLength(), static_cast<MsgType>(frame_->getMsgType()));
  }

  void next(std::uint32_t frame_length, MsgType msg_type) {
    if (!current_segment_)
      return;
    if (msg_type == MsgType::SegmentEnd) {
      loadNextSegment();
    } else {
      frame_->moveToNext(frame_length);
      ++segment_frame_nb_;
    }
  }

  void seekToTime(std::int64_t timestamp) {
    int segment_id = Segment::findSegmentId(location_, dest_id_, timestamp);
    loadSegment(segment_id);
    if (!current_segment_)
      return;
    SPDLOG_TRACE(
        "seeking timestamp {} in segment [{}] [beginTime({}) - endTime({})]",
        revobase::Timestamp(timestamp), segment_id,
        revobase::Timestamp(current_segment_->beginTime()),
        revobase::Timestamp(current_segment_->endTime()));
    while (current_segment_->isFull() &&
           current_segment_->endTime() < timestamp) {
      loadNextSegment();
    }
    while (frame_->hasData() && frame_->getGenTime() < timestamp) {
      next();
    }
  }

private:
  const revoq::LocationPtr location_;
  const std::uint32_t dest_id_;
  const bool is_writing_;
  const SegmentPopulate populate_;
  const bool lock_pages_;
  const bool background_threads_;
  const size_t max_retained_segments_;
  const int warmer_timeout_ms_;
  const int warmer_cpu_;
  const int retire_cpu_;
  const std::filesystem::path stream_dir_;
  const std::uint32_t segment_size_;
  SegmentPtr current_segment_;
  FramePtr frame_;
  int segment_frame_nb_;

  std::deque<SegmentPtr> retired_segments_;

  std::mutex retire_mutex_;
  std::condition_variable retire_cv_;
  std::deque<SegmentPtr> retire_queue_;
  std::atomic<bool> retire_running_{false};
  std::thread retire_thread_;

  // Cursor warmer: owns its thread, single-slot handoff state, and warmer
  // diagnostics. Null unless populate is Background.
  CursorWarmerPtr warmer_;

#ifdef REVOQ_ENABLE_DIAGNOSTICS
  std::atomic<uint64_t> stat_load_segments_{0};
  std::atomic<uint64_t> stat_slow_loads_{0};
  std::atomic<uint64_t> stat_retire_enqueues_{0};
  std::atomic<uint64_t> stat_retire_segments_{0};
  std::atomic<uint64_t> stat_load_max_ns_{0};
  std::atomic<uint64_t> stat_slow_load_max_ns_{0};
  std::atomic<uint64_t> stat_retire_max_ns_{0};

  static uint64_t nowNs() { return detail::revoqNowNs(); }

  static void recordMax(std::atomic<uint64_t> &target, uint64_t value) {
    detail::recordMax(target, value);
  }
#endif

  void retireLoop() {
    // Timed-poll instead of parking on a producer notify: the cursor hot thread
    // enqueues WITHOUT notifying (no FUTEX_WAKE on the rotation path), and this
    // thread drains on a short timer. Retire tolerates the <= kPollInterval
    // latency — at ~2 s/rotation it leaves at most one extra segment mapped for
    // a few ms. retire_cv_ is kept only so the destructor's notify_all wakes us
    // immediately at shutdown (no teardown stall). Runs on a housekeeping core,
    // so it must NOT busy-spin.
    constexpr auto kPollInterval = std::chrono::milliseconds(10);
    while (true) {
      std::unique_lock<std::mutex> lock(retire_mutex_);
      retire_cv_.wait_for(lock, kPollInterval, [this] {
        return !retire_queue_.empty() ||
               !retire_running_.load(std::memory_order_relaxed);
      });
      while (!retire_queue_.empty()) {
        SegmentPtr segment = std::move(retire_queue_.front());
        retire_queue_.pop_front();
        lock.unlock();
#ifdef REVOQ_ENABLE_DIAGNOSTICS
        const uint64_t t0 = nowNs();
#endif
        segment.reset();
#ifdef REVOQ_ENABLE_DIAGNOSTICS
        const uint64_t dt = nowNs() - t0;
        stat_retire_segments_.fetch_add(1, std::memory_order_relaxed);
        recordMax(stat_retire_max_ns_, dt);
#endif
        lock.lock();
      }
      if (!retire_running_.load(std::memory_order_relaxed))
        break;
    }
  }

  [[nodiscard]] bool segmentFileExists(int segment_id) const {
    // Build from the cached stream_dir_ rather than Segment::segmentPath(),
    // which would recompute layoutDir() on every rotation.
    const auto path =
        stream_dir_ / Segment::segmentFileName(dest_id_, segment_id);
    return access(path.c_str(), F_OK) == 0;
  }

  void clearCurrentSegment() {
    current_segment_.reset();
    segment_frame_nb_ = 0;
  }

  void loadSegment(int segment_id) {
    if (!current_segment_ || current_segment_->getSegmentId() != segment_id) {
#ifdef REVOQ_ENABLE_DIAGNOSTICS
      const uint64_t load_t0 = nowNs();
#endif
      SegmentPtr new_segment;
      // Fast path: use the warmer's pre-populated mapping (zero page faults).
      // tryLoadWarmedSegment claims + constructs the Segment under its own lock
      // and records hit/invalid/miss diagnostics; returns null on a miss.
      if (warmer_)
        new_segment = warmer_->tryLoadWarmedSegment(segment_id);
      // Slow path: the warmer had nothing ready, so map the segment on this thread.
      // Inline populates it all now (one stall, then no faults). Lazy, and a
      // Background warmer miss, map without populating: each page soft-faults on
      // first access instead.
      if (!new_segment) {
        if (!is_writing_ && !segmentFileExists(segment_id)) {
          clearCurrentSegment();
          return;
        }
#ifdef REVOQ_ENABLE_DIAGNOSTICS
        stat_slow_loads_.fetch_add(1, std::memory_order_relaxed);
        const uint64_t slow_t0 = nowNs();
#endif
        new_segment =
            Segment::load(location_, dest_id_, segment_id, is_writing_,
                          segmentMapOptions(populate_, lock_pages_));
#ifdef REVOQ_ENABLE_DIAGNOSTICS
        const uint64_t slow_dt = nowNs() - slow_t0;
        recordMax(stat_slow_load_max_ns_, slow_dt);
#endif
      }
#ifdef REVOQ_ENABLE_DIAGNOSTICS
      stat_load_segments_.fetch_add(1, std::memory_order_relaxed);
#endif
      SegmentPtr old_segment = std::move(current_segment_);
      current_segment_ = std::move(new_segment);
      frame_->setAddress(current_segment_->firstFrameAddress());
      segment_frame_nb_ = 0;
      if (old_segment) {
        if (background_threads_) {
          retired_segments_.push_back(std::move(old_segment));
          while (retired_segments_.size() > max_retained_segments_) {
            SegmentPtr retired_segment = std::move(retired_segments_.front());
            retired_segments_.pop_front();
            {
              std::lock_guard<std::mutex> lock(retire_mutex_);
              retire_queue_.push_back(std::move(retired_segment));
            }
#ifdef REVOQ_ENABLE_DIAGNOSTICS
            stat_retire_enqueues_.fetch_add(1, std::memory_order_relaxed);
#endif
            // No retire_cv_.notify_one(): the retire thread timed-polls the
            // queue, so the cursor hot thread issues no FUTEX_WAKE here.
          }
        }
        // if !background_threads_: old_segment destroyed inline here
      }
      // Signal the warmer to start pre-mapping the next segment (cursor only).
      if (warmer_)
        warmer_->signalNext(segment_id + 1);
#ifdef REVOQ_ENABLE_DIAGNOSTICS
      const uint64_t load_dt = nowNs() - load_t0;
      recordMax(stat_load_max_ns_, load_dt);
#endif
    }
  }

  void loadNextSegment() {
    if (!current_segment_)
      return;
    loadSegment(current_segment_->getSegmentId() + 1);
  }

  friend class MergingCursor;
  template <typename> friend class Appender;
};

using StreamPtr = std::shared_ptr<Stream>;

} // namespace revoq

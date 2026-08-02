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

#include <revobase/TimeUtils.h>

namespace racer::revoq {

struct StreamOptions {
  bool is_writing;
  bool prefault;
  bool background_threads = true;
  size_t retained_segment_limit = 0;
  int warmer_timeout_ms = 5000;

  int warmer_cpu = -1;
  int retire_cpu = -1;
};

class Stream {
public:
  Stream(revoq::LocationPtr location, std::uint32_t dest_id,
         StreamOptions options)
      : location_(std::move(location)), dest_id_(dest_id),
        is_writing_(options.is_writing), prefault_(options.prefault),
        background_threads_(options.background_threads),
        max_retained_segments_(options.retained_segment_limit),
        warmer_timeout_ms_(options.warmer_timeout_ms),
        warmer_cpu_(options.warmer_cpu), retire_cpu_(options.retire_cpu),
        stream_dir_(location_->locator->layoutDir(location_, Layout::STREAM)),
        segment_size_(findSegmentSize(location_, dest_id_)),
        frame_(std::shared_ptr<Frame>(new Frame())), segment_frame_nb_(0) {
    if (!background_threads_)
      return;

    retire_running_.store(true, std::memory_order_relaxed);
    retire_thread_ = std::thread(&Stream::retireLoop, this);

#ifdef __linux__
    const bool retire_pinned = detail::pinThreadToBackgroundCpus(
        retire_thread_.native_handle(), retire_cpu_, "cursor retire thread");
    assert(retire_pinned && "cursor retire thread affinity pin failed");
#endif

    if (!prefault_) {
      warmer_ = std::make_unique<CursorWarmer>(
          location_, dest_id_, segment_size_, warmer_timeout_ms_);
      warmer_->start(warmer_cpu_);
    }
  }

  ~Stream() {

    warmer_.reset();
    {
      std::lock_guard<std::mutex> lock(retire_mutex_);
      if (current_segment_)
        retire_queue_.push_back(std::move(current_segment_));

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
  const bool prefault_;
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

  CursorWarmerPtr warmer_;

  void retireLoop() {

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
        segment.reset();
        lock.lock();
      }
      if (!retire_running_.load(std::memory_order_relaxed))
        break;
    }
  }

  [[nodiscard]] bool segmentFileExists(int segment_id) const {

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
      SegmentPtr new_segment;

      if (warmer_)
        new_segment = warmer_->tryLoadWarmedSegment(segment_id);

      if (!new_segment) {
        if (!is_writing_ && !segmentFileExists(segment_id)) {
          clearCurrentSegment();
          return;
        }
        new_segment = Segment::load(location_, dest_id_, segment_id,
                                    is_writing_, prefault_);
      }
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
          }
        }
      }

      if (warmer_)
        warmer_->signalNext(segment_id + 1);
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

} // namespace racer::revoq

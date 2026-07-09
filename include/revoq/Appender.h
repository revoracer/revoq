// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 RevoRacer
#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>

#include "AppenderWarmer.h"
#include "Frame.h"
#include "Segment.h"
#include "StreamBase.h"
#include "TimeUtils.h"
#include "detail/StreamPlatform.h"

#include <spdlog/spdlog.h>

#include "portable_intrinsics.h"

namespace racer::revoq {

struct SingleAppenderPolicy {
  static constexpr bool serialize = false;
};
struct MultiAppenderPolicy {
  static constexpr bool serialize = true;
};

inline constexpr size_t DEFAULT_STREAM_APPENDER_RETAINED_SEGMENTS = 10;
inline constexpr size_t kAppenderWarmerRingSize = 16;

struct AppenderOptions {
  bool prefault = true;
  size_t warmup_segments = 0;
  int warmer_cpu_core = -1;

  int retire_cpu = -1;
  bool background_threads = true;
  size_t retained_segment_limit = DEFAULT_STREAM_APPENDER_RETAINED_SEGMENTS;
};

template <bool Serialize> struct WriteLockGuard;

template <> struct WriteLockGuard<false> {
  explicit WriteLockGuard(std::atomic<bool> &) noexcept {}
};

template <> struct WriteLockGuard<true> {
  explicit WriteLockGuard(std::atomic<bool> &flag) noexcept : flag_(flag) {
    int backoff = 1;
    while (flag_.exchange(true, std::memory_order_acquire)) {
      for (int i = 0; i < backoff; ++i)
        PORTABLE_PAUSE();
      backoff = std::min(backoff * 2, 64);
    }
  }
  ~WriteLockGuard() { flag_.store(false, std::memory_order_release); }

private:
  std::atomic<bool> &flag_;
};

template <typename AppenderPolicy = SingleAppenderPolicy> class Appender {
public:
  Appender(LocationPtr location, uint32_t dest_id, AppenderOptions options = {})
      : location_(std::move(location)), dest_id_(dest_id),
        prefault_(options.prefault),
        background_threads_(options.background_threads),
        max_retained_segments_(options.retained_segment_limit),
        segment_size_(findSegmentSize(location_, dest_id_)), writing_(false),
        current_sequence_(0), current_segment_id_(0) {

    const auto last_segment_id = findLastWrittenSegment();
    if (last_segment_id > 0) {
      resumeFromSegment(last_segment_id);
    } else {
      SPDLOG_INFO("No existing segments found, starting fresh from segment 1");
    }
    if (!current_segment_)
      loadInitialSegment();
    if (options.warmup_segments > 0) {
      if (options.warmup_segments > kAppenderWarmerRingSize) {
        throw std::invalid_argument(
            "warmup_segments exceeds appender segment warmer ring size");
      }
      appender_warmer_ =
          std::make_shared<AppenderWarmer<kAppenderWarmerRingSize>>(
              location_, dest_id_, segment_size_, true, options.warmer_cpu_core,
              options.warmup_segments);
      appender_warmer_->start();
      appender_warmer_->updateCurrentSegment(
          current_segment_id_.load(std::memory_order_relaxed));
      SPDLOG_DEBUG("Appender created with AppenderWarmer ({} segments ahead)",
                   options.warmup_segments);
    }

    if (background_threads_) {
      retire_running_.store(true, std::memory_order_relaxed);
      retire_thread_ = std::thread(&Appender::retireLoop, this);
#ifdef __linux__

      detail::pinThreadToBackgroundCpus(retire_thread_.native_handle(),
                                        options.retire_cpu,
                                        "appender retire thread");
#endif
    }
  }

  template <typename T>
  T &openData(int64_t timestamp, MsgType msg_type,
              uint16_t flags = FRAME_NORMAL) {
    if constexpr (AppenderPolicy::serialize) {
      int backoff = 1;
      while (writing_.exchange(true, std::memory_order_acquire)) {
        for (int i = 0; i < backoff; ++i)
          PORTABLE_PAUSE();
        backoff = std::min(backoff * 2, 64);
      }
    }
    if (!current_segment_) {
      rotateSegment();
    }
    open_frame_header_ = openFrame(timestamp, msg_type, sizeof(T), flags);
    return *reinterpret_cast<T *>(open_frame_header_ + 1);
  }

  template <typename T> void closeData() {
    closeData<T>([](FrameHeader &) noexcept {});
  }

  template <typename T, typename BeforePublish>
  void closeData(BeforePublish &&before_publish) {
    if (!open_frame_header_) [[unlikely]] {
      if constexpr (AppenderPolicy::serialize)
        writing_.store(false, std::memory_order_release);
      throw std::runtime_error("No frame opened");
    }
    try {
      closeFrame(open_frame_header_, sizeof(T),
                 std::forward<BeforePublish>(before_publish));
      open_frame_header_ = nullptr;
    } catch (...) {
      if constexpr (AppenderPolicy::serialize)
        writing_.store(false, std::memory_order_release);
      throw;
    }
    if constexpr (AppenderPolicy::serialize)
      writing_.store(false, std::memory_order_release);
  }

  ~Appender() {
    SPDLOG_DEBUG("Appender teardown begin");
    if (appender_warmer_) {
      appender_warmer_->stop();
      appender_warmer_.reset();
    }
    current_segment_.reset();

    if (background_threads_) {
      std::lock_guard<std::mutex> lock(retire_mutex_);
      while (!retired_segments_.empty()) {
        retire_queue_.push_back(std::move(retired_segments_.front()));
        retired_segments_.pop_front();
      }
      retire_running_.store(false, std::memory_order_relaxed);
    }
    retire_cv_.notify_all();
    if (retire_thread_.joinable())
      retire_thread_.join();
    retired_segments_.clear();
    SPDLOG_DEBUG("Appender teardown complete");
  }

  void mark(int64_t timestamp, MsgType msg_type) {
    WriteLockGuard<AppenderPolicy::serialize> lock(writing_);
    if (!current_segment_) {
      rotateSegment();
    }
    auto *frame_hdr = openFrame(timestamp, msg_type, 0, FRAME_NORMAL);
    closeFrame(frame_hdr, 0);
  }

  void writeBytes(int64_t timestamp, MsgType msg_type, const void *data,
                  std::size_t size) {
    WriteLockGuard<AppenderPolicy::serialize> lock(writing_);
    if (!current_segment_)
      rotateSegment();
    auto *hdr = openFrame(timestamp, msg_type, size, FRAME_NORMAL);
    if (size > 0)
      std::memcpy(hdr + 1, data, size);
    closeFrame(hdr, size);
  }

  [[nodiscard]] uint64_t getCurrentSequence() const {
    return current_sequence_.load(std::memory_order_acquire);
  }
  [[nodiscard]] int64_t getCurrentSegmentId() const {
    return current_segment_id_.load(std::memory_order_acquire);
  }

private:
  const LocationPtr location_;
  const uint32_t dest_id_;
  const bool prefault_;
  const bool background_threads_;
  const size_t max_retained_segments_;
  const uint32_t segment_size_;
  std::atomic<bool> writing_;
  std::atomic<uint64_t> current_sequence_;
  std::atomic<int> current_segment_id_;

  SegmentPtr current_segment_{nullptr};
  FrameHeader *open_frame_header_{nullptr};
  uintptr_t current_frame_address_{};

  AppenderWarmerPtr appender_warmer_;
  std::deque<SegmentPtr> retired_segments_;

  std::mutex retire_mutex_;
  std::condition_variable retire_cv_;
  std::deque<SegmentPtr> retire_queue_;
  std::atomic<bool> retire_running_{false};
  std::thread retire_thread_;

  uint64_t nextSeq() {
    if constexpr (AppenderPolicy::serialize) {
      return current_sequence_.fetch_add(1, std::memory_order_relaxed);
    } else {
      uint64_t s = current_sequence_.load(std::memory_order_relaxed);
      current_sequence_.store(s + 1, std::memory_order_relaxed);
      return s;
    }
  }

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

  [[nodiscard]] bool isCommittedFrameAt(const FrameHeader *hdr,
                                        std::uintptr_t frame_addr,
                                        std::uintptr_t segment_end) const {
    const uint32_t len = hdr->length.load(std::memory_order_acquire);
    if (len < sizeof(FrameHeader) || len > segment_size_)
      return false;
    const uint32_t stride = framePhysicalLength(len);
    return frame_addr + stride <= segment_end;
  }

  [[nodiscard]] const FrameHeader *
  lastCommittedFrame(const Segment &segment) const {
    const auto segment_end = segment.address() + segment_size_;
    std::uintptr_t addr = segment.firstFrameAddress();
    const FrameHeader *last = nullptr;
    while (addr + sizeof(FrameHeader) <= segment_end) {
      const auto *hdr = reinterpret_cast<const FrameHeader *>(addr);
      if (!isCommittedFrameAt(hdr, addr, segment_end))
        break;
      last = hdr;
      if (static_cast<MsgType>(hdr->msg_type) == MsgType::SegmentEnd)
        break;
      addr += framePhysicalLength(hdr->length.load(std::memory_order_acquire));
    }
    return last;
  }

  [[nodiscard]] FrameHeader *lastCommittedFrame(Segment &segment) const {
    return const_cast<FrameHeader *>(
        lastCommittedFrame(static_cast<const Segment &>(segment)));
  }

  [[nodiscard]] bool segmentHasCommittedFrame(const Segment &segment) const {
    return lastCommittedFrame(segment) != nullptr;
  }

  int findLastWrittenSegment() {
    const auto segment_ids =
        location_->locator->listSegmentId(location_, dest_id_);
    if (segment_ids.empty()) {
      return 0;
    }
    const auto highest_segment = segment_ids.back();
    SPDLOG_INFO("Highest existing segment: {}", highest_segment);
    for (auto it = segment_ids.rbegin(); it != segment_ids.rend(); ++it) {
      const int segment_id = *it;
      try {

        SegmentPtr segment =
            Segment::load(location_, dest_id_, segment_id, false, false);
        uintptr_t last_frame_pos = segment->header_->last_frame_position;
        SPDLOG_INFO("Segment {} last_frame_position: {}", segment_id,
                    last_frame_pos);
        if (segmentHasCommittedFrame(*segment)) {
          SPDLOG_INFO("Found last segment with data: {}", segment_id);
          return segment_id;
        }
      } catch (const std::exception &e) {
        SPDLOG_ERROR("Failed to check segment {}: {}", segment_id, e.what());
      }
    }
    SPDLOG_WARN("No segments with data found, starting from 0");
    return 0;
  }

  void loadInitialSegment() {
    int new_segment_id =
        current_segment_id_.fetch_add(1, std::memory_order_relaxed) + 1;
    current_segment_ =
        Segment::load(location_, dest_id_, new_segment_id, true, prefault_);
    current_frame_address_ = current_segment_->firstFrameAddress();
    SPDLOG_DEBUG("Loaded initial segment {} at sequence {}", new_segment_id,
                 current_sequence_.load());
  }

  void resumeFromSegment(int segment_id) {
    current_segment_id_.store(segment_id, std::memory_order_relaxed);
    try {
      SegmentPtr segment =
          Segment::load(location_, dest_id_, segment_id, true, prefault_);
      FrameHeader *last = lastCommittedFrame(*segment);
      if (!last) {
        SPDLOG_WARN("Resume segment {} has no committed frame", segment_id);
        return;
      }

      const uint32_t last_len = last->length.load(std::memory_order_acquire);
      const std::uintptr_t append_addr =
          reinterpret_cast<std::uintptr_t>(last) +
          framePhysicalLength(last_len);
      current_sequence_.store(last->sequence + 1, std::memory_order_relaxed);

      if (static_cast<MsgType>(last->msg_type) == MsgType::SegmentEnd) {
        SPDLOG_INFO(
            "Resuming stream after sealed segment {}, starting sequence = {}",
            segment_id, last->sequence + 1);
        return;
      }

      current_segment_ = std::move(segment);
      current_frame_address_ = append_addr;
      SPDLOG_INFO("Resuming stream in segment {} at sequence {}", segment_id,
                  last->sequence + 1);
    } catch (const std::exception &e) {
      SPDLOG_ERROR("Failed to resume from segment {}: {}", segment_id,
                   e.what());
    }
  }

  void rotateSegment() {
    int new_segment_id =
        current_segment_id_.fetch_add(1, std::memory_order_relaxed) + 1;
    SegmentPtr next_segment;
    if (appender_warmer_) {

      appender_warmer_->updateCurrentSegment(new_segment_id);
      size_t warmed_size = 0;
      std::uintptr_t warmed_addr =
          appender_warmer_->getWarmedSegment(new_segment_id, warmed_size);
      if (warmed_addr != 0) {
        next_segment = Segment::loadFromAddress(
            location_, dest_id_, new_segment_id, warmed_addr, prefault_);
        SPDLOG_TRACE("Using pre-warmed segment {}", new_segment_id);
      } else {
        next_segment =
            Segment::load(location_, dest_id_, new_segment_id, true, prefault_);
        SPDLOG_TRACE("Created segment {} (not pre-warmed)", new_segment_id);
      }
    } else {
      SPDLOG_INFO("No warmer - load segment normally");
      next_segment =
          Segment::load(location_, dest_id_, new_segment_id, true, prefault_);
    }

    if (current_segment_) {
      uintptr_t segment_base = current_segment_->address();
      uintptr_t border = segment_base + segment_size_ - FRAME_ALIGNMENT;
      SPDLOG_TRACE(
          "rotateSegment: current_frame_address_={:#x}, segment_base={:#x}, "
          "border={:#x}",
          current_frame_address_, segment_base, border);
      assert(current_frame_address_ <= border);
      try {
        auto *end_hdr = std::launder(
            reinterpret_cast<FrameHeader *>(current_frame_address_));
        new (end_hdr) FrameHeader{};
        end_hdr->gen_time = Timestamp::now().nsec();
        end_hdr->event_time = 0;
        end_hdr->msg_type = static_cast<uint16_t>(MsgType::SegmentEnd);
        end_hdr->flags = FRAME_NORMAL;
        end_hdr->sequence = nextSeq();
        end_hdr->length.store(static_cast<uint32_t>(sizeof(FrameHeader)),
                              std::memory_order_release);
        uintptr_t offset = current_frame_address_ - current_segment_->address();
        current_segment_->setLastFramePosition(offset);
      } catch (...) {
        SPDLOG_WARN("rotateSegment(): failed to append SegmentEnd sentinel");
      }

      retired_segments_.push_back(std::move(current_segment_));

      while (retired_segments_.size() > max_retained_segments_) {
        SegmentPtr old_segment = std::move(retired_segments_.front());
        retired_segments_.pop_front();
        if (background_threads_) {
          {
            std::lock_guard<std::mutex> lock(retire_mutex_);
            retire_queue_.push_back(std::move(old_segment));
          }

        }

      }
    }

    current_segment_ = std::move(next_segment);
    current_frame_address_ = current_segment_->firstFrameAddress();
    SPDLOG_DEBUG("Rotated to segment {} at sequence {}", new_segment_id,
                 current_sequence_.load());
  }

  FrameHeader *openFrame(int64_t timestamp, MsgType msg_type,
                         size_t payload_size, uint16_t flags = FRAME_NORMAL) {
    size_t total_size = sizeof(FrameHeader) + payload_size;
    size_t stride = framePhysicalLength(static_cast<uint32_t>(total_size));
    uintptr_t segment_addr = current_segment_->address();
    uintptr_t border = segment_addr + segment_size_ - FRAME_ALIGNMENT;
    if (current_frame_address_ + stride > border) [[unlikely]] {
      rotateSegment();
    }
    auto *raw = reinterpret_cast<FrameHeader *>(current_frame_address_);
    auto *frame_header = std::launder(new (raw) FrameHeader{});
    frame_header->gen_time = timestamp;
    frame_header->event_time = 0;
    frame_header->msg_type = static_cast<uint16_t>(msg_type);
    frame_header->flags = flags;
    frame_header->sequence = nextSeq();
    frame_header->length.store(0, std::memory_order_relaxed);
    return frame_header;
  }

  void closeFrame(FrameHeader *frame_header, size_t payload_size) {
    closeFrame(frame_header, payload_size, [](FrameHeader &) noexcept {});
  }

  template <typename BeforePublish>
  void closeFrame(FrameHeader *frame_header, size_t payload_size,
                  BeforePublish &&before_publish) {
    size_t total_size = sizeof(FrameHeader) + payload_size;
    std::forward<BeforePublish>(before_publish)(*frame_header);
    frame_header->length.store(static_cast<uint32_t>(total_size),
                               std::memory_order_release);
    auto frame_addr = reinterpret_cast<uintptr_t>(frame_header);
    uintptr_t offset = frame_addr - current_segment_->address();
    current_segment_->setLastFramePosition(offset);
    current_frame_address_ =
        frame_addr + framePhysicalLength(static_cast<uint32_t>(total_size));
  }

  static uint16_t calculateCRC16(const uint8_t *data, size_t len) {
    uint16_t crc = 0xFFFF;
    for (size_t i = 0; i < len; ++i) {
      crc ^= data[i];
      for (int j = 0; j < 8; ++j) {
        if (crc & 1) {
          crc = (crc >> 1) ^ 0xA001;
        } else {
          crc >>= 1;
        }
      }
    }
    return crc;
  }
};

using AppenderPtr = std::shared_ptr<Appender<>>;

} // namespace racer::revoq

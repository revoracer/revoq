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

#include "MmapBuffer.h"
#include "RealtimePriority.h"
#include "Segment.h"
#include "StreamBase.h"
#include "detail/StreamPlatform.h"
#include "portable_intrinsics.h"

namespace racer::revoq {

class CursorWarmer {
public:
  CursorWarmer(LocationPtr location, std::uint32_t dest_id,
               std::uint32_t segment_size, int warmer_timeout_ms)
      : location_(std::move(location)), dest_id_(dest_id),
        segment_size_(segment_size), warmer_timeout_ms_(warmer_timeout_ms) {}

  ~CursorWarmer() { stop(); }

  CursorWarmer(const CursorWarmer &) = delete;
  CursorWarmer &operator=(const CursorWarmer &) = delete;

  void start(int background_worker_cpu) {

    if (running_.exchange(true, std::memory_order_relaxed))
      return;
    {

      std::lock_guard<std::mutex> lock(mutex_);
      stopped_ = false;
      dedicated_ = false;
      config_ready_ = false;
    }
    thread_ = std::thread(&CursorWarmer::warmerLoop, this);
    bool pinned_to_requested = false;
#ifdef __linux__
    const bool pinned = detail::pinThreadToBackgroundCpus(
        thread_.native_handle(), background_worker_cpu, "cursor warmer thread",
        &pinned_to_requested);
    assert(pinned && "cursor warmer thread affinity pin failed");
    (void)pinned;
#else
    (void)background_worker_cpu;
#endif
    {

      std::lock_guard<std::mutex> lock(mutex_);
      dedicated_ = pinned_to_requested;
      config_ready_ = true;
    }
    cv_.notify_one();
  }

  void stop() {
    detail::stopAndJoin(running_, mutex_, cv_, thread_);
    std::lock_guard<std::mutex> lock(mutex_);
    stopped_ = true;
    warmer_pending_ = false;
    pending_flag_.store(false, std::memory_order_relaxed);
    warmer_next_segment_ = -1;
    releaseWarmedSegment();
  }

  [[nodiscard]] SegmentPtr tryLoadWarmedSegment(int segment_id) {
    SegmentPtr new_segment;
    std::lock_guard<std::mutex> lock(mutex_);
    if (warmer_ready_id_ == segment_id && warmer_addr_ != 0) {
      try {

        new_segment = Segment::loadFromAddress(location_, dest_id_, segment_id,
                                               warmer_addr_, false);
        warmer_addr_ = 0;
        warmer_ready_id_ = -1;
      } catch (...) {

        os::MmapBuffer::releaseMmapBuffer(warmer_addr_, segment_size_, false,
                                          os::MsyncMode::NONE);
        warmer_addr_ = 0;
        warmer_ready_id_ = -1;
      }
    }
    return new_segment;
  }

  void signalNext(int segment_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (stopped_)
      return;
    warmer_next_segment_ = segment_id;
    warmer_pending_ = true;

    pending_flag_.store(true, std::memory_order_release);

    if (!dedicated_)
      cv_.notify_one();
  }

private:
  const LocationPtr location_;
  const std::uint32_t dest_id_;
  const std::uint32_t segment_size_;
  const int warmer_timeout_ms_;

  std::atomic<bool> running_{false};
  std::thread thread_;

  std::atomic<bool> pending_flag_{false};

  std::mutex mutex_;
  std::condition_variable cv_;

  bool stopped_{false};

  bool dedicated_{false};

  bool config_ready_{false};
  int64_t warmer_next_segment_{-1};
  bool warmer_pending_{false};
  std::uintptr_t warmer_addr_{0};
  int64_t warmer_ready_id_{-1};

  void warmerLoop() {
    {

      std::unique_lock<std::mutex> lock(mutex_);
      cv_.wait(lock, [this] {
        return config_ready_ || !running_.load(std::memory_order_relaxed);
      });
      if (!running_.load(std::memory_order_relaxed))
        return;
    }

    if (dedicated_)
      system::RealtimePriority::setRealtimePriority(
          20, system::SchedulerPolicy::FIFO);

    while (true) {
      int64_t segment_to_warm;
      {
        std::unique_lock<std::mutex> lock(mutex_, std::defer_lock);
        if (dedicated_) {

          while (!pending_flag_.load(std::memory_order_acquire) &&
                 running_.load(std::memory_order_relaxed)) {
            PORTABLE_PAUSE();
          }
          lock.lock();
        } else {

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

          continue;
        warmer_pending_ = false;
        segment_to_warm = warmer_next_segment_;
      }

      if (segment_to_warm < 0)
        continue;

      auto path = Segment::getSegmentPath(location_, dest_id_,
                                          static_cast<int>(segment_to_warm));

      const std::string path_str = path.string();

      bool file_ready = false;
      for (int ms = 0; ms < warmer_timeout_ms_; ++ms) {
        {
          std::lock_guard<std::mutex> lk(mutex_);
          if (warmer_pending_ || !running_.load(std::memory_order_relaxed)) {
            goto next_signal;
          }
        }
        {
          struct stat st{};

          if (stat(path.c_str(), &st) == 0 &&
              st.st_size == static_cast<off_t>(segment_size_)) {
            file_ready = true;
            break;
          }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
      }

      if (!file_ready) {
        goto next_signal;
      }

      {

        std::lock_guard<std::mutex> lk(mutex_);
        if (warmer_pending_ || !running_.load(std::memory_order_relaxed)) {
          goto next_signal;
        }
      }

      {
#ifdef __linux__
        std::uintptr_t addr = 0;
        try {
          addr = os::MmapBuffer::loadExistingReadOnlyMmapBuffer(path_str,
                                                                segment_size_);
        } catch (const os::MmapError &) {
          goto next_signal;
        }

        {
          const auto *hdr = reinterpret_cast<const SegmentHeader *>(addr);
          bool header_ok = false;
          for (int ms = 0; ms < warmer_timeout_ms_; ++ms) {
            {
              std::lock_guard<std::mutex> lk(mutex_);
              if (warmer_pending_ ||
                  !running_.load(std::memory_order_relaxed)) {
                os::MmapBuffer::releaseMmapBuffer(addr, segment_size_, false,
                                                  os::MsyncMode::NONE);
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
          if (!header_ok) {
            os::MmapBuffer::releaseMmapBuffer(addr, segment_size_, false,
                                              os::MsyncMode::NONE);
            goto next_signal;
          }
        }

        try {
          os::MmapBuffer::prepareReadOnlyMmapBuffer(
              path_str, addr, segment_size_,
              {.populate_read = true, .advise_hugepage = true});
        } catch (const os::MmapError &) {
          os::MmapBuffer::releaseMmapBuffer(addr, segment_size_, false,
                                            os::MsyncMode::NONE);
          goto next_signal;
        }

        {
          std::lock_guard<std::mutex> lk(mutex_);
          if (warmer_pending_ || !running_.load(std::memory_order_relaxed)) {
            os::MmapBuffer::releaseMmapBuffer(addr, segment_size_, false,
                                              os::MsyncMode::NONE);
            goto next_signal;
          }
          if (warmer_addr_ != 0)
            os::MmapBuffer::releaseMmapBuffer(warmer_addr_, segment_size_,
                                              false, os::MsyncMode::NONE);
          warmer_addr_ = addr;
          warmer_ready_id_ = segment_to_warm;
        }
#endif
      }
    next_signal:;
    }
  }

  void releaseWarmedSegment() {
    if (warmer_addr_ != 0) {
      os::MmapBuffer::releaseMmapBuffer(warmer_addr_, segment_size_, false,
                                        os::MsyncMode::NONE);
      warmer_addr_ = 0;
      warmer_ready_id_ = -1;
    }
  }
};

using CursorWarmerPtr = std::unique_ptr<CursorWarmer>;

} // namespace racer::revoq

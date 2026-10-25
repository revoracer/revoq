// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 RevoRacer
#pragma once

#include <algorithm>
#include <cstddef>
#include <limits>
#include <memory>
#include <queue>
#include <unordered_set>
#include <utility>
#include <vector>

#include <spdlog/spdlog.h>

#include "Stream.h"

namespace revoq {

struct MergingCursorOptions {
  // When segment pages get established; see SegmentPopulate.
  SegmentPopulate populate = SegmentPopulate::Background;
  // mlock segments while mapped. Not with Lazy.
  bool lock_pages = false;
  // Background requires these (warmer and retire threads).
  bool background_threads = true;
  size_t retained_segment_limit = 0;
  int warmer_timeout_ms = 5000;
  // Warmer busy-polls, so it wants a dedicated (ideally nohz_full) core.
  // Retire timed-polls and tolerates latency, so it belongs on a housekeeping
  // core (-1 => the housekeeping fallback in
  // CpuAffinity::pinThreadOrHousekeeping).
  int warmer_cpu = -1;
  int retire_cpu = -1;
};

// Comparator for min-heap: stream with earliest timestamp has highest priority
struct StreamComparator {
  bool operator()(const std::pair<int64_t, size_t> &a,
                  const std::pair<int64_t, size_t> &b) const {
    // Min-heap: smaller timestamp = higher priority
    return a.first > b.first;
  }
};

class MergingCursor {
public:
  // The frame wrapper is mutable. This is not a snapshot and must not be kept
  // after the cursor advances or its joined streams change.
  struct FrameView {
    Frame *frame{nullptr};
    std::uint32_t frame_length{0};
    std::int32_t msg_type{0};

    [[nodiscard]] explicit operator bool() const noexcept {
      return frame != nullptr;
    }
    [[nodiscard]] Frame *operator->() const noexcept { return frame; }
    [[nodiscard]] Frame &operator*() const noexcept { return *frame; }
  };

  explicit MergingCursor(MergingCursorOptions options)
      : populate_(options.populate), lock_pages_(options.lock_pages),
        background_threads_(options.background_threads),
        retained_segment_limit_(options.retained_segment_limit),
        warmer_timeout_ms_(options.warmer_timeout_ms),
        warmer_cpu_(options.warmer_cpu), retire_cpu_(options.retire_cpu),
        current_stream_idx_(static_cast<size_t>(-1)), needs_rebuild_(true),
        use_heap_(true) {
    validateSegmentPopulate(populate_, lock_pages_, background_threads_);
  }

  ~MergingCursor() { streams_.clear(); }

  void join(const revoq::LocationPtr &location, std::uint32_t dest_id,
            std::int64_t from_time) {
    // Fast duplicate check using set
    uint64_t key = makeKey(location->uid, dest_id);
    if (joined_keys_.find(key) != joined_keys_.end()) {
      SPDLOG_WARN("cursor cannot join stream {}/[{:08x}] more than once",
                  location->uname, dest_id);
      return;
    }
    auto stream = std::make_shared<Stream>(
        location, dest_id,
        StreamOptions{
            .is_writing = false,
            .populate = populate_,
            .lock_pages = lock_pages_,
            .background_threads = background_threads_,
            .retained_segment_limit = retained_segment_limit_,
            .warmer_timeout_ms = warmer_timeout_ms_,
            .warmer_cpu = warmer_cpu_,
            .retire_cpu = retire_cpu_,
        });
    stream->seekToTime(from_time);
    streams_.push_back(stream);
    joined_keys_.insert(key);
    refreshSingleStream();
    needs_rebuild_ = true;
  }

  void disjoin(std::uint32_t location_uid) {
    // Remove from streams vector
    auto old_size = streams_.size();
    streams_.erase(std::remove_if(streams_.begin(), streams_.end(),
                                  [location_uid](const StreamPtr &j) {
                                    return j->location_->uid == location_uid;
                                  }),
                   streams_.end());
    if (streams_.size() != old_size) {
      // Rebuild keys set
      joined_keys_.clear();
      for (const auto &j : streams_) {
        joined_keys_.insert(makeKey(j->location_->uid, j->dest_id_));
      }
      current_stream_idx_ = static_cast<size_t>(-1);
      refreshSingleStream();
      needs_rebuild_ = true;
    }
  }

  int getCurrentStreamSegmentId() const {
    if (current_stream_idx_ != static_cast<size_t>(-1) &&
        current_stream_idx_ < streams_.size()) {
      return streams_[current_stream_idx_]->current_segment_->segment_id_;
    }
    return -1;
  }

  std::uintptr_t getCurrentStreamFrameOffset() const {
    if (current_stream_idx_ != static_cast<size_t>(-1) &&
        current_stream_idx_ < streams_.size()) {
      const auto &stream = streams_[current_stream_idx_];
      if (stream->current_segment_ && stream->frame_)
        return stream->frame_->address() - stream->current_segment_->address();
    }
    return 0;
  }

  std::uint32_t getCurrentStreamSegmentSize() const {
    if (current_stream_idx_ != static_cast<size_t>(-1) &&
        current_stream_idx_ < streams_.size()) {
      const auto &segment = streams_[current_stream_idx_]->current_segment_;
      if (segment)
        return segment->getSegmentSize();
    }
    return 0;
  }

  [[nodiscard]] FrameView tryCurrentFrame() {
    if (single_frame_)
      return tryCurrentSingleFrame();
    if (!isDataAvailable())
      return {};
    Frame *frame = currentFrameRaw();
    const std::uint32_t frame_length = frame->getFrameLength();
    const std::int32_t msg_type = frame->getMsgType();
    return frame->hasData(frame_length, msg_type)
               ? FrameView{frame, frame_length, msg_type}
               : FrameView{};
  }

  bool isDataAvailable() {
    // Fast path: single stream, already positioned
    if (single_stream_)
      return single_stream_->isDataAvailable();
    // Rebuild heap/find minimum if needed
    if (needs_rebuild_) {
      rebuildHeap();
    }
    Frame *frame = currentFrameRaw();
    // CRITICAL FIX: If we have no current frame but streams exist,
    // we need to rebuild on next call to check if data became available
    if (!frame && !streams_.empty()) {
      needs_rebuild_ = true;
    }
    return frame && frame->hasData();
  }

  void seekToTime(std::int64_t timestamp) {
    for (const auto &stream : streams_) {
      stream->seekToTime(timestamp);
    }
    needs_rebuild_ = true;
  }

  void next() {
    if (current_stream_idx_ == static_cast<size_t>(-1) ||
        current_stream_idx_ >= streams_.size()) {
      return;
    }
    // Advance current stream
    if (single_stream_) {
      single_stream_->next();
      return;
    }
    streams_[current_stream_idx_]->next();
    if (use_heap_ && streams_.size() > 3) {
      // Heap-based approach for many streams
      updateHeap();
    } else {
      // Linear scan for few streams (simpler, less overhead)
      needs_rebuild_ = true;
    }
  }

  void advance(FrameView current) {
    if (single_stream_) {
      single_stream_->next(current.frame_length,
                           static_cast<MsgType>(current.msg_type));
      return;
    }
    next();
  }

  // Get number of active streams
  size_t getStreamCount() const { return streams_.size(); }

  // Check if a specific stream is joined
  bool isJoined(std::uint32_t location_uid, std::uint32_t dest_id) const {
    uint64_t key = makeKey(location_uid, dest_id);
    return joined_keys_.find(key) != joined_keys_.end();
  }

  // Enable/disable heap optimization (useful for testing)
  void setUseHeap(bool use_heap) {
    use_heap_ = use_heap;
    needs_rebuild_ = true;
  }

#ifdef REVOQ_ENABLE_DIAGNOSTICS
  [[nodiscard]] StreamRuntimeStats getStats() const {
    StreamRuntimeStats total;
    for (const auto &stream : streams_) {
      const auto s = stream->getStats();
      total.load_segments += s.load_segments;
      total.slow_loads += s.slow_loads;
      total.warmer_hits += s.warmer_hits;
      total.warmer_misses += s.warmer_misses;
      total.warmer_invalid += s.warmer_invalid;
      total.warmer_signals += s.warmer_signals;
      total.warmer_ready += s.warmer_ready;
      total.warmer_file_timeouts += s.warmer_file_timeouts;
      total.warmer_header_timeouts += s.warmer_header_timeouts;
      total.warmer_superseded += s.warmer_superseded;
      total.warmer_map_errors += s.warmer_map_errors;
      total.warmer_populate_errors += s.warmer_populate_errors;
      total.warmer_signal_overwrites += s.warmer_signal_overwrites;
      total.warmer_miss_pending += s.warmer_miss_pending;
      total.warmer_miss_working += s.warmer_miss_working;
      total.warmer_miss_wrong_ready += s.warmer_miss_wrong_ready;
      total.warmer_miss_wrong_working += s.warmer_miss_wrong_working;
      total.warmer_miss_no_request += s.warmer_miss_no_request;
      total.warmer_ready_after_need += s.warmer_ready_after_need;
      total.warmer_cpu_changes += s.warmer_cpu_changes;
      total.retire_enqueues += s.retire_enqueues;
      total.retire_segments += s.retire_segments;
      total.load_max_ns = std::max(total.load_max_ns, s.load_max_ns);
      total.slow_load_max_ns =
          std::max(total.slow_load_max_ns, s.slow_load_max_ns);
      total.warmer_wake_max_ns =
          std::max(total.warmer_wake_max_ns, s.warmer_wake_max_ns);
      total.warmer_file_wait_max_ns =
          std::max(total.warmer_file_wait_max_ns, s.warmer_file_wait_max_ns);
      total.warmer_map_max_ns =
          std::max(total.warmer_map_max_ns, s.warmer_map_max_ns);
      total.warmer_header_wait_max_ns = std::max(
          total.warmer_header_wait_max_ns, s.warmer_header_wait_max_ns);
      total.warmer_populate_max_ns =
          std::max(total.warmer_populate_max_ns, s.warmer_populate_max_ns);
      total.warmer_signal_to_ready_max_ns = std::max(
          total.warmer_signal_to_ready_max_ns, s.warmer_signal_to_ready_max_ns);
      if (s.warmer_ready_ahead_min_ns != 0 &&
          (total.warmer_ready_ahead_min_ns == 0 ||
           s.warmer_ready_ahead_min_ns < total.warmer_ready_ahead_min_ns)) {
        total.warmer_ready_ahead_min_ns = s.warmer_ready_ahead_min_ns;
      }
      total.warmer_need_to_ready_max_ns = std::max(
          total.warmer_need_to_ready_max_ns, s.warmer_need_to_ready_max_ns);
      total.retire_max_ns = std::max(total.retire_max_ns, s.retire_max_ns);
      if (s.warmer_last_cpu >= 0)
        total.warmer_last_cpu = s.warmer_last_cpu;
    }
    return total;
  }

  void resetStats() {
    for (const auto &stream : streams_)
      stream->resetStats();
  }
#endif

  // The dispatcher is a concrete caller-owned strategy, typically a direct
  // switch over current.msg_type. It is intentionally separate from runtime
  // handler registration so the compiler can inline the complete hot path.
  // It receives every committed frame, including internal SegmentEnd sentinels.
  template <typename Dispatcher> bool nextStatic(Dispatcher &dispatcher) {
    const FrameView current =
        single_frame_ ? tryCurrentSingleFrame() : tryCurrentFrame();
    if (!current)
      return false;
    dispatcher(current);
    advance(current);
    return true;
  }

private:
  [[nodiscard]] Frame *currentFrameRaw() const noexcept {
    if (current_stream_idx_ != static_cast<size_t>(-1) &&
        current_stream_idx_ < streams_.size()) {
      return streams_[current_stream_idx_]->getCurrentFrameRaw();
    }
    return nullptr;
  }

  [[nodiscard]] FrameView tryCurrentSingleFrame() const noexcept {
    if (!single_stream_->isDataAvailable())
      return {};
    Frame *frame = single_frame_;
    const std::uint32_t frame_length = frame->getFrameLength();
    const std::int32_t msg_type = frame->getMsgType();
    return frame->hasData(frame_length, msg_type)
               ? FrameView{frame, frame_length, msg_type}
               : FrameView{};
  }

  void refreshSingleStream() noexcept {
    single_stream_ = streams_.size() == 1 ? streams_.front().get() : nullptr;
    single_frame_ =
        single_stream_ ? single_stream_->getCurrentFrameRaw() : nullptr;
    if (single_stream_)
      current_stream_idx_ = 0;
  }

  void rebuildHeap() {
    if (streams_.empty()) {
      current_stream_idx_ = static_cast<size_t>(-1);
      needs_rebuild_ = false;
      return;
    }
    if (use_heap_ && streams_.size() > 3) {
      // Build min-heap of (timestamp, stream_index) pairs
      std::vector<std::pair<int64_t, size_t>> temp;
      temp.reserve(streams_.size());
      for (size_t i = 0; i < streams_.size(); ++i) {
        if (streams_[i]->isDataAvailable()) {
          const auto *frame = streams_[i]->getCurrentFrameRaw();
          // FIXED: Use acquire memory order for proper synchronization
          int64_t gen_time = frame->getGenTime();
          temp.emplace_back(gen_time, i);
        }
      }
      if (temp.empty()) {
        current_stream_idx_ = static_cast<size_t>(-1);
      } else {
        // Build heap and extract minimum
        std::make_heap(temp.begin(), temp.end(), StreamComparator{});
        current_stream_idx_ = temp.front().second;
        // Store heap for incremental updates
        heap_ = std::priority_queue<std::pair<int64_t, size_t>,
                                    std::vector<std::pair<int64_t, size_t>>,
                                    StreamComparator>(StreamComparator{},
                                                      std::move(temp));
      }
    } else {
      // Linear scan for few streams
      findMinimumLinear();
    }
    needs_rebuild_ = false;
  }

  void findMinimumLinear() {
    int64_t min_time = std::numeric_limits<int64_t>::max();
    size_t min_index = static_cast<size_t>(-1);
    for (size_t i = 0; i < streams_.size(); ++i) {
      if (streams_[i]->isDataAvailable()) {
        const auto *frame = streams_[i]->getCurrentFrameRaw();
        // FIXED: Proper memory ordering for synchronization with appender
        int64_t gen_time = frame->getGenTime();
        if (gen_time < min_time) {
          min_time = gen_time;
          min_index = i;
        }
      }
    }
    current_stream_idx_ = min_index;
  }

  void updateHeap() {
    if (heap_.empty()) {
      needs_rebuild_ = true;
      return;
    }
    // Remove old entry for current stream because we just consumed the
    // current stream frame
    heap_.pop();
    // Check if current stream has more data
    if (streams_[current_stream_idx_]->isDataAvailable()) {
      const auto *frame = streams_[current_stream_idx_]->getCurrentFrameRaw();
      int64_t gen_time = frame->getGenTime();
      heap_.emplace(gen_time, current_stream_idx_);
    }
    // Get new minimum
    if (heap_.empty()) {
      current_stream_idx_ = static_cast<size_t>(-1);
      needs_rebuild_ = true;
    } else {
      current_stream_idx_ = heap_.top().second;
    }
  }

  // Create unique key for location+dest pair
  static uint64_t makeKey(std::uint32_t location_uid, std::uint32_t dest_id) {
    return (static_cast<uint64_t>(location_uid) << 32) | dest_id;
  }

  const SegmentPopulate populate_;
  const bool lock_pages_;
  const bool background_threads_;
  const size_t retained_segment_limit_;
  const int warmer_timeout_ms_;
  const int warmer_cpu_;
  const int retire_cpu_;
  size_t current_stream_idx_; // Index into streams_ vector
  bool needs_rebuild_;        // Flag to rebuild heap/rescan
  bool use_heap_;             // Use heap optimization for many streams

  std::vector<StreamPtr> streams_;
  Stream *single_stream_{nullptr};
  Frame *single_frame_{nullptr};
  std::unordered_set<uint64_t> joined_keys_; // Fast duplicate detection

  // Min-heap for efficient multi-stream merging
  std::priority_queue<std::pair<int64_t, size_t>, // (timestamp, stream_index)
                      std::vector<std::pair<int64_t, size_t>>, StreamComparator>
      heap_;
};

using MergingCursorPtr = std::shared_ptr<MergingCursor>;

} // namespace revoq

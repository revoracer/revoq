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

namespace racer::revoq {

struct MergingCursorOptions {
  bool prefault;
  bool background_threads = true;
  size_t retained_segment_limit = 0;
  int warmer_timeout_ms = 5000;

  int warmer_cpu = -1;
  int retire_cpu = -1;
};

struct StreamComparator {
  bool operator()(const std::pair<int64_t, size_t> &a,
                  const std::pair<int64_t, size_t> &b) const {

    return a.first > b.first;
  }
};

class MergingCursor {
public:
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
      : prefault_(options.prefault),
        background_threads_(options.background_threads),
        retained_segment_limit_(options.retained_segment_limit),
        warmer_timeout_ms_(options.warmer_timeout_ms),
        warmer_cpu_(options.warmer_cpu), retire_cpu_(options.retire_cpu),
        current_stream_idx_(static_cast<size_t>(-1)), needs_rebuild_(true),
        use_heap_(true) {}

  ~MergingCursor() { streams_.clear(); }

  void join(const revoq::LocationPtr &location, std::uint32_t dest_id,
            std::int64_t from_time) {

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
            .prefault = prefault_,
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

    auto old_size = streams_.size();
    streams_.erase(std::remove_if(streams_.begin(), streams_.end(),
                                  [location_uid](const StreamPtr &j) {
                                    return j->location_->uid == location_uid;
                                  }),
                   streams_.end());
    if (streams_.size() != old_size) {

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

    if (single_stream_)
      return single_stream_->isDataAvailable();

    if (needs_rebuild_) {
      rebuildHeap();
    }
    Frame *frame = currentFrameRaw();

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

    if (single_stream_) {
      single_stream_->next();
      return;
    }
    streams_[current_stream_idx_]->next();
    if (use_heap_ && streams_.size() > 3) {

      updateHeap();
    } else {

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

  size_t getStreamCount() const { return streams_.size(); }

  bool isJoined(std::uint32_t location_uid, std::uint32_t dest_id) const {
    uint64_t key = makeKey(location_uid, dest_id);
    return joined_keys_.find(key) != joined_keys_.end();
  }

  void setUseHeap(bool use_heap) {
    use_heap_ = use_heap;
    needs_rebuild_ = true;
  }

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

      std::vector<std::pair<int64_t, size_t>> temp;
      temp.reserve(streams_.size());
      for (size_t i = 0; i < streams_.size(); ++i) {
        if (streams_[i]->isDataAvailable()) {
          const auto *frame = streams_[i]->getCurrentFrameRaw();

          int64_t gen_time = frame->getGenTime();
          temp.emplace_back(gen_time, i);
        }
      }
      if (temp.empty()) {
        current_stream_idx_ = static_cast<size_t>(-1);
      } else {

        std::make_heap(temp.begin(), temp.end(), StreamComparator{});
        current_stream_idx_ = temp.front().second;

        heap_ = std::priority_queue<std::pair<int64_t, size_t>,
                                    std::vector<std::pair<int64_t, size_t>>,
                                    StreamComparator>(StreamComparator{},
                                                      std::move(temp));
      }
    } else {

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

    heap_.pop();

    if (streams_[current_stream_idx_]->isDataAvailable()) {
      const auto *frame = streams_[current_stream_idx_]->getCurrentFrameRaw();
      int64_t gen_time = frame->getGenTime();
      heap_.emplace(gen_time, current_stream_idx_);
    }

    if (heap_.empty()) {
      current_stream_idx_ = static_cast<size_t>(-1);
      needs_rebuild_ = true;
    } else {
      current_stream_idx_ = heap_.top().second;
    }
  }

  static uint64_t makeKey(std::uint32_t location_uid, std::uint32_t dest_id) {
    return (static_cast<uint64_t>(location_uid) << 32) | dest_id;
  }

  const bool prefault_;
  const bool background_threads_;
  const size_t retained_segment_limit_;
  const int warmer_timeout_ms_;
  const int warmer_cpu_;
  const int retire_cpu_;
  size_t current_stream_idx_;
  bool needs_rebuild_;
  bool use_heap_;

  std::vector<StreamPtr> streams_;
  Stream *single_stream_{nullptr};
  Frame *single_frame_{nullptr};
  std::unordered_set<uint64_t> joined_keys_;

  std::priority_queue<std::pair<int64_t, size_t>,
                      std::vector<std::pair<int64_t, size_t>>, StreamComparator>
      heap_;
};

using MergingCursorPtr = std::shared_ptr<MergingCursor>;

} // namespace racer::revoq

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 RevoRacer
#pragma once

#include <cstdint>
#include <filesystem>
#include <memory>

#include "Frame.h"
#include "RevoqError.h"
#include "StreamBase.h"

#include <revobase/MmapBuffer.h>

namespace revoq {

// Segment size constants: should be multiple of 2MB
constexpr std::uint32_t SEGMENT_SIZE_MARKETDATA = 128 * 1024 * 1024; // 128 MB
constexpr std::uint32_t SEGMENT_SIZE_EXECUTION = 4 * 1024 * 1024;    // 4 MB
constexpr std::uint32_t SEGMENT_SIZE_DEFAULT = 2 * 1024 * 1024;      // 2 MB
// Optimized segment size lookup with inline hint
inline static std::uint32_t findSegmentSize(const LocationPtr &location,
                                            std::uint32_t dest_id) {
  if (location->category == Category::MARKETDATA ||
      location->category == Category::RESEARCHDATA) {
    return SEGMENT_SIZE_MARKETDATA;
  }
  // Execution with specific dest_id uses medium segments
  if (location->category == Category::EXECUTION && dest_id != 0) {
    return SEGMENT_SIZE_EXECUTION;
  }
  return SEGMENT_SIZE_DEFAULT;
}
#pragma pack(push, 1)
struct alignas(64) SegmentHeader {
  std::uint32_t version{};
  std::uint32_t segment_header_length{};
  std::uint32_t segment_size{};
  std::uint32_t frame_header_length{};
  std::uint64_t last_frame_position{};
  char padding[40]{};
};
#pragma pack(pop)
static_assert(sizeof(SegmentHeader) == 64,
              "SegmentHeader must be exactly one cache line");
static_assert(alignof(SegmentHeader) == 64,
              "SegmentHeader must be cache-line aligned");

class Segment;
using SegmentPtr = std::shared_ptr<Segment>;

class Segment {
public:
  ~Segment() {
    if (owned_by_segment_ && header_) {
      // MARKETDATA/RESEARCHDATA: no msync — rely on kernel writeback.
      // All others: async msync on the retire thread normally
      const auto msync_mode = (location_->category == Category::MARKETDATA ||
                               location_->category == Category::RESEARCHDATA)
                                  ? revobase::os::MsyncMode::NONE
                                  : revobase::os::MsyncMode::ASYNC;
      if (revobase::os::MmapBuffer::releaseMmapBuffer(address(), size_, prefault_,
                                                   msync_mode)) {
        SPDLOG_TRACE("released segment {}/{:08x}.{}.stream", location_->uname,
                     dest_id_, segment_id_);
      } else {
        SPDLOG_ERROR("cannot release segment {}/{:08x}.{}.stream",
                     location_->uname, dest_id_, segment_id_);
      }
    }
  }

  [[nodiscard]] std::uint32_t getSegmentSize() const {
    return header_->segment_size;
  }
  [[nodiscard]] revoq::LocationPtr getLocation() const { return location_; }
  [[nodiscard]] std::uint32_t getDestId() const { return dest_id_; }
  [[nodiscard]] int getSegmentId() const { return segment_id_; }

  [[nodiscard]] std::int64_t beginTime() const {
    auto *frame = reinterpret_cast<revoq::FrameHeader *>(firstFrameAddress());
    // Use acquire to ensure we see the frame properly
    if (frame->length.load(std::memory_order_acquire) == 0) {
      return 0;
    }
    return frame->gen_time;
  }

  [[nodiscard]] std::int64_t endTime() const {
    auto *frame = reinterpret_cast<revoq::FrameHeader *>(lastFrameAddress());
    // Use acquire to ensure we see the frame properly
    if (frame->length.load(std::memory_order_acquire) == 0) {
      return 0;
    }
    return frame->gen_time;
  }

  [[nodiscard]] std::uintptr_t address() const {
    return reinterpret_cast<std::uintptr_t>(header_);
  }

  [[nodiscard]] std::uintptr_t addressBorder() const {
    // the last FRAME_ALIGNMENT bytes of every segment are kept free for the
    // SegmentEnd sentinel
    return address() + header_->segment_size - revoq::FRAME_ALIGNMENT;
  }

  [[nodiscard]] std::uintptr_t firstFrameAddress() const {
    return address() + header_->segment_header_length;
  }

  [[nodiscard]] std::uintptr_t lastFrameAddress() const {
    return address() + header_->last_frame_position;
  }

  [[nodiscard]] bool isFull() const {
    auto *last_frame =
        reinterpret_cast<revoq::FrameHeader *>(lastFrameAddress());
    // Use acquire to synchronize with the appender's release
    uint32_t frame_len = last_frame->length.load(std::memory_order_acquire);
    return lastFrameAddress() + revoq::framePhysicalLength(frame_len) >
           addressBorder();
  }

  static SegmentPtr load(const revoq::LocationPtr &location,
                         std::uint32_t dest_id, int segment_id, bool is_writing,
                         bool prefault) {
    std::uint32_t segment_size = findSegmentSize(location, dest_id);
    // Only appenders own directory creation. The stream dir already exists by
    // the time a cursor loads a segment (created at join via listSegmentId),
    // so the compute-only path cannot strand a fresh stream — it just drops a
    // per-slow-load stat/mkdir.
    auto path = is_writing ? makeSegmentPath(location, dest_id, segment_id)
                           : segmentPath(location, dest_id, segment_id);
    std::uintptr_t address = 0;
    try {
      address = revobase::os::MmapBuffer::loadMmapBuffer(path.string(), segment_size,
                                               is_writing, prefault);
    } catch (const std::exception &e) {
      throw RevoqError(fmt::format("Failed to mmap segment {}: {}",
                                   path.string(), e.what()));
    }

    if (address == 0) {
      throw RevoqError(fmt::format(
          "MmapBuffer returned null address for segment {}", path.string()));
    }

    auto header = reinterpret_cast<SegmentHeader *>(address);
    bool is_new_segment = (header->last_frame_position == 0);
    if (is_new_segment) {
      if (!is_writing) {
        revobase::os::MmapBuffer::releaseMmapBuffer(address, segment_size, prefault,
                                          revobase::os::MsyncMode::NONE);
        throw RevoqError(
            fmt::format("Uninitialized segment {}", path.string()));
      }
      // Use memset for zeroing (compiler may optimize to efficient instruction)
      std::memset(header, 0, sizeof(SegmentHeader));

      // Initialize header fields
      header->version = SEGMENT_VERSION;
      header->segment_header_length = sizeof(SegmentHeader);
      header->segment_size = segment_size;
      header->frame_header_length = sizeof(FrameHeader);
      header->last_frame_position =
          sizeof(SegmentHeader); // Points to first frame position

      // Ensure header is visible before segment is used (write barrier)
      std::atomic_thread_fence(std::memory_order_release);

      SPDLOG_DEBUG("Initialized new segment {}/{:08x}.{}.stream (size: {} MB)",
                   location->uname, dest_id, segment_id,
                   segment_size / (1024 * 1024));
    }

    // Validation - check version
    if (header->version != SEGMENT_VERSION) {
      revobase::os::MmapBuffer::releaseMmapBuffer(address, segment_size, prefault,
                                        revobase::os::MsyncMode::NONE);
      throw RevoqError(
          fmt::format("Version mismatch for segment {}, required {}, found {}",
                      path.string(), SEGMENT_VERSION, header->version));
    }

    // Validation - check header length
    if (header->segment_header_length != sizeof(SegmentHeader)) {
      revobase::os::MmapBuffer::releaseMmapBuffer(address, segment_size, prefault,
                                        revobase::os::MsyncMode::NONE);
      throw RevoqError(fmt::format(
          "Header length mismatch for segment {}, required {}, found {}",
          path.string(), sizeof(SegmentHeader), header->segment_header_length));
    }

    // Validation - check segment size
    if (header->segment_size != segment_size) {
      revobase::os::MmapBuffer::releaseMmapBuffer(address, segment_size, prefault,
                                        revobase::os::MsyncMode::NONE);
      throw RevoqError(fmt::format(
          "Segment size mismatch for segment {}, required {}, found {}",
          path.string(), segment_size, header->segment_size));
    }

    // Validation - check last_frame_position is within bounds
    if (header->last_frame_position >= segment_size) {
      SPDLOG_WARN(
          "Invalid last_frame_position {} for segment {} (size {}), resetting",
          header->last_frame_position, path.string(), segment_size);
      revobase::os::MmapBuffer::releaseMmapBuffer(address, segment_size, prefault,
                                        revobase::os::MsyncMode::NONE);
      throw RevoqError(fmt::format(
          "Invalid last_frame_position {} for segment {} (size {}), resetting",
          header->last_frame_position, path.string(), segment_size));
      // header->last_frame_position = sizeof(SegmentHeader);
    }

    SPDLOG_TRACE("Loaded segment {}/{:08x}.{}.stream ({})", location->uname,
                 dest_id, segment_id, is_new_segment ? "new" : "existing");

    // Create and return Segment object (owned_by_segment=true)
    return std::shared_ptr<Segment>(new Segment(
        location, dest_id, segment_id, segment_size, prefault, address, true));
  }

  // Load from pre-warmed address (avoids MmapBuffer call)
  static SegmentPtr loadFromAddress(const revoq::LocationPtr &location,
                                    std::uint32_t dest_id, int segment_id,
                                    std::uintptr_t address, bool prefault) {
    if (address == 0) {
      throw RevoqError("Cannot load segment from null address");
    }
    auto header = reinterpret_cast<SegmentHeader *>(address);
    std::uint32_t segment_size = header->segment_size;
    // Validation (header should already be initialized by AppenderWarmer)
    if (header->version != SEGMENT_VERSION) {
      throw RevoqError(fmt::format(
          "version mismatch for pre-warmed segment {} (expected {}, got {})",
          segment_id, SEGMENT_VERSION, header->version));
    }
    if (header->segment_header_length != sizeof(SegmentHeader)) {
      throw RevoqError(fmt::format("header length mismatch for pre-warmed "
                                   "segment {} (expected {}, got {})",
                                   segment_id, sizeof(SegmentHeader),
                                   header->segment_header_length));
    }
    SPDLOG_TRACE("Loaded pre-warmed segment {} from address 0x{:x}", segment_id,
                 address);
    // owned_by_segment=true means this Segment object owns the memory
    return std::shared_ptr<Segment>(new Segment(
        location, dest_id, segment_id, segment_size, prefault, address, true));
  }

  // Compute only: no filesystem access.
  [[nodiscard]] static std::filesystem::path
  segmentPath(const LocationPtr &location, std::uint32_t dest_id,
              int segment_id) {
    return location->locator->layoutDir(location, Layout::STREAM) /
           segmentFileName(dest_id, segment_id);
  }

  // Writer-side: creates the stream directory (not the segment file), then
  // returns the path. Readers use segmentPath().
  static std::filesystem::path makeSegmentPath(const LocationPtr &location,
                                               std::uint32_t dest_id,
                                               int segment_id) {
    const auto dir = location->locator->makeLayoutDir(location, Layout::STREAM);
    return dir / segmentFileName(dest_id, segment_id);
  }

  [[nodiscard]] static std::string segmentFileName(std::uint32_t dest_id,
                                                   int segment_id) {
    return fmt::format("{:08x}.{}.stream", dest_id, segment_id);
  }

  static int findSegmentId(const revoq::LocationPtr &location,
                           std::uint32_t dest_id, std::int64_t time) {
    std::vector<int> segment_ids =
        location->locator->listSegmentId(location, dest_id);
    if (segment_ids.empty()) {
      return 1;
    }
    if (time == 0) {
      return segment_ids.front();
    }

    // Binary search for the right segment (assumes segments are time-ordered)
    int left = 0;
    int right = static_cast<int>(segment_ids.size()) - 1;
    int best_segment = segment_ids.front();

    while (left <= right) {
      int mid = left + (right - left) / 2;
      auto segment = load(location, dest_id, segment_ids[mid], false, false);
      auto segment_begin_time = segment->beginTime();
      // Skip empty segments (beginTime() returns 0 for empty segments)
      if (segment_begin_time == 0) {
        // Empty segment - search earlier segments
        right = mid - 1;
        continue;
      }
      if (segment_begin_time <= time) {
        best_segment = segment_ids[mid];
        left = mid + 1; // Look for later segments
      } else {
        right = mid - 1; // Look for earlier segments
      }
    }

    return best_segment;
  }

private:
  const LocationPtr location_;
  const std::uint32_t dest_id_;
  const int segment_id_;
  const bool prefault_;
  const std::size_t size_;
  const SegmentHeader *header_;
  const bool owned_by_segment_; // Track if we own the memory

  Segment(const LocationPtr &location, std::uint32_t dest_id, int segment_id,
          std::size_t size, bool prefault, std::uintptr_t address,
          bool owned = true)
      : location_(location), dest_id_(dest_id), segment_id_(segment_id),
        prefault_(prefault), size_(size),
        header_(reinterpret_cast<SegmentHeader *>(address)),
        owned_by_segment_(owned) {}

  // it is actually an offset from the beginning add
  void setLastFramePosition(std::uint64_t position) {
    const_cast<SegmentHeader *>(header_)->last_frame_position = position;
  }

  friend class Stream;
  template <typename> friend class Appender;
  friend class MergingCursor;
};
} // namespace revoq

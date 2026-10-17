// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 RevoRacer
#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>

#include <spdlog/spdlog.h>

namespace revoq {

inline constexpr std::uint32_t SEGMENT_VERSION = 2;
inline constexpr std::size_t FRAME_ALIGNMENT = 64;

enum FrameFlags : uint16_t {
  FRAME_NORMAL = 0x0000,
  FRAME_COMPRESSED = 0x0001,
  FRAME_HIGH_PRIORITY = 0x0002,
  FRAME_RETRY = 0x0004,
  FRAME_CORRUPTED = 0x0008
};

#pragma pack(push, 1)
struct alignas(32) FrameHeader {
  // Logical frame length in bytes. This is published last with release
  // semantics; physical frame stride is align_up(length, FRAME_ALIGNMENT).
  std::atomic<std::uint32_t> length{0};
  std::uint16_t msg_type{0};
  std::uint16_t flags{FRAME_NORMAL};
  std::uint64_t sequence{0};
  std::int64_t gen_time{0};
  std::int64_t event_time{0};
};
#pragma pack(pop)
static_assert(sizeof(FrameHeader) == 32, "FrameHeader must be 32 bytes");
static_assert(alignof(FrameHeader) == 32,
              "FrameHeader must be 32-byte aligned");

constexpr std::size_t alignUp(std::size_t value,
                              std::size_t alignment) noexcept {
  return (value + alignment - 1) / alignment * alignment;
}

constexpr std::uint32_t framePhysicalLength(std::uint32_t logical_length) {
  return static_cast<std::uint32_t>(alignUp(logical_length, FRAME_ALIGNMENT));
}

class Frame {
public:
  // Lock-free but not wait-free committed-frame check. Loads the published
  // frame length and validates that the current frame is complete
  [[nodiscard]] bool hasData() const { return hasData(getFrameLength()); }

  [[nodiscard]] bool hasData(std::uint32_t frame_length) const {
    return hasData(frame_length, getMsgType());
  }

  [[nodiscard]] bool hasData(std::uint32_t frame_length,
                             std::int32_t msg_type) const {
    return frame_length > 0 && msg_type > 0;
  }

  [[nodiscard]] std::uintptr_t address() const {
    return reinterpret_cast<std::uintptr_t>(header_);
  }

  [[nodiscard]] std::uint32_t getFrameLength() const {
    return header_->length.load(std::memory_order_acquire);
  }

  [[nodiscard]] std::uint32_t getHeaderLength() const {
    return sizeof(FrameHeader);
  }

  [[nodiscard]] std::uint32_t getDataLength() const {
    return getDataLength(getFrameLength());
  }

  [[nodiscard]] std::uint32_t getDataLength(std::uint32_t frame_length) const {
    if (frame_length < sizeof(FrameHeader)) [[unlikely]] {
      return 0;
    }
    return frame_length - sizeof(FrameHeader);
  }

  [[nodiscard]] std::int64_t getGenTime() const { return header_->gen_time; }

  [[nodiscard]] std::int64_t getEventTime() const {
    return header_->event_time;
  }

  [[nodiscard]] std::int32_t getMsgType() const { return header_->msg_type; }

  [[nodiscard]] std::uint64_t getSequence() const { return header_->sequence; }

  [[nodiscard]] uint16_t getFlags() const { return header_->flags; }

  [[nodiscard]] bool isCompressed() const {
    return header_->flags & FRAME_COMPRESSED;
  }

  [[nodiscard]] bool isHighPriority() const {
    return header_->flags & FRAME_HIGH_PRIORITY;
  }

  [[nodiscard]] const char *dataAsBytes() const {
    return reinterpret_cast<char *>(address() + getHeaderLength());
  }

  [[nodiscard]] std::string dataAsString() const {
    return {dataAsBytes(), getDataLength()};
  }

  [[nodiscard]] std::string toString() const {
    return {reinterpret_cast<char *>(address())};
  }

  template <typename T> std::size_t copyData(const T &data) {
    std::size_t length = sizeof(T);
    memcpy(const_cast<void *>(dataAddress()), &data, length);
    return length;
  }

  template <typename T> inline const T &data() const {
    return *(reinterpret_cast<const T *>(dataAddress()));
  }

private:
  [[nodiscard]] const void *dataAddress() const {
    return reinterpret_cast<void *>(address() + getHeaderLength());
  }

  FrameHeader *header_{nullptr};
  Frame() = default;

  void setAddress(std::uintptr_t address) {
    header_ = reinterpret_cast<FrameHeader *>(address);
  }

  void moveToNext() { moveToNext(getFrameLength()); }

  void moveToNext(std::uint32_t frame_length) {
    setAddress(address() + framePhysicalLength(frame_length));
  }

  void setDataLength(std::uint32_t length) {
    // Write data length LAST with release semantics for visibility
    header_->length.store(sizeof(FrameHeader) + length,
                          std::memory_order_release);
  }

  void setGenTime(std::int64_t gen_time) { header_->gen_time = gen_time; }

  void setEventTime(std::int64_t event_time) {
    header_->event_time = event_time;
  }

  void setMsgType(std::int32_t msg_type) {
    header_->msg_type = static_cast<std::uint16_t>(msg_type);
  }

  void setSequence(std::uint64_t seq) { header_->sequence = seq; }

  void setFlags(uint16_t flags) { header_->flags = flags; }

  friend class Stream;
};

using FramePtr = std::shared_ptr<Frame>;

} // namespace revoq

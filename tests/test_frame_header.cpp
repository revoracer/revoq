// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 RevoRacer
#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <thread>

#include <revoq/Frame.h>
#include <revoq/StreamBase.h>

// Opaque payload tag — revoq stores msg_type without interpreting it, so a
// journal test uses an arbitrary value rather than a market-data enum.
static constexpr revoq::MsgType kTagA =
    static_cast<revoq::MsgType>(100);

using namespace revoq;

namespace {

constexpr std::size_t kV2FrameHeaderBytes = 32;

template <typename T>
concept HasHeaderLength = requires(T h) { h.header_length; };

template <typename T>
concept HasSource = requires(T h) { h.source; };

template <typename T>
concept HasDest = requires(T h) { h.dest; };

template <typename T>
concept HasTriggerTime = requires(T h) { h.trigger_time; };

template <typename T>
concept HasEventTime = requires(T h) { h.event_time; };

template <typename T>
concept HasChecksum = requires(T h) { h.checksum; };

template <typename T>
concept HasAtomicSequence = requires(T h) {
  {
    h.sequence.load(std::memory_order_relaxed)
  } -> std::convertible_to<std::uint64_t>;
};

template <typename T> constexpr bool hasUint64Sequence() {
  return std::same_as<decltype(std::declval<T &>().sequence), std::uint64_t>;
}

template <typename T> constexpr std::size_t eventTimeOffsetOrMax() {
  if constexpr (HasEventTime<T>) {
    return offsetof(T, event_time);
  }
  return std::numeric_limits<std::size_t>::max();
}

template <typename T> void setSequence(T &header, std::uint64_t sequence) {
  if constexpr (HasAtomicSequence<T>) {
    header.sequence.store(sequence, std::memory_order_relaxed);
  } else {
    header.sequence = sequence;
  }
}

template <typename T> std::uint64_t getSequence(T &header) {
  if constexpr (HasAtomicSequence<T>) {
    return header.sequence.load(std::memory_order_relaxed);
  } else {
    return header.sequence;
  }
}

template <typename T> void setEventTime(T &header, std::int64_t event_time) {
  if constexpr (HasEventTime<T>) {
    header.event_time = event_time;
  }
}

template <typename T> std::int64_t getEventTimeOrSentinel(T &header) {
  if constexpr (HasEventTime<T>) {
    return header.event_time;
  }
  return std::numeric_limits<std::int64_t>::min();
}

} // namespace

TEST_CASE("FrameHeader v2 has the compact production layout",
          "[frame][header][v2]") {
  SECTION("Header is 32 bytes and aligned for the v2 frame format") {
    REQUIRE(sizeof(FrameHeader) == kV2FrameHeaderBytes);
    REQUIRE(alignof(FrameHeader) == 32);
  }

  SECTION("Field order is stable") {
    REQUIRE(offsetof(FrameHeader, length) == 0);
    REQUIRE(offsetof(FrameHeader, msg_type) == 4);
    REQUIRE(offsetof(FrameHeader, flags) == 6);
    REQUIRE(offsetof(FrameHeader, sequence) == 8);
    REQUIRE(offsetof(FrameHeader, gen_time) == 16);
    REQUIRE(eventTimeOffsetOrMax<FrameHeader>() == 24);
  }

  SECTION("Field types are sized for production") {
    REQUIRE((std::same_as<decltype(std::declval<FrameHeader &>().length),
                          std::atomic<std::uint32_t>>));
    REQUIRE((std::same_as<decltype(std::declval<FrameHeader &>().msg_type),
                          std::uint16_t>));
    REQUIRE((std::same_as<decltype(std::declval<FrameHeader &>().flags),
                          std::uint16_t>));
    REQUIRE(hasUint64Sequence<FrameHeader>());
    REQUIRE((std::same_as<decltype(std::declval<FrameHeader &>().gen_time),
                          std::int64_t>));
    REQUIRE(HasEventTime<FrameHeader>);
  }
}

TEST_CASE("FrameHeader v2 removes v1-only metadata", "[frame][header][v2]") {
  REQUIRE_FALSE(HasHeaderLength<FrameHeader>);
  REQUIRE_FALSE(HasSource<FrameHeader>);
  REQUIRE_FALSE(HasDest<FrameHeader>);
  REQUIRE_FALSE(HasTriggerTime<FrameHeader>);
  REQUIRE_FALSE(HasChecksum<FrameHeader>);
}

TEST_CASE("FrameHeader v2 initializes to an unpublished empty frame",
          "[frame][header][v2]") {
  FrameHeader header{};

  REQUIRE(header.length.load(std::memory_order_relaxed) == 0);
  REQUIRE(header.msg_type == 0);
  REQUIRE(header.flags == FRAME_NORMAL);
  REQUIRE(getSequence(header) == 0);
  REQUIRE(header.gen_time == 0);
  REQUIRE(getEventTimeOrSentinel(header) == 0);
}

TEST_CASE("FrameHeader event_time is optional production metadata",
          "[frame][header][v2]") {
  FrameHeader header{};

  SECTION("Unspecified event_time stays zero") {
    header.gen_time = 1'000'000;

    REQUIRE(getEventTimeOrSentinel(header) == 0);
  }

  SECTION("event_time can differ from gen_time when the publisher has one") {
    header.gen_time = 1'000'200;
    setEventTime(header, 1'000'000);

    REQUIRE(header.gen_time == 1'000'200);
    REQUIRE(getEventTimeOrSentinel(header) == 1'000'000);
  }
}

TEST_CASE("FrameHeader length publish orders all metadata",
          "[frame][header][ordering]") {
  FrameHeader header{};
  std::atomic<bool> cursor_saw_frame{false};

  std::thread cursor([&]() {
    while (header.length.load(std::memory_order_acquire) == 0) {
      std::this_thread::yield();
    }

    cursor_saw_frame.store(true, std::memory_order_relaxed);
    CHECK(header.msg_type == static_cast<std::uint16_t>(kTagA));
    CHECK(header.flags == FRAME_HIGH_PRIORITY);
    CHECK(getSequence(header) == 42);
    CHECK(header.gen_time == 1'000'200);
    CHECK(getEventTimeOrSentinel(header) == 1'000'000);
  });

  header.msg_type = static_cast<std::uint16_t>(kTagA);
  header.flags = FRAME_HIGH_PRIORITY;
  setSequence(header, 42);
  header.gen_time = 1'000'200;
  setEventTime(header, 1'000'000);
  header.length.store(kV2FrameHeaderBytes, std::memory_order_release);

  cursor.join();

  REQUIRE(cursor_saw_frame.load(std::memory_order_relaxed));
}

TEST_CASE("FrameHeader sequence is a 64-bit production counter",
          "[frame][header][sequence]") {
  FrameHeader header{};
  constexpr std::uint64_t past_32_bit_wrap =
      static_cast<std::uint64_t>(std::numeric_limits<std::uint32_t>::max()) + 7;

  setSequence(header, past_32_bit_wrap);

  REQUIRE(getSequence(header) == past_32_bit_wrap);
}

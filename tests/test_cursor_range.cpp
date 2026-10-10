// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 RevoRacer
#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <vector>

#include <revoq/Appender.h>
#include <revoq/MergingCursor.h>
#include "test_helpers.h"

using namespace revoq;
using namespace revoq::test;

namespace {

struct RangeTrade {
  int64_t timestamp;
  uint32_t id;
  double price;
};

} // namespace

TEST_CASE("MergingCursor FrameView loop reads one stream",
          "[cursor][frame-view]") {
  TempTestDir temp_dir;
  auto location = createTestLocation(temp_dir.string());
  constexpr uint32_t dest_id = 0;

  {
    auto appender = std::make_shared<Appender<>>(
        location, dest_id, AppenderOptions{.prefault = true});
    for (uint32_t i = 0; i < 5; ++i) {
      RangeTrade trade{1'000'000 + static_cast<int64_t>(i * 1000), i,
                       100.0 + i};
      publish(*appender, trade.timestamp, kTagA, trade);
    }
  }

  MergingCursor cursor(MergingCursorOptions{.prefault = true});
  cursor.join(location, dest_id, 0);

  std::vector<uint32_t> ids;
  std::vector<uint64_t> sequences;
  while (auto frame = cursor.tryCurrentFrame()) {
    REQUIRE(frame->getMsgType() == static_cast<int32_t>(kTagA));
    const auto &trade = frame->data<RangeTrade>();
    ids.push_back(trade.id);
    sequences.push_back(frame->getSequence());
    cursor.advance(frame);
  }

  CHECK(ids == std::vector<uint32_t>{0, 1, 2, 3, 4});
  CHECK(sequences == std::vector<uint64_t>{0, 1, 2, 3, 4});
  CHECK_FALSE(cursor.isDataAvailable());
}

TEST_CASE("MergingCursor FrameView loop preserves merged stream order",
          "[cursor][frame-view]") {
  TempTestDir temp_dir;
  auto location = createTestLocation(temp_dir.string());

  {
    auto appender = std::make_shared<Appender<>>(
        location, 0, AppenderOptions{.prefault = true});
    RangeTrade first{1'000'000, 10, 100.0};
    RangeTrade third{3'000'000, 30, 300.0};
    publish(*appender, first.timestamp, kTagA, first);
    publish(*appender, third.timestamp, kTagA, third);
  }

  {
    auto appender = std::make_shared<Appender<>>(
        location, 1, AppenderOptions{.prefault = true});
    RangeTrade second{2'000'000, 20, 200.0};
    publish(*appender, second.timestamp, kTagA, second);
  }

  MergingCursor cursor(MergingCursorOptions{.prefault = true});
  cursor.join(location, 0, 0);
  cursor.join(location, 1, 0);

  std::vector<uint32_t> ids;
  while (auto frame = cursor.tryCurrentFrame()) {
    ids.push_back(frame->data<RangeTrade>().id);
    cursor.advance(frame);
  }

  CHECK(ids == std::vector<uint32_t>{10, 20, 30});
}

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 RevoRacer
#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <filesystem>
#include <stdexcept>
#include <thread>
#include <vector>

#include <revoq/Appender.h>
#include <revoq/MergingCursor.h>
#include <revoq/TypedDispatcher.h>
#include "test_helpers.h"

using namespace racer::revoq;
using namespace racer::revoq::test;

namespace {

struct TypedTrade {
  static constexpr MsgType kMsgType = MsgType::Trade;

  int64_t timestamp;
  uint32_t id;
  double price;
};

struct TypedBBO {
  static constexpr MsgType kMsgType = MsgType::BBO;

  int64_t timestamp;
  uint32_t bid_size;
  uint32_t ask_size;
};

struct TypedBookReset {
  static constexpr MsgType kMsgType = MsgType::BookReset;
};

struct TinyTrade {
  int64_t timestamp;
  uint32_t id;
};

} // namespace

TEST_CASE("MergingCursor typed dispatch invokes registered handlers",
          "[cursor][typed]") {
  TempTestDir temp_dir;
  auto location = createTestLocation(temp_dir.string());
  constexpr uint32_t dest_id = 0;

  {
    auto appender = std::make_shared<Appender<>>(
        location, dest_id, AppenderOptions{.prefault = true});
    TypedTrade trade1{1'000'000, 10, 101.25};
    TypedBBO bbo{2'000'000, 100, 200};
    TypedTrade trade2{3'000'000, 11, 102.50};

    publish(*appender, trade1.timestamp, TypedTrade::kMsgType, trade1);
    publish(*appender, bbo.timestamp, TypedBBO::kMsgType, bbo);
    appender->mark(2'500'000, TypedBookReset::kMsgType);
    publish(*appender, trade2.timestamp, TypedTrade::kMsgType, trade2);
  }

  MergingCursor cursor(MergingCursorOptions{.prefault = true});
  cursor.join(location, dest_id, 0);

  std::vector<uint32_t> trade_ids;
  std::vector<uint64_t> trade_sequences;
  uint32_t total_bbo_size = 0;
  int reset_count = 0;

  TypedDispatcher dispatcher(cursor);
  dispatcher
      .on<TypedTrade>([&](const TypedTrade &trade, const Frame &frame) {
        trade_ids.push_back(trade.id);
        trade_sequences.push_back(frame.getSequence());
      })
      .on<TypedBBO>([&](const TypedBBO &bbo) {
        total_bbo_size = bbo.bid_size + bbo.ask_size;
      })
      .on<TypedBookReset>([&](const TypedBookReset &, const Frame &frame) {
        ++reset_count;
        CHECK(frame.getDataLength() == 0);
      });

  CHECK(dispatcher.run() == 4);
  CHECK(trade_ids == std::vector<uint32_t>{10, 11});
  CHECK(trade_sequences == std::vector<uint64_t>{0, 3});
  CHECK(total_bbo_size == 300);
  CHECK(reset_count == 1);
}

TEST_CASE("MergingCursor typed dispatch skips unknown message types",
          "[cursor][typed]") {
  TempTestDir temp_dir;
  auto location = createTestLocation(temp_dir.string());
  constexpr uint32_t dest_id = 1;

  {
    auto appender = std::make_shared<Appender<>>(
        location, dest_id, AppenderOptions{.prefault = true});
    TypedTrade trade{1'000'000, 42, 99.0};
    TypedBBO bbo{2'000'000, 5, 7};

    publish(*appender, trade.timestamp, TypedTrade::kMsgType, trade);
    publish(*appender, bbo.timestamp, TypedBBO::kMsgType, bbo);
  }

  MergingCursor cursor(MergingCursorOptions{.prefault = true});
  cursor.join(location, dest_id, 0);

  int trade_count = 0;
  std::vector<int32_t> unknown_types;

  TypedDispatcher dispatcher(cursor);
  dispatcher
      .on<TypedTrade>([&](const TypedTrade &trade) {
        ++trade_count;
        CHECK(trade.id == 42);
      })
      .onUnknown([&](const Frame &frame) {
        unknown_types.push_back(frame.getMsgType());
      });

  CHECK(dispatcher.run() == 2);
  CHECK(trade_count == 1);
  REQUIRE(unknown_types.size() == 1);
  CHECK(unknown_types.front() == static_cast<int32_t>(TypedBBO::kMsgType));
}

TEST_CASE("MergingCursor typed dispatch rejects mismatched payload sizes",
          "[cursor][typed]") {
  TempTestDir temp_dir;
  auto location = createTestLocation(temp_dir.string());
  constexpr uint32_t dest_id = 2;

  {
    auto appender = std::make_shared<Appender<>>(
        location, dest_id, AppenderOptions{.prefault = true});
    TinyTrade tiny{1'000'000, 7};
    publish(*appender, tiny.timestamp, MsgType::Trade, tiny);
  }

  MergingCursor cursor(MergingCursorOptions{.prefault = true});
  cursor.join(location, dest_id, 0);
  TypedDispatcher dispatcher(cursor);
  dispatcher.on<TypedTrade>([](const TypedTrade &) {});

  CHECK_THROWS_AS(dispatcher.next(), std::runtime_error);
}

TEST_CASE("TypedDispatcher dispatches and advances one stream",
          "[cursor][typed]") {
  TempTestDir temp_dir;
  auto location = createTestLocation(temp_dir.string());
  constexpr uint32_t dest_id = 3;

  {
    auto appender = std::make_shared<Appender<>>(
        location, dest_id, AppenderOptions{.prefault = true});
    TypedTrade trade{1'000'000, 17, 103.25};
    TypedBBO bbo{2'000'000, 11, 13};
    publish(*appender, trade.timestamp, TypedTrade::kMsgType, trade);
    publish(*appender, bbo.timestamp, TypedBBO::kMsgType, bbo);
  }

  MergingCursor cursor(MergingCursorOptions{.prefault = true});
  cursor.join(location, dest_id, 0);

  int trade_count = 0;
  int unknown_count = 0;
  TypedDispatcher dispatcher(cursor);
  dispatcher
      .on<TypedTrade>([&](const TypedTrade &trade) {
        ++trade_count;
        CHECK(trade.id == 17);
      })
      .onUnknown([&](const Frame &frame) {
        ++unknown_count;
        CHECK(frame.getMsgType() == static_cast<int32_t>(TypedBBO::kMsgType));
      });

  const auto first = cursor.tryCurrentFrame();
  REQUIRE(first);
  CHECK(first->getMsgType() == static_cast<int32_t>(TypedTrade::kMsgType));
  CHECK(first.frame_length == sizeof(FrameHeader) + sizeof(TypedTrade));
  CHECK(first.msg_type == static_cast<int32_t>(TypedTrade::kMsgType));

  CHECK(dispatcher.next());
  CHECK(dispatcher.next());
  CHECK_FALSE(dispatcher.next());
  CHECK_FALSE(cursor.tryCurrentFrame());
  CHECK(trade_count == 1);
  CHECK(unknown_count == 1);
}

TEST_CASE("MergingCursor static fast path invokes concrete dispatcher",
          "[cursor][typed][static]") {
  TempTestDir temp_dir;
  auto location = createTestLocation(temp_dir.string());
  constexpr uint32_t dest_id = 4;

  {
    auto appender = std::make_shared<Appender<>>(
        location, dest_id, AppenderOptions{.prefault = true});
    TypedTrade trade{1'000'000, 23, 104.75};
    TypedBBO bbo{2'000'000, 19, 29};
    publish(*appender, trade.timestamp, TypedTrade::kMsgType, trade);
    publish(*appender, bbo.timestamp, TypedBBO::kMsgType, bbo);
  }

  MergingCursor cursor(MergingCursorOptions{.prefault = true});
  cursor.join(location, dest_id, 0);

  int trade_count = 0;
  int bbo_count = 0;
  auto dispatcher = [&](MergingCursor::FrameView current) {
    switch (static_cast<MsgType>(current.msg_type)) {
    case MsgType::Trade:
      ++trade_count;
      CHECK(current->data<TypedTrade>().id == 23);
      break;
    case MsgType::BBO:
      ++bbo_count;
      CHECK(current->data<TypedBBO>().ask_size == 29);
      break;
    default:
      FAIL("unexpected static-dispatch message type");
    }
  };

  CHECK(cursor.nextStatic(dispatcher));
  CHECK(cursor.nextStatic(dispatcher));
  CHECK_FALSE(cursor.nextStatic(dispatcher));
  CHECK(trade_count == 1);
  CHECK(bbo_count == 1);
}

TEST_CASE("MergingCursor static fast path clears its single-stream cache",
          "[cursor][typed][static]") {
  TempTestDir temp_dir;
  auto location = createTestLocation(temp_dir.string());
  constexpr uint32_t first_dest_id = 6;
  constexpr uint32_t second_dest_id = 7;

  {
    auto appender = std::make_shared<Appender<>>(
        location, first_dest_id, AppenderOptions{.prefault = true});
    TypedTrade trade{2'000'000, 61, 106.25};
    publish(*appender, trade.timestamp, TypedTrade::kMsgType, trade);
  }
  {
    auto appender = std::make_shared<Appender<>>(
        location, second_dest_id, AppenderOptions{.prefault = true});
    TypedTrade trade{1'000'000, 71, 107.25};
    publish(*appender, trade.timestamp, TypedTrade::kMsgType, trade);
  }

  MergingCursor cursor(MergingCursorOptions{.prefault = true});
  cursor.join(location, first_dest_id, 0);
  cursor.join(location, second_dest_id, 0);

  uint32_t trade_id = 0;
  auto dispatcher = [&](MergingCursor::FrameView current) {
    REQUIRE(current.msg_type == static_cast<int32_t>(TypedTrade::kMsgType));
    trade_id = current->data<TypedTrade>().id;
  };

  CHECK(cursor.nextStatic(dispatcher));
  CHECK(trade_id == 71);

  cursor.disjoin(location->uid);
  CHECK_FALSE(cursor.nextStatic(dispatcher));
}

TEST_CASE("Appender closeData finalizes metadata before publication",
          "[appender][publish][metadata]") {
  TempTestDir temp_dir;
  auto location = createTestLocation(temp_dir.string());
  constexpr uint32_t dest_id = 5;
  constexpr int64_t event_time = 9'876'543;

  {
    auto appender = std::make_shared<Appender<>>(
        location, dest_id, AppenderOptions{.prefault = true});
    auto &slot =
        appender->openData<TypedTrade>(1'234'567, TypedTrade::kMsgType);
    slot = TypedTrade{1'234'567, 31, 105.50};
    appender->closeData<TypedTrade>(
        [](FrameHeader &header) { header.event_time = event_time; });
  }

  MergingCursor cursor(MergingCursorOptions{.prefault = true});
  cursor.join(location, dest_id, 0);
  const auto current = cursor.tryCurrentFrame();
  REQUIRE(current);
  CHECK(current->getGenTime() == 1'234'567);
  CHECK(current->getEventTime() == event_time);
  CHECK(current->data<TypedTrade>().id == 31);
}

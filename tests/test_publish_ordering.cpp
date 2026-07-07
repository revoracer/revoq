// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 RevoRacer

#include <atomic>
#include <catch2/catch_test_macros.hpp>
#include <filesystem>
#include <thread>

#include <revoq/Appender.h>
#include <revoq/MergingCursor.h>
#include "test_helpers.h"

using namespace racer::revoq;
using namespace racer::revoq::test;
namespace fs = std::filesystem;

namespace {

// Payload with an embedded invariant: check must equal ~value.
// If the cursor ever sees a torn frame (payload partially visible before
// the release-store on length), check != ~value and we catch it.
struct OrderMsg {
  uint64_t value;
  uint64_t check;
};

class OrderingFixture {
public:
  OrderingFixture() {
    test_dir = fs::temp_directory_path() / "journal_ordering_test";
    if (fs::exists(test_dir))
      fs::remove_all(test_dir);
    fs::create_directories(test_dir);
    locator = std::make_shared<Locator>(test_dir.string());
    location = Location::make(Mode::LIVE, Category::EXECUTION, "test",
                              "ordering", locator);
  }

  ~OrderingFixture() {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    try {
      fs::remove_all(test_dir);
    } catch (...) {
    }
  }

  fs::path test_dir;
  ILocatorPtr locator;
  LocationPtr location;
};

} // namespace

TEST_CASE_METHOD(OrderingFixture, "Frame publish ordering", "[ordering]") {
  SECTION("cursor never observes a torn frame when length > 0") {
    // 10,000 frames with DEFAULT 2MB segments (32,766 frames/segment) → no
    // rotation, keeping the test focused on the acquire/release path in the hot
    // loop.
    constexpr uint64_t N = 10'000;

    Appender<> appender(
        location, 0,
        AppenderOptions{.prefault = false, .background_threads = false});

    std::atomic<int> torn_frames{0};
    std::atomic<bool> cursor_ready{false};

    std::thread cursor_thread([&]() {
      // No background threads: cursor is single-threaded, isolated to this
      // thread. background_threads=false avoids any warmer/retire concurrency
      // that could obscure a real ordering bug.
      MergingCursor cursor(
          MergingCursorOptions{.prefault = false, .background_threads = false});
      cursor.join(location, 0, 0);

      // Signal appender: cursor is positioned at frame 0 of segment 1.
      cursor_ready.store(true, std::memory_order_release);

      uint64_t count = 0;
      while (count < N) {
        if (!cursor.isDataAvailable())
          continue;

        auto frame = cursor.tryCurrentFrame();

        // SegmentEnd frames appear during rotation; none expected here but
        // guard defensively in case segment sizes differ across machines.
        if (static_cast<MsgType>(frame->getMsgType()) == MsgType::SegmentEnd) {
          cursor.next();
          continue;
        }

        // When length > 0 (the acquire-load in hasData/isDataAvailable), all
        // stores that preceded the release-store must be visible.  The payload
        // invariant check == ~value catches any ordering violation.
        if (frame->getDataLength() >= sizeof(OrderMsg)) {
          const auto &msg = frame->data<OrderMsg>();
          if (msg.check != ~msg.value)
            torn_frames.fetch_add(1, std::memory_order_relaxed);
        }

        ++count;
        cursor.next();
      }
    });

    // Block until cursor is positioned before writing, ensuring the test
    // exercises concurrent access rather than a sequential post-hoc read.
    while (!cursor_ready.load(std::memory_order_acquire)) {
    }

    for (uint64_t i = 0; i < N; ++i) {
      OrderMsg msg{i, ~i};
      publish(appender, static_cast<int64_t>(i + 1), MsgType::Trade, msg);
    }

    cursor_thread.join();
    REQUIRE(torn_frames.load() == 0);
  }
}

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 RevoRacer
#include <catch2/catch_test_macros.hpp>
#include <chrono>
#include <filesystem>
#include <thread>

#include <revoq/Appender.h>
#include <revoq/MergingCursor.h>
#include <revobase/TimeUtils.h>
#include "test_helpers.h"

using namespace racer::revoq;
using namespace racer::revoq::test;
namespace fs = std::filesystem;

// Test data
struct TestMessage {
  int64_t timestamp;
  uint32_t id;
  double value;
};

// Test fixture
class MergingCursorFixture {
public:
  MergingCursorFixture() {
    test_dir = fs::temp_directory_path() / "journal_cursor_test";

    if (fs::exists(test_dir)) {
      fs::remove_all(test_dir);
    }

    fs::create_directories(test_dir);

    locator = std::make_shared<Locator>(test_dir.string());
    location = Location::make(Mode::LIVE, Category::EXECUTION, "test", "cursor",
                              locator);
  }

  ~MergingCursorFixture() {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    if (fs::exists(test_dir)) {
      try {
        fs::remove_all(test_dir);
      } catch (...) {
      }
    }
  }

  // Helper: Write test data
  void writeTestData(uint32_t dest_id, int num_messages,
                     int64_t start_time = 0) {
    auto appender = std::make_shared<Appender<>>(
        location, dest_id, AppenderOptions{.prefault = true});

    if (start_time == 0) {
      start_time = revobase::Timestamp::now().nsec();
    }

    for (int i = 0; i < num_messages; ++i) {
      TestMessage msg{start_time + i * 1000, static_cast<uint32_t>(i),
                      100.0 + i};
      publish(*appender, msg.timestamp, MsgType::Trade, msg);
    }
  }

  fs::path test_dir;
  ILocatorPtr locator;
  LocationPtr location;
};

// ============================================================================
// Test 1: cursor Construction
// ============================================================================

TEST_CASE_METHOD(MergingCursorFixture, "MergingCursor construction",
                 "[cursor][construct]") {
  SECTION("Can create cursor with prefault=true") {
    auto cursor =
        std::make_shared<MergingCursor>(MergingCursorOptions{.prefault = true});
    REQUIRE(cursor != nullptr);
  }

  SECTION("Can create cursor with prefault=false") {
    auto cursor = std::make_shared<MergingCursor>(
        MergingCursorOptions{.prefault = false});
    REQUIRE(cursor != nullptr);
  }

  SECTION("New cursor has no streams") {
    auto cursor =
        std::make_shared<MergingCursor>(MergingCursorOptions{.prefault = true});
    REQUIRE(cursor->getStreamCount() == 0);
    REQUIRE_FALSE(cursor->isDataAvailable());
  }
}

// ============================================================================
// Test 2: Join/Disjoin
// ============================================================================

TEST_CASE_METHOD(MergingCursorFixture, "MergingCursor join/disjoin",
                 "[cursor][join]") {
  writeTestData(0, 10);
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  auto cursor =
      std::make_shared<MergingCursor>(MergingCursorOptions{.prefault = true});

  SECTION("Can join stream") {
    cursor->join(location, 0, 0);
    REQUIRE(cursor->getStreamCount() == 1);
    REQUIRE(cursor->isJoined(location->uid, 0));
  }

  SECTION("Cannot join same stream twice") {
    cursor->join(location, 0, 0);
    cursor->join(location, 0, 0); // Should warn but not crash
    REQUIRE(cursor->getStreamCount() == 1);
  }

  SECTION("Can join multiple streams") {
    writeTestData(1, 10);
    writeTestData(2, 10);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    cursor->join(location, 0, 0);
    cursor->join(location, 1, 0);
    cursor->join(location, 2, 0);

    REQUIRE(cursor->getStreamCount() == 3);
  }

  SECTION("Can disjoin stream") {
    cursor->join(location, 0, 0);
    REQUIRE(cursor->getStreamCount() == 1);

    cursor->disjoin(location->uid);
    REQUIRE(cursor->getStreamCount() == 0);
    REQUIRE_FALSE(cursor->isJoined(location->uid, 0));
  }

  SECTION("Disjoin non-existent stream is safe") {
    cursor->join(location, 0, 0);
    cursor->disjoin(999);                   // Non-existent uid
    REQUIRE(cursor->getStreamCount() == 1); // Should still have stream
  }
}

// ============================================================================
// Test 3: Basic Reading
// ============================================================================

TEST_CASE_METHOD(MergingCursorFixture, "MergingCursor basic reading",
                 "[cursor][read]") {
  const int NUM_MESSAGES = 100;
  writeTestData(0, NUM_MESSAGES);
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  auto cursor =
      std::make_shared<MergingCursor>(MergingCursorOptions{.prefault = true});
  cursor->join(location, 0, 0);

  SECTION("Can read all messages") {
    int count = 0;
    while (cursor->isDataAvailable()) {
      auto frame = cursor->tryCurrentFrame();
      REQUIRE(frame);
      REQUIRE(frame->hasData());

      const auto &msg = frame->data<TestMessage>();
      REQUIRE(msg.id == static_cast<uint32_t>(count));

      count++;
      cursor->next();
    }

    REQUIRE(count == NUM_MESSAGES);
  }

  SECTION("tryCurrentFrame returns empty view when no data") {
    // Read all data
    while (cursor->isDataAvailable()) {
      cursor->next();
    }

    auto frame = cursor->tryCurrentFrame();
    if (frame) {
      REQUIRE_FALSE(frame->hasData());
    }
  }

  SECTION("Can read messages in sequence") {
    std::vector<uint32_t> ids;

    while (cursor->isDataAvailable()) {
      auto frame = cursor->tryCurrentFrame();
      const auto &msg = frame->data<TestMessage>();
      ids.push_back(msg.id);
      cursor->next();
    }

    // Verify sequential
    for (size_t i = 1; i < ids.size(); ++i) {
      REQUIRE(ids[i] == ids[i - 1] + 1);
    }
  }
}

// ============================================================================
// Test 4: Multiple Stream Reading (Merge)
// ============================================================================

TEST_CASE_METHOD(MergingCursorFixture, "MergingCursor merges streams",
                 "[cursor][merge]") {
  // Write to 3 different streams with interleaved timestamps
  int64_t base_time = revobase::Timestamp::now().nsec();

  // Stream 0: times 0, 3, 6, 9...
  {
    auto appender = std::make_shared<Appender<>>(
        location, 0, AppenderOptions{.prefault = true});
    for (int i = 0; i < 10; ++i) {
      TestMessage msg{base_time + i * 3000, static_cast<uint32_t>(i), 0.0};
      publish(*appender, msg.timestamp, MsgType::Trade, msg);
    }
  }

  // Stream 1: times 1, 4, 7, 10...
  {
    auto appender = std::make_shared<Appender<>>(
        location, 1, AppenderOptions{.prefault = true});
    for (int i = 0; i < 10; ++i) {
      TestMessage msg{base_time + 1000 + i * 3000, static_cast<uint32_t>(i),
                      1.0};
      publish(*appender, msg.timestamp, MsgType::Trade, msg);
    }
  }

  // Stream 2: times 2, 5, 8, 11...
  {
    auto appender = std::make_shared<Appender<>>(
        location, 2, AppenderOptions{.prefault = true});
    for (int i = 0; i < 10; ++i) {
      TestMessage msg{base_time + 2000 + i * 3000, static_cast<uint32_t>(i),
                      2.0};
      publish(*appender, msg.timestamp, MsgType::Trade, msg);
    }
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  SECTION("Merges streams in timestamp order") {
    auto cursor =
        std::make_shared<MergingCursor>(MergingCursorOptions{.prefault = true});
    cursor->join(location, 0, 0);
    cursor->join(location, 1, 0);
    cursor->join(location, 2, 0);

    std::vector<int64_t> timestamps;

    while (cursor->isDataAvailable()) {
      auto frame = cursor->tryCurrentFrame();
      timestamps.push_back(frame->getGenTime());
      cursor->next();
    }

    // Verify sorted by timestamp
    REQUIRE(timestamps.size() == 30); // 10 from each stream

    for (size_t i = 1; i < timestamps.size(); ++i) {
      REQUIRE(timestamps[i] >= timestamps[i - 1]);
    }
  }

  SECTION("Can identify source stream") {
    auto cursor =
        std::make_shared<MergingCursor>(MergingCursorOptions{.prefault = true});
    cursor->join(location, 0, 0);
    cursor->join(location, 1, 0);
    cursor->join(location, 2, 0);

    int count_j0 = 0, count_j1 = 0, count_j2 = 0;

    while (cursor->isDataAvailable()) {
      auto frame = cursor->tryCurrentFrame();
      const auto &msg = frame->data<TestMessage>();

      if (msg.value == 0.0)
        count_j0++;
      else if (msg.value == 1.0)
        count_j1++;
      else if (msg.value == 2.0)
        count_j2++;

      cursor->next();
    }

    REQUIRE(count_j0 == 10);
    REQUIRE(count_j1 == 10);
    REQUIRE(count_j2 == 10);
  }
}

// ============================================================================
// Test 5: Seek Operations
// ============================================================================

TEST_CASE_METHOD(MergingCursorFixture, "MergingCursor seek", "[cursor][seek]") {
  int64_t base_time = revobase::Timestamp::now().nsec();

  // Write 100 messages with known timestamps
  {
    auto appender = std::make_shared<Appender<>>(
        location, 0, AppenderOptions{.prefault = true});
    for (int i = 0; i < 100; ++i) {
      TestMessage msg{base_time + i * 1000, static_cast<uint32_t>(i),
                      100.0 + i};
      publish(*appender, msg.timestamp, MsgType::Trade, msg);
    }
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  SECTION("Can seek to specific time") {
    auto cursor =
        std::make_shared<MergingCursor>(MergingCursorOptions{.prefault = true});
    cursor->join(location, 0, base_time + 50000); // Seek to message 50

    if (cursor->isDataAvailable()) {
      auto frame = cursor->tryCurrentFrame();
      const auto &msg = frame->data<TestMessage>();
      REQUIRE(msg.id >= 50); // Should start at or after message 50
    }
  }

  SECTION("Can seek during reading") {
    auto cursor =
        std::make_shared<MergingCursor>(MergingCursorOptions{.prefault = true});
    cursor->join(location, 0, 0);

    // Read first 20 messages
    for (int i = 0; i < 20 && cursor->isDataAvailable(); ++i) {
      cursor->next();
    }

    // Seek to message 50
    cursor->seekToTime(base_time + 50000);

    if (cursor->isDataAvailable()) {
      auto frame = cursor->tryCurrentFrame();
      const auto &msg = frame->data<TestMessage>();
      REQUIRE(msg.id >= 50);
    }
  }
}

// ============================================================================
// Test 6: Performance - Sort Optimization
// ============================================================================

TEST_CASE_METHOD(MergingCursorFixture, "MergingCursor performance",
                 "[cursor][perf][!benchmark]") {
  const int NUM_MESSAGES = 10000;
  writeTestData(0, NUM_MESSAGES);
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  auto cursor =
      std::make_shared<MergingCursor>(MergingCursorOptions{.prefault = false});
  cursor->join(location, 0, 0);

  SECTION("Reading throughput") {
    auto start = std::chrono::high_resolution_clock::now();

    int count = 0;
    while (cursor->isDataAvailable()) {
      auto frame = cursor->tryCurrentFrame();
      [[maybe_unused]] const auto &msg = frame->data<TestMessage>();
      count++;
      cursor->next();
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration =
        std::chrono::duration_cast<std::chrono::microseconds>(end - start);

    double avg_latency = static_cast<double>(duration.count()) / count;
    double throughput = (count * 1000000.0) / duration.count();

    INFO("Read " << count << " messages");
    INFO("Average latency: " << avg_latency << " µs");
    INFO("Throughput: " << throughput << " msgs/sec");

    REQUIRE(count == NUM_MESSAGES);
    REQUIRE(avg_latency < 2.0); // Should be fast
  }
}

// ============================================================================
// Test 7: Edge Cases
// ============================================================================

TEST_CASE_METHOD(MergingCursorFixture, "MergingCursor edge cases",
                 "[cursor][edge]") {
  SECTION("Reading empty stream") {
    writeTestData(0, 0); // Empty
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    auto cursor =
        std::make_shared<MergingCursor>(MergingCursorOptions{.prefault = true});
    cursor->join(location, 0, 0);

    REQUIRE_FALSE(cursor->isDataAvailable());
  }

  SECTION("Reading after disjoin all") {
    writeTestData(0, 10);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    auto cursor =
        std::make_shared<MergingCursor>(MergingCursorOptions{.prefault = true});
    cursor->join(location, 0, 0);

    REQUIRE(cursor->isDataAvailable());

    cursor->disjoin(location->uid);

    REQUIRE_FALSE(cursor->isDataAvailable());
  }

  SECTION("next() on empty cursor is safe") {
    auto cursor =
        std::make_shared<MergingCursor>(MergingCursorOptions{.prefault = true});

    cursor->next(); // Should not crash
    REQUIRE_FALSE(cursor->isDataAvailable());
  }

  SECTION("tryCurrentFrame on empty cursor") {
    auto cursor =
        std::make_shared<MergingCursor>(MergingCursorOptions{.prefault = true});

    auto frame = cursor->tryCurrentFrame();
    REQUIRE_FALSE(frame);
  }
}

// ============================================================================
// Test 8: Concurrent Reading
// ============================================================================

TEST_CASE_METHOD(MergingCursorFixture, "MergingCursor thread safety",
                 "[cursor][thread]") {
  const int NUM_MESSAGES = 1000;
  writeTestData(0, NUM_MESSAGES);
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  SECTION("Multiple cursors can read same stream") {
    std::vector<std::thread> threads;
    std::atomic<int> total_read{0};

    for (int i = 0; i < 3; ++i) {
      threads.emplace_back([&]() {
        auto cursor = std::make_shared<MergingCursor>(
            MergingCursorOptions{.prefault = true});
        cursor->join(location, 0, 0);

        int count = 0;
        while (cursor->isDataAvailable()) {
          cursor->next();
          count++;
        }

        total_read += count;
      });
    }

    for (auto &t : threads) {
      t.join();
    }

    // Each cursor should read all messages
    REQUIRE(total_read == NUM_MESSAGES * 3);
  }
}

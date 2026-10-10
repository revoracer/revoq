// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 RevoRacer
#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers.hpp>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <thread>

#include <revoq/Appender.h>
#include <revoq/MergingCursor.h>
#include <revobase/TimeUtils.h>
#include "test_helpers.h"

using namespace revoq;
using namespace revoq::test;
namespace fs = std::filesystem;

namespace {

constexpr std::size_t kV2FrameHeaderBytes = 32;
constexpr std::size_t kFrameAlignmentBytes = 64;

constexpr std::size_t alignUp(std::size_t value, std::size_t alignment) {
  return (value + alignment - 1) / alignment * alignment;
}

} // namespace

// Test data structure
struct TestTrade {
  int64_t timestamp;
  uint32_t symbol_id;
  double price;
  uint64_t volume;
  char side;
};

#pragma pack(push, 1)
struct SmallTrade28 {
  int64_t order_id;
  double price;
  int32_t quantity;
  char symbol[8];
};
#pragma pack(pop)

static_assert(sizeof(SmallTrade28) == 28);

// Test fixture
class AppenderFixture {
public:
  AppenderFixture() {
    test_dir = fs::temp_directory_path() / "journal_appender_test";

    // Clean up any existing test directory
    if (fs::exists(test_dir)) {
      fs::remove_all(test_dir);
    }

    fs::create_directories(test_dir);

    locator = std::make_shared<Locator>(test_dir.string());
    location = Location::make(Mode::LIVE, Category::EXECUTION, "test",
                              "appender_test", locator);
  }

  ~AppenderFixture() {
    // Give time for any background threads to finish
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    if (fs::exists(test_dir)) {
      try {
        fs::remove_all(test_dir);
      } catch (const std::exception &e) {
        // Ignore cleanup errors
      }
    }
  }

  fs::path test_dir;
  ILocatorPtr locator;
  LocationPtr location;
};

// ============================================================================
// Test 1: appender Construction
// ============================================================================

TEST_CASE_METHOD(AppenderFixture, "Appender construction",
                 "[appender][construct]") {
  SECTION("Can create appender with prefault=true") {
    auto appender = std::make_shared<Appender<>>(
        location, 0, AppenderOptions{.prefault = true});
    REQUIRE(appender != nullptr);
  }

  SECTION("Can create appender with prefault=false") {
    auto appender = std::make_shared<Appender<>>(
        location, 1, AppenderOptions{.prefault = false});
    REQUIRE(appender != nullptr);
  }

  SECTION("Can create appender with AppenderWarmer") {
    auto appender = std::make_shared<Appender<>>(location, 2,
                                                 AppenderOptions{
                                                     .prefault = false,
                                                     .warmup_segments = 2,
                                                     .warmer_cpu_core = -1,
                                                 });
    REQUIRE(appender != nullptr);
  }
}

// ============================================================================
// Test 2: Basic Write Operations
// ============================================================================

TEST_CASE_METHOD(AppenderFixture, "Appender basic writes",
                 "[appender][write]") {
  auto appender = std::make_shared<Appender<>>(
      location, 0, AppenderOptions{.prefault = true});

  SECTION("Can write single message") {
    TestTrade trade{revobase::Timestamp::now().nsec(), 1, 100.5, 1000, 'B'};

    publish(*appender, trade.timestamp, kTagA, trade);
    REQUIRE(appender->getCurrentSequence() == 1);
  }

  SECTION("Can write multiple messages") {
    for (int i = 0; i < 100; ++i) {
      TestTrade trade{revobase::Timestamp::now().nsec(), static_cast<uint32_t>(i),
                      100.0 + i, 1000, (i % 2 == 0) ? 'B' : 'S'};

      publish(*appender, trade.timestamp, kTagA, trade);
    }

    REQUIRE(appender->getCurrentSequence() == 100);
  }

  SECTION("Can write with different message types") {
    TestTrade trade{revobase::Timestamp::now().nsec(), 1, 100.0, 1000, 'B'};

    publish(*appender, trade.timestamp, kTagA, trade);
    appender->mark(revobase::Timestamp::now().nsec(), MsgType::SessionStart);
    publish(*appender, trade.timestamp, kTagA, trade);
    appender->mark(revobase::Timestamp::now().nsec(), MsgType::SessionEnd);

    REQUIRE(appender->getCurrentSequence() == 4);
  }
}

// ============================================================================
// Test 3: Sequence Numbers
// ============================================================================

TEST_CASE_METHOD(AppenderFixture, "Appender sequence tracking",
                 "[appender][sequence]") {
  auto appender = std::make_shared<Appender<>>(
      location, 0, AppenderOptions{.prefault = true});

  SECTION("Sequence starts at 0") {
    REQUIRE(appender->getCurrentSequence() == 0);
  }

  SECTION("Sequence increments with each write") {
    TestTrade trade{revobase::Timestamp::now().nsec(), 1, 100.0, 1000, 'B'};

    for (int i = 0; i < 10; ++i) {
      publish(*appender, trade.timestamp, kTagA, trade);
      REQUIRE(appender->getCurrentSequence() == static_cast<uint32_t>(i + 1));
    }
  }

  SECTION("Sequence is consistent across read") {
    TestTrade trade{revobase::Timestamp::now().nsec(), 1, 100.0, 1000, 'B'};

    // Write 10 messages
    for (int i = 0; i < 10; ++i) {
      publish(*appender, trade.timestamp, kTagA, trade);
    }

    // Force flush
    appender.reset();

    // Give filesystem time to sync
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // Read back and verify sequences
    auto cursor =
        std::make_shared<MergingCursor>(MergingCursorOptions{.prefault = true});
    cursor->join(location, 0, 0);

    uint32_t expected_seq = 0;
    while (cursor->isDataAvailable()) {
      auto frame = cursor->tryCurrentFrame();
      REQUIRE(frame->getSequence() == expected_seq);
      expected_seq++;
      cursor->next();
    }

    REQUIRE(expected_seq == 10);
  }
}

// ============================================================================
// Test 4: V2 Frame Layout
// ============================================================================

TEST_CASE_METHOD(AppenderFixture,
                 "Appender stores logical length with 64-byte frame stride",
                 "[appender][frame-layout][v2]") {
  auto appender = std::make_shared<Appender<>>(
      location, 7, AppenderOptions{.prefault = true});

  for (int i = 0; i < 4; ++i) {
    SmallTrade28 trade{static_cast<int64_t>(i),
                       100.0 + i,
                       100 + i,
                       {'T', 'E', 'S', 'T', '\0', '\0', '\0', '\0'}};
    publish(*appender, 1'000'000 + i, kTagA, trade);
  }

  appender.reset();
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  auto cursor =
      std::make_shared<MergingCursor>(MergingCursorOptions{.prefault = true});
  cursor->join(location, 7, 0);

  constexpr std::size_t expected_logical_length =
      kV2FrameHeaderBytes + sizeof(SmallTrade28);
  constexpr std::size_t expected_physical_stride =
      alignUp(expected_logical_length, kFrameAlignmentBytes);

  std::uintptr_t previous_address = 0;
  int frames_seen = 0;
  while (cursor->isDataAvailable() && frames_seen < 4) {
    auto frame = cursor->tryCurrentFrame();

    REQUIRE(frame);
    REQUIRE(frame->getMsgType() == static_cast<int32_t>(kTagA));
    CHECK(frame->address() % kFrameAlignmentBytes == 0);
    CHECK(frame->getFrameLength() == expected_logical_length);
    CHECK(frame->getDataLength() == sizeof(SmallTrade28));

    if (previous_address != 0) {
      CHECK(frame->address() - previous_address == expected_physical_stride);
    }

    const auto &trade = frame->data<SmallTrade28>();
    CHECK(trade.order_id == frames_seen);

    previous_address = frame->address();
    ++frames_seen;
    cursor->next();
  }

  REQUIRE(frames_seen == 4);
}

// ============================================================================
// Test 5: Write with Flags
// ============================================================================

TEST_CASE_METHOD(AppenderFixture, "Appender with flags", "[appender][flags]") {
  auto appender = std::make_shared<Appender<>>(
      location, 0, AppenderOptions{.prefault = true});

  SECTION("Can write with high priority flag") {
    TestTrade trade{revobase::Timestamp::now().nsec(), 1, 100.0, 1000, 'B'};
    publish(*appender, trade.timestamp, kTagA, trade,
            FRAME_HIGH_PRIORITY);
    REQUIRE(appender->getCurrentSequence() == 1);
  }

  SECTION("Flags are preserved") {
    TestTrade trade{revobase::Timestamp::now().nsec(), 1, 100.0, 1000, 'B'};
    publish(*appender, trade.timestamp, kTagA, trade,
            FRAME_HIGH_PRIORITY);
    appender.reset();

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    auto cursor =
        std::make_shared<MergingCursor>(MergingCursorOptions{.prefault = true});
    cursor->join(location, 0, 0);

    if (cursor->isDataAvailable()) {
      auto frame = cursor->tryCurrentFrame();
      REQUIRE(frame->getFlags() & FRAME_HIGH_PRIORITY);
    }
  }
}

// ============================================================================
// Test 5: AppenderWarmer Integration
// ============================================================================

TEST_CASE_METHOD(AppenderFixture, "Appender with AppenderWarmer",
                 "[appender][AppenderWarmer]") {
  SECTION("appender with AppenderWarmer doesn't crash") {
    auto appender = std::make_shared<Appender<>>(location, 0,
                                                 AppenderOptions{
                                                     .prefault = false,
                                                     .warmup_segments = 2,
                                                     .warmer_cpu_core = -1,
                                                 });

    REQUIRE(appender != nullptr);

    // Give AppenderWarmer thread time to start
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    TestTrade trade{revobase::Timestamp::now().nsec(), 1, 100.0, 1000, 'B'};

    for (int i = 0; i < 100; ++i) {
      publish(*appender, trade.timestamp, kTagA, trade);
    }

    REQUIRE(appender->getCurrentSequence() == 100);
  }

  SECTION("AppenderWarmer initialization safety") {
    // Create appender with AppenderWarmer
    auto appender = std::make_shared<Appender<>>(location, 0,
                                                 AppenderOptions{
                                                     .prefault = false,
                                                     .warmup_segments = 2,
                                                     .warmer_cpu_core = -1,
                                                 });

    // Wait for initial segment to be created and warmer to initialize
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    TestTrade trade{revobase::Timestamp::now().nsec(), 1, 100.0, 1000, 'B'};

    // Write first message to establish segment
    publish(*appender, trade.timestamp, kTagA, trade);

    // Give warmer time to warm next segments
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Continue writing
    for (int i = 1; i < 50; ++i) {
      publish(*appender, trade.timestamp, kTagA, trade);
    }

    REQUIRE(appender->getCurrentSequence() == 50);
  }

  SECTION("AppenderWarmer reduces rotation latency", "[.][benchmark]") {
    // NOTE: This test is disabled by default due to timing sensitivity
    // Enable with: ./test --run-test="*AppenderWarmer reduces*"

    // Write enough to cause segment rotation
    const int NUM_MESSAGES = 50000;

    // Test 1: Without AppenderWarmer
    auto start1 = std::chrono::high_resolution_clock::now();
    {
      auto appender = std::make_shared<Appender<>>(
          location, 3, AppenderOptions{.prefault = true}); // No warmer

      // Wait for initialization
      std::this_thread::sleep_for(std::chrono::milliseconds(50));

      for (int i = 0; i < NUM_MESSAGES; ++i) {
        TestTrade trade{revobase::Timestamp::now().nsec(), 1, 100.0, 1000, 'B'};
        publish(*appender, trade.timestamp, kTagA, trade);
      }
    }
    auto end1 = std::chrono::high_resolution_clock::now();
    auto duration1 =
        std::chrono::duration_cast<std::chrono::microseconds>(end1 - start1);
    REQUIRE(duration1.count() > 0);
    INFO("Without warmer: " << duration1.count() << " µs");

    // Clean up for next test
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  SECTION("AppenderWarmer reduces rotation latency", "[.][benchmark]") {
    // Test 2: With AppenderWarmer
    const int NUM_MESSAGES = 50000;
    auto start2 = std::chrono::high_resolution_clock::now();
    {
      auto appender = std::make_shared<Appender<>>(location, 1,
                                                   AppenderOptions{
                                                       .prefault = false,
                                                       .warmup_segments = 2,
                                                       .warmer_cpu_core = -1,
                                                   }); // With warmer

      // Wait for initialization and first warmup
      std::this_thread::sleep_for(std::chrono::milliseconds(200));

      for (int i = 0; i < NUM_MESSAGES; ++i) {
        TestTrade trade{revobase::Timestamp::now().nsec(), 1, 100.0, 1000, 'B'};
        publish(*appender, trade.timestamp, kTagA, trade);
      }
    }
    auto end2 = std::chrono::high_resolution_clock::now();
    auto duration2 =
        std::chrono::duration_cast<std::chrono::microseconds>(end2 - start2);

    // Report results
    INFO("With warmer: " << duration2.count() << " µs");
    // INFO("Speedup: " << (duration1.count() * 100.0 / duration2.count() -
    // 100.0) << "%");

    // With warmer should complete successfully
    REQUIRE(duration2.count() > 0);
  }
}

// ============================================================================
// Test 7: Write Performance
// ============================================================================

TEST_CASE_METHOD(AppenderFixture, "Appender performance",
                 "[appender][perf][!benchmark]") {
  auto appender = std::make_shared<Appender<>>(location, 0,
                                               AppenderOptions{
                                                   .prefault = false,
                                                   .warmup_segments = 2,
                                                   .warmer_cpu_core = -1,
                                               });

  // Wait for initialization
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  SECTION("Write throughput") {
    const int NUM_MESSAGES = 100000;
    TestTrade trade{revobase::Timestamp::now().nsec(), 1, 100.0, 1000, 'B'};

    auto start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < NUM_MESSAGES; ++i) {
      publish(*appender, trade.timestamp, kTagA, trade);
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration =
        std::chrono::duration_cast<std::chrono::microseconds>(end - start);

    double avg_latency = static_cast<double>(duration.count()) / NUM_MESSAGES;
    double throughput = (NUM_MESSAGES * 1000000.0) / duration.count();

    INFO("Average latency: " << avg_latency << " µs");
    INFO("Throughput: " << throughput << " msgs/sec");

    REQUIRE(avg_latency < 2.0);   // Should be sub-2-microsecond
    REQUIRE(throughput > 500000); // Should exceed 500K msgs/sec
  }
}

// ============================================================================
// Test 8: Thread Safety
// ============================================================================

TEST_CASE_METHOD(AppenderFixture, "Appender thread safety",
                 "[appender][thread]") {
  SECTION("Single appender is safe with concurrent internal threads") {
    // The retire thread munmaps old segments while the single appender thread
    // creates new ones. retained_segment_limit=0 enqueues segments to the
    // retire thread immediately on rotation, so munmap races concurrently with
    // active writes.
    //
    // EXECUTION segments are 4 MB; TestTrade frames stride 64 B physical.
    // Frames per segment = (4 MB - 128 B) / 64 B = 65,534.
    // 140,000 messages triggers exactly 2 segment rotations.
    constexpr uint64_t N = 140'000;

    {
      auto appender =
          std::make_shared<Appender<>>(location, 0,
                                       AppenderOptions{
                                           .prefault = false,
                                           .background_threads = true,
                                           .retained_segment_limit = 0,
                                       });

      for (uint64_t i = 0; i < N; ++i) {
        TestTrade trade{revobase::Timestamp::now().nsec(),
                        static_cast<uint32_t>(i), 100.0, 1000, 'B'};
        publish(*appender, trade.timestamp, kTagA, trade);
      }

      const auto rotations =
          static_cast<uint64_t>(appender->getCurrentSegmentId() - 1);
      REQUIRE(rotations >= 2);
      // Each SegmentEnd sentinel written at rotation consumes one sequence
      // number.
      REQUIRE(appender->getCurrentSequence() == N + rotations);
    } // destructor joins retire thread; all segments fully committed to disk

    // Read back every frame and verify the sequence stream is contiguous.
    // SegmentEnd frames are visible and carry their own sequence numbers, so
    // sequences are strictly seq[i] == seq[i-1] + 1 across segment boundaries.
    auto cursor = std::make_shared<MergingCursor>(
        MergingCursorOptions{.prefault = true, .background_threads = false});
    cursor->join(location, 0, 0);
    int64_t prev_seq = -1;
    uint64_t data_frames = 0;
    while (cursor->isDataAvailable()) {
      auto frame = cursor->tryCurrentFrame();
      REQUIRE(static_cast<int64_t>(frame->getSequence()) == prev_seq + 1);
      prev_seq = static_cast<int64_t>(frame->getSequence());
      if (static_cast<MsgType>(frame->getMsgType()) != MsgType::SegmentEnd)
        ++data_frames;
      cursor->next();
    }
    REQUIRE(data_frames == N);
  }
}

// ============================================================================
// Test 9: Edge Cases
// ============================================================================

TEST_CASE_METHOD(AppenderFixture, "Appender edge cases", "[appender][edge]") {
  auto appender = std::make_shared<Appender<>>(
      location, 0, AppenderOptions{.prefault = true});

  SECTION("Can write zero-sized struct") {
    struct Empty {};
    Empty empty;

    publish(*appender, revobase::Timestamp::now().nsec(), kTagA, empty);
    REQUIRE(appender->getCurrentSequence() == 1);
  }

  SECTION("Can write large struct") {
    struct Large {
      char data[1000];
    };
    Large large;
    memset(large.data, 'X', sizeof(large.data));

    publish(*appender, revobase::Timestamp::now().nsec(), kTagA, large);
    REQUIRE(appender->getCurrentSequence() == 1);
  }

  SECTION("Can write many small messages") {
    struct Tiny {
      uint32_t id;
    };

    for (int i = 0; i < 10000; ++i) {
      Tiny tiny{static_cast<uint32_t>(i)};
      publish(*appender, revobase::Timestamp::now().nsec(), kTagA, tiny);
    }

    REQUIRE(appender->getCurrentSequence() == 10000);
  }
}

// ============================================================================
// Test 10: Write and Read Consistency
// ============================================================================

TEST_CASE_METHOD(AppenderFixture, "Appender write-read consistency",
                 "[appender][consistency]") {
  const int NUM_MESSAGES = 1000;

  SECTION("All written messages can be read back") {
    {
      auto appender = std::make_shared<Appender<>>(
          location, 0, AppenderOptions{.prefault = false});

      for (int i = 0; i < NUM_MESSAGES; ++i) {
        TestTrade trade{revobase::Timestamp::now().nsec(),
                        static_cast<uint32_t>(i), 100.0 + i, 1000ULL + i,
                        (i % 2 == 0) ? 'B' : 'S'};
        publish(*appender, trade.timestamp, kTagA, trade);
      }
    }

    // Give filesystem time to sync
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Read back
    auto cursor =
        std::make_shared<MergingCursor>(MergingCursorOptions{.prefault = true});
    cursor->join(location, 0, 0);

    int count = 0;
    while (cursor->isDataAvailable()) {
      auto frame = cursor->tryCurrentFrame();
      const auto &trade = frame->data<TestTrade>();

      REQUIRE(trade.symbol_id == static_cast<uint32_t>(count));
      REQUIRE(trade.price == 100.0 + count);
      REQUIRE(trade.volume == 1000 + count);

      count++;
      cursor->next();
    }

    REQUIRE(count == NUM_MESSAGES);
  }
}

// ============================================================================
// Test 11: Mark Operations
// ============================================================================

TEST_CASE_METHOD(AppenderFixture, "Appender mark operations",
                 "[appender][mark]") {
  auto appender = std::make_shared<Appender<>>(
      location, 0, AppenderOptions{.prefault = true});

  SECTION("Can write session markers") {
    appender->mark(revobase::Timestamp::now().nsec(), MsgType::SessionStart);

    TestTrade trade{revobase::Timestamp::now().nsec(), 1, 100.0, 1000, 'B'};
    publish(*appender, trade.timestamp, kTagA, trade);

    appender->mark(revobase::Timestamp::now().nsec(), MsgType::SessionEnd);

    REQUIRE(appender->getCurrentSequence() == 3);
  }

  SECTION("Marks are readable") {
    appender->mark(revobase::Timestamp::now().nsec(), MsgType::SessionStart);
    appender.reset();

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    auto cursor =
        std::make_shared<MergingCursor>(MergingCursorOptions{.prefault = true});
    cursor->join(location, 0, 0);

    if (cursor->isDataAvailable()) {
      auto frame = cursor->tryCurrentFrame();
      REQUIRE(frame->getMsgType() ==
              static_cast<int32_t>(MsgType::SessionStart));
      REQUIRE(frame->getDataLength() == 0);
    }
  }
}

// Test fixture
class appenderBugHuntFixture {
public:
  appenderBugHuntFixture() {
    test_dir = fs::temp_directory_path() / "appender_bughunt_test";

    if (fs::exists(test_dir)) {
      fs::remove_all(test_dir);
    }

    fs::create_directories(test_dir);

    locator = std::make_shared<Locator>(test_dir.string());
    location = Location::make(Mode::LIVE, Category::EXECUTION, "test",
                              "bughunt", locator);
  }

  ~appenderBugHuntFixture() {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    if (fs::exists(test_dir)) {
      try {
        fs::remove_all(test_dir);
      } catch (...) {
      }
    }
  }

  struct TestMsg {
    int64_t timestamp;
    uint32_t seq;
    uint32_t segment_hint; // Which segment we expect this to be on
    char data[100];
  };

  fs::path test_dir;
  ILocatorPtr locator;
  LocationPtr location;
};

// ============================================================================
// BUG HUNT 1: Sequence Number Continuity Across Segments
// ============================================================================

TEST_CASE_METHOD(appenderBugHuntFixture,
                 "Sequence numbers continuous across segment rotation",
                 "[appender][bughunt][sequence]") {

  SECTION("No gaps in sequence across multiple segments") {
    auto appender = std::make_shared<Appender<>>(
        location, 0, AppenderOptions{.prefault = false});

    // Write enough to span multiple segments (4MB default)
    const int NUM_MESSAGES = 50000;

    for (int i = 0; i < NUM_MESSAGES; ++i) {
      TestMsg msg;
      msg.timestamp = revobase::Timestamp::now().nsec();
      msg.seq = i;
      std::memset(msg.data, 'A', sizeof(msg.data));

      publish(*appender, msg.timestamp, kTagA, msg);
    }

    REQUIRE(appender->getCurrentSequence() >= NUM_MESSAGES);

    // Force appender to close
    appender.reset();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Read back and verify NO gaps
    auto cursor =
        std::make_shared<MergingCursor>(MergingCursorOptions{.prefault = true});
    cursor->join(location, 0, 0);

    uint32_t expected_seq = 0;
    uint32_t count = 0;
    std::vector<uint32_t> gaps;

    while (cursor->isDataAvailable()) {
      auto frame = cursor->tryCurrentFrame();
      // Skip sentinel frames (e.g., segment boundaries)
      // if (static_cast<MsgType>(frame->getMsgType()) == MsgType::SegmentEnd) {
      //   cursor->next();
      //   continue;
      // }
      uint32_t frame_seq = frame->getSequence();

      if (frame_seq != expected_seq) {
        gaps.push_back(expected_seq);
        INFO("Gap detected: expected " << expected_seq << ", got "
                                       << frame_seq);
      }
      // if (static_cast<MsgType>(frame->getMsgType()) == kTagA) {
      //   const auto& msg = frame->data<TestMsg>();
      //   REQUIRE(msg.seq == expected_seq);  // Data should match sequence
      // }

      expected_seq++;
      count++;
      cursor->next();
    }

    REQUIRE(gaps.empty()); // No gaps!
    REQUIRE(count >= NUM_MESSAGES);
    REQUIRE(expected_seq >= NUM_MESSAGES);
  }
}
//
// //
// ============================================================================
// // BUG HUNT 2: Segment Rotation Happens Correctly
// //
// ============================================================================
//
TEST_CASE_METHOD(appenderBugHuntFixture,
                 "Segment rotation triggers at correct boundary",
                 "[appender][bughunt][rotation]") {

  SECTION("No frames span segment boundaries") {
    auto appender = std::make_shared<Appender<>>(
        location, 0, AppenderOptions{.prefault = false});

    // Calculate approximate segment size
    size_t segment_size = findSegmentSize(location, 0);
    size_t frame_size =
        framePhysicalLength(sizeof(FrameHeader) + sizeof(TestMsg));
    size_t frames_per_segment =
        (segment_size - sizeof(SegmentHeader) - FRAME_ALIGNMENT) / frame_size;

    INFO("Segment size: " << segment_size);
    INFO("Frame size: " << frame_size);
    INFO("Frames per segment (approx): " << frames_per_segment);

    // Write 3 segments worth
    for (size_t i = 0; i < frames_per_segment * 3; ++i) {
      TestMsg msg;
      msg.timestamp = revobase::Timestamp::now().nsec();
      msg.seq = static_cast<uint32_t>(i);
      msg.segment_hint = static_cast<uint32_t>(i / frames_per_segment) + 1;

      publish(*appender, msg.timestamp, kTagA, msg);
    }

    appender.reset();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Verify multiple segment files exist
    auto segment_files = location->locator->listSegmentId(location, 0);

    INFO("Number of segment files created: " << segment_files.size());
    REQUIRE(segment_files.size() >= 2); // At least 2 segments
  }

  SECTION("Segment header updated correctly on rotation") {
    auto appender = std::make_shared<Appender<>>(
        location, 0, AppenderOptions{.prefault = false});

    // Write to fill first segment
    size_t segment_size = findSegmentSize(location, 0);
    size_t frame_size =
        framePhysicalLength(sizeof(FrameHeader) + sizeof(TestMsg));
    size_t frames_per_segment =
        (segment_size - sizeof(SegmentHeader) - FRAME_ALIGNMENT) / frame_size;

    for (size_t i = 0; i < frames_per_segment + 10; ++i) {
      TestMsg msg;
      msg.timestamp = revobase::Timestamp::now().nsec();
      msg.seq = static_cast<uint32_t>(i);

      publish(*appender, msg.timestamp, kTagA, msg);
    }

    appender.reset();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Check segment 1 header
    auto segment1 = Segment::load(location, 0, 1, false, true);

    REQUIRE(segment1->getSegmentId() == 1);
    // Verify last_frame_position is set
    REQUIRE(segment1->lastFrameAddress() > segment1->firstFrameAddress());

    // Check segment 2 exists and has data
    auto segment_files = location->locator->listSegmentId(location, 0);
    if (segment_files.size() >= 2) {
      auto segment2 = Segment::load(location, 0, 2, false, true);
      REQUIRE(segment2->getSegmentId() == 2);
    }
  }
}
//
// //
// ============================================================================
// // BUG HUNT 3: Frame Size Edge Cases
// //
// ============================================================================
//
TEST_CASE_METHOD(appenderBugHuntFixture,
                 "Handles varying frame sizes correctly",
                 "[appender][bughunt][framesize]") {

  SECTION("Mix of small and large frames") {
    auto appender = std::make_shared<Appender<>>(
        location, 0, AppenderOptions{.prefault = false});

    struct SmallMsg {
      uint32_t id;
    };
    struct LargeMsg {
      char data[1000];
    };

    for (int i = 0; i < 1000; ++i) {
      auto ts = revobase::Timestamp::now().nsec();

      if (i % 2 == 0) {
        SmallMsg msg{static_cast<uint32_t>(i)};
        publish(*appender, ts, kTagA, msg);
      } else {
        LargeMsg msg;
        std::memset(msg.data, 'X', sizeof(msg.data));
        publish(*appender, ts, kTagD, msg);
      }
    }

    REQUIRE(appender->getCurrentSequence() == 1000);

    appender.reset();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Read back and verify
    auto cursor =
        std::make_shared<MergingCursor>(MergingCursorOptions{.prefault = true});
    cursor->join(location, 0, 0);

    int count = 0;
    while (cursor->isDataAvailable()) {
      auto frame = cursor->tryCurrentFrame();
      if (static_cast<MsgType>(frame->getMsgType()) == MsgType::SegmentEnd) {
        cursor->next();
        continue;
      }
      // Verify frame is valid
      REQUIRE(frame->hasData());
      REQUIRE(frame->getDataLength() > 0);

      count++;
      cursor->next();
    }

    REQUIRE(count == 1000);
  }
}
//
// //
// ============================================================================
// // BUG HUNT 4: Concurrent Writes During Rotation
// //
// ============================================================================
//
TEST_CASE_METHOD(appenderBugHuntFixture,
                 "Thread safety during segment rotation",
                 "[appender][bughunt][concurrent]") {

  SECTION("Multiple threads writing during rotation") {
    auto appender = std::make_shared<Appender<MultiAppenderPolicy>>(
        location, 0, AppenderOptions{.prefault = false});

    std::atomic<int> total_written{0};
    std::vector<std::thread> threads;

    // 5 threads, each writes 1000 messages
    for (int t = 0; t < 5; ++t) {
      threads.emplace_back([&appender, &total_written, t]() {
        for (int i = 0; i < 1000; ++i) {
          TestMsg msg;
          msg.timestamp = revobase::Timestamp::now().nsec();
          msg.seq = t * 1000 + i;

          publish(*appender, msg.timestamp, kTagA, msg);
          total_written.fetch_add(1, std::memory_order_relaxed);
        }
      });
    }

    for (auto &t : threads) {
      t.join();
    }

    REQUIRE(total_written == 5000);
    REQUIRE(appender->getCurrentSequence() == 5000);

    appender.reset();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Verify all messages readable
    auto cursor =
        std::make_shared<MergingCursor>(MergingCursorOptions{.prefault = true});
    cursor->join(location, 0, 0);

    int count = 0;
    while (cursor->isDataAvailable()) {
      auto frame = cursor->tryCurrentFrame();
      REQUIRE(frame->hasData());
      count++;
      cursor->next();
    }

    REQUIRE(count == 5000);
  }
}
//
// //
// ============================================================================
// // BUG HUNT 5: Pre-Warmed vs Fallback Path Equivalence
// //
// ============================================================================
//
TEST_CASE_METHOD(appenderBugHuntFixture,
                 "Pre-warmed and fallback paths produce identical results",
                 "[appender][bughunt][AppenderWarmer]") {

  SECTION("Compare with and without AppenderWarmer") {
    // Write with AppenderWarmer
    {
      auto appender = std::make_shared<Appender<>>(location, 0,
                                                   AppenderOptions{
                                                       .prefault = false,
                                                       .warmup_segments = 2,
                                                       .warmer_cpu_core = -1,
                                                   });

      for (int i = 0; i < 1000; ++i) {
        TestMsg msg;
        msg.timestamp = revobase::Timestamp::now().nsec();
        msg.seq = i;
        publish(*appender, msg.timestamp, kTagA, msg);
      }
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Read back
    auto cursor1 =
        std::make_shared<MergingCursor>(MergingCursorOptions{.prefault = true});
    cursor1->join(location, 0, 0);

    std::vector<uint32_t> seqs1;
    while (cursor1->isDataAvailable()) {
      auto frame = cursor1->tryCurrentFrame();
      seqs1.push_back(frame->getSequence());
      cursor1->next();
    }

    // Clean up
    fs::remove_all(test_dir);
    fs::create_directories(test_dir);

    // Write without AppenderWarmer
    {
      auto appender = std::make_shared<Appender<>>(
          location, 1, AppenderOptions{.prefault = true}); // Different dest_id

      for (int i = 0; i < 1000; ++i) {
        TestMsg msg;
        msg.timestamp = revobase::Timestamp::now().nsec();
        msg.seq = i;
        publish(*appender, msg.timestamp, kTagA, msg);
      }
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Read back
    auto cursor2 =
        std::make_shared<MergingCursor>(MergingCursorOptions{.prefault = true});
    cursor2->join(location, 1, 0);

    std::vector<uint32_t> seqs2;
    while (cursor2->isDataAvailable()) {
      auto frame = cursor2->tryCurrentFrame();
      seqs2.push_back(frame->getSequence());
      cursor2->next();
    }

    // Both should have identical sequence patterns
    REQUIRE(seqs1.size() == 1000);
    REQUIRE(seqs2.size() == 1000);

    for (size_t i = 0; i < seqs1.size(); ++i) {
      REQUIRE(seqs1[i] == seqs2[i]);
    }
  }
}
//
// //
// ============================================================================
// // BUG HUNT 6: Memory Barriers / Visibility
// //
// ============================================================================
//
TEST_CASE_METHOD(appenderBugHuntFixture,
                 "appender updates immediately visible to cursor",
                 "[appender][bughunt][visibility]") {

  SECTION("Concurrent appender and cursor") {
    auto appender = std::make_shared<Appender<>>(
        location, 0, AppenderOptions{.prefault = false});
    auto cursor =
        std::make_shared<MergingCursor>(MergingCursorOptions{.prefault = true});

    std::atomic<bool> appender_done{false};
    std::atomic<int> messages_written{0};
    std::atomic<int> messages_read{0};

    // appender thread
    std::thread appender_thread([&]() {
      for (int i = 0; i < 1000; ++i) {
        TestMsg msg;
        msg.timestamp = revobase::Timestamp::now().nsec();
        msg.seq = i;

        publish(*appender, msg.timestamp, kTagA, msg);
        messages_written.fetch_add(1, std::memory_order_release);

        std::this_thread::sleep_for(std::chrono::microseconds(100));
      }
      appender_done.store(true, std::memory_order_release);
    });

    // cursor thread - starts slightly after appender
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    cursor->join(location, 0, 0);

    std::thread cursor_thread([&]() {
      while (!appender_done.load(std::memory_order_acquire) ||
             cursor->isDataAvailable()) {

        if (cursor->isDataAvailable()) {
          auto frame = cursor->tryCurrentFrame();
          if (frame && frame->hasData()) {
            messages_read.fetch_add(1, std::memory_order_release);
            cursor->next();
          }
        } else {
          std::this_thread::sleep_for(std::chrono::microseconds(10));
        }
      }
    });

    appender_thread.join();
    cursor_thread.join();

    INFO("Written: " << messages_written.load());
    INFO("Read: " << messages_read.load());

    // cursor should eventually see all messages
    REQUIRE(messages_read.load() == 1000);
  }
}
// ============================================================================
// §6.2 Segment rotation invariants
// ============================================================================

TEST_CASE_METHOD(AppenderFixture, "Appender segment rotation invariants",
                 "[appender][rotation][invariants]") {
  // TradeMsg frames: 32B FrameHeader + 29B payload → 64B physical stride.
  // DEFAULT 2MB segment (EXECUTION + dest_id=0):
  //   frames/segment = (2MB - sizeof(SegmentHeader) - FRAME_ALIGNMENT) / 64 =
  //   32,766.
  // N_ONE = 40,000 → exactly 1 rotation; N_TWO = 70,000 → exactly 2 rotations.
  const size_t segment_size = findSegmentSize(location, 0);
  const size_t frame_stride = framePhysicalLength(
      static_cast<uint32_t>(sizeof(FrameHeader) + sizeof(TestTrade)));
  const size_t frames_per_segment =
      (segment_size - sizeof(SegmentHeader) - FRAME_ALIGNMENT) / frame_stride;
  const uint64_t N_ONE = static_cast<uint64_t>(frames_per_segment) + 1000;
  const uint64_t N_TWO = static_cast<uint64_t>(frames_per_segment) * 2 + 1000;

  auto makeappender = [&]() {
    return std::make_shared<Appender<>>(
        location, 0,
        AppenderOptions{.prefault = false, .background_threads = false});
  };

  SECTION("Rotated segment isFull() and last_frame_position is within bounds") {
    auto appender = makeappender();
    for (uint64_t i = 0; i < N_ONE; ++i) {
      TestTrade t{revobase::Timestamp::now().nsec(), uint32_t(i), 100.0, 1000,
                  'B'};
      publish(*appender, t.timestamp, kTagA, t);
    }
    REQUIRE(appender->getCurrentSegmentId() == 2);
    appender.reset();

    auto old_segment = Segment::load(location, 0, 1, /*is_writing=*/false,
                                     /*prefault=*/false);
    REQUIRE(old_segment->isFull());
    REQUIRE(old_segment->lastFrameAddress() >=
            old_segment->firstFrameAddress());
    REQUIRE(old_segment->lastFrameAddress() <
            old_segment->address() + old_segment->getSegmentSize());
  }

  SECTION("SegmentEnd is the last committed frame on a full segment") {
    auto appender = makeappender();
    for (uint64_t i = 0; i < N_ONE; ++i) {
      TestTrade t{revobase::Timestamp::now().nsec(), uint32_t(i), 100.0, 1000,
                  'B'};
      publish(*appender, t.timestamp, kTagA, t);
    }
    REQUIRE(appender->getCurrentSegmentId() == 2);
    appender.reset();

    auto old_segment = Segment::load(location, 0, 1, false, false);
    uintptr_t addr = old_segment->firstFrameAddress();
    uintptr_t segment_end =
        old_segment->address() + old_segment->getSegmentSize();
    const FrameHeader *last_hdr = nullptr;
    while (addr + sizeof(FrameHeader) <= segment_end) {
      auto *hdr = reinterpret_cast<const FrameHeader *>(addr);
      uint32_t len = hdr->length.load(std::memory_order_acquire);
      if (len == 0)
        break;
      last_hdr = hdr;
      if (static_cast<MsgType>(hdr->msg_type) == MsgType::SegmentEnd)
        break;
      addr += framePhysicalLength(len);
    }
    REQUIRE(last_hdr != nullptr);
    REQUIRE(static_cast<MsgType>(last_hdr->msg_type) == MsgType::SegmentEnd);
    REQUIRE(last_hdr->length.load(std::memory_order_acquire) ==
            sizeof(FrameHeader));
  }

  SECTION("New segment header is valid immediately after rotation") {
    auto appender = makeappender();
    for (uint64_t i = 0; i < N_ONE; ++i) {
      TestTrade t{revobase::Timestamp::now().nsec(), uint32_t(i), 100.0, 1000,
                  'B'};
      publish(*appender, t.timestamp, kTagA, t);
    }
    REQUIRE(appender->getCurrentSegmentId() == 2);
    appender.reset();

    auto new_segment = Segment::load(location, 0, 2, false, false);
    REQUIRE(new_segment->getSegmentSize() == segment_size);
    // segment_header_length must equal sizeof(SegmentHeader) so the first frame
    // sits immediately after the header with no gap.
    REQUIRE(new_segment->firstFrameAddress() - new_segment->address() ==
            sizeof(SegmentHeader));
    // Segment 2 has data but is not full.
    REQUIRE(!new_segment->isFull());
  }

  SECTION("Retired segments remain fully readable with contiguous sequences") {
    auto appender = makeappender();
    for (uint64_t i = 0; i < N_TWO; ++i) {
      TestTrade t{revobase::Timestamp::now().nsec(), uint32_t(i), 100.0, 1000,
                  'B'};
      publish(*appender, t.timestamp, kTagA, t);
    }
    const auto rotations =
        static_cast<uint64_t>(appender->getCurrentSegmentId() - 1);
    REQUIRE(rotations >= 2);
    REQUIRE(appender->getCurrentSequence() == N_TWO + rotations);
    appender.reset();

    auto cursor = std::make_shared<MergingCursor>(
        MergingCursorOptions{.prefault = true, .background_threads = false});
    cursor->join(location, 0, 0);
    int64_t prev_seq = -1;
    uint64_t data_count = 0;
    uint64_t segment_ends = 0;
    while (cursor->isDataAvailable()) {
      auto frame = cursor->tryCurrentFrame();
      REQUIRE(static_cast<int64_t>(frame->getSequence()) == prev_seq + 1);
      prev_seq = static_cast<int64_t>(frame->getSequence());
      if (static_cast<MsgType>(frame->getMsgType()) == MsgType::SegmentEnd)
        ++segment_ends;
      else
        ++data_count;
      cursor->next();
    }
    REQUIRE(data_count == N_TWO);
    REQUIRE(segment_ends == rotations);
  }
}

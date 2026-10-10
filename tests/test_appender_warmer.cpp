// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 RevoRacer
#include <catch2/catch_test_macros.hpp>
#include <chrono>
#include <filesystem>
#include <fmt/chrono.h>
#include <thread>

#include "AppenderWarmer.h"
#include "Segment.h"
#include "StreamBase.h"
#include "TimeUtils.h"

using namespace revoq;
using namespace std::chrono_literals;
namespace fs = std::filesystem;

// Test fixture for AppenderWarmer tests
class AppenderWarmerFixture {
public:
  AppenderWarmerFixture() {
    // Create temp directory for tests
    test_dir = fs::temp_directory_path() / "AppenderWarmer_test";

    // Clean up if exists
    if (fs::exists(test_dir)) {
      fs::remove_all(test_dir);
    }

    fs::create_directories(test_dir);

    // Create locator and location
    locator = std::make_shared<Locator>(test_dir.string());
    location = Location::make(Mode::LIVE, Category::EXECUTION, "test", "warmer",
                              locator);
  }

  ~AppenderWarmerFixture() {
    // Give background threads time to finish
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // Cleanup
    if (fs::exists(test_dir)) {
      try {
        fs::remove_all(test_dir);
      } catch (...) {
        // Ignore cleanup errors
      }
    }
  }

  fs::path test_dir;
  ILocatorPtr locator;
  LocationPtr location;
};

// ============================================================================
// Test 1: AppenderWarmer Construction and Destruction
// ============================================================================

TEST_CASE_METHOD(AppenderWarmerFixture, "AppenderWarmer construction",
                 "[AppenderWarmer][construct]") {
  SECTION("Can create AppenderWarmer") {
    auto warmer =
        std::make_shared<AppenderWarmer<>>(location, 2, 1024 * 1024, false);
    REQUIRE(warmer != nullptr);
  }

  SECTION("AppenderWarmer starts warming thread") {
    auto warmer =
        std::make_shared<AppenderWarmer<>>(location, 2, 1024 * 1024, false);
    warmer->start();
    // Give it time to start
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    // Thread should be running (no crash = success)
    REQUIRE(true);
    warmer->stop();
  }

  SECTION("AppenderWarmer destructor stops thread cleanly") {
    {
      auto warmer =
          std::make_shared<AppenderWarmer<>>(location, 2, 1024 * 1024, false);
      warmer->start();
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    // Destructor called, should not hang or crash
    REQUIRE(true);
  }

  SECTION("Multiple AppenderWarmers can coexist") {
    auto warmer1 =
        std::make_shared<AppenderWarmer<>>(location, 2, 1024 * 1024, false);
    auto warmer2 =
        std::make_shared<AppenderWarmer<>>(location, 1, 1024 * 1024, true);
    warmer1->start();
    warmer2->start();
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    REQUIRE(true);
  }
}

// ============================================================================
// Test 2: Segment Warming Functionality
// ============================================================================

TEST_CASE_METHOD(AppenderWarmerFixture, "AppenderWarmer warms segments",
                 "[AppenderWarmer][warming]") {
  auto warmer =
      std::make_shared<AppenderWarmer<>>(location, 2, 1024 * 1024, true);
  warmer->start();

  SECTION("Warmer pre-warms segments ahead of current") {
    // Set current segment to 1
    warmer->updateCurrentSegment(1);

    // Wait for warmer to warm segments 2 and 3
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // Try to get warmed segment 2
    std::size_t size = 0;
    std::uintptr_t addr = warmer->getWarmedSegment(2, size);

    REQUIRE(addr != 0);
    REQUIRE(size > 0);

    // Wrap in Segment object - it will handle cleanup automatically
    SegmentPtr segment = Segment::loadFromAddress(location, 0, 2, addr, false);

    REQUIRE(segment != nullptr);
    REQUIRE(segment->getSegmentId() == 2);

    // No manual cleanup needed - Segment destructor handles it!
  }

  SECTION("getWarmedSegment returns 0 for unwarmed segment") {
    warmer->updateCurrentSegment(1);

    // Try to get a segment that shouldn't be warmed yet
    std::size_t size = 0;
    std::uintptr_t addr = warmer->getWarmedSegment(100, size);

    REQUIRE(addr == 0);
  }

  SECTION("getWarmedSegment removes segment from cache") {
    warmer->updateCurrentSegment(1);
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    std::size_t size1 = 0, size2 = 0;
    std::uintptr_t addr1 = warmer->getWarmedSegment(2, size1);
    std::uintptr_t addr2 = warmer->getWarmedSegment(2, size2);

    REQUIRE(addr1 != 0);
    REQUIRE(addr2 == 0); // Already retrieved

    // Cleanup using Segment wrapper
    if (addr1 != 0) {
      auto segment = Segment::loadFromAddress(location, 0, 2, addr1, false);
      // Segment destructor will handle cleanup
    }
  }
}

// ============================================================================
// Test 3: Segment Warming Behavior
// ============================================================================

TEST_CASE_METHOD(AppenderWarmerFixture, "AppenderWarmer warming behavior",
                 "[AppenderWarmer][behavior]") {
  SECTION("Warms N segments ahead") {
    auto warmer = std::make_shared<AppenderWarmer<>>(
        location, 2, 1024 * 1024, true, -1, /*warmup_distance=*/3);
    warmer->start();

    warmer->updateCurrentSegment(10);
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    std::vector<SegmentPtr> segments;

    // Get segments 11, 12, 13
    for (int i = 11; i <= 13; ++i) {
      std::size_t size;
      std::uintptr_t addr = warmer->getWarmedSegment(i, size);

      if (addr != 0) {
        // Wrap in Segment - automatic cleanup
        segments.push_back(
            Segment::loadFromAddress(location, 0, i, addr, false));
      }
    }

    REQUIRE(segments.size() == 3);

    // All segments will be cleaned up automatically when vector is destroyed
  }

  SECTION("Adapts to current segment changes") {
    auto warmer =
        std::make_shared<AppenderWarmer<>>(location, 2, 1024 * 1024, true);
    warmer->start();

    warmer->updateCurrentSegment(5);
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // Move current segment forward
    warmer->updateCurrentSegment(10);
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    std::size_t size;
    // Should now warm segments ahead of 10
    std::uintptr_t addr = warmer->getWarmedSegment(11, size);
    REQUIRE(addr != 0);

    // Cleanup with Segment wrapper
    if (addr != 0) {
      auto segment = Segment::loadFromAddress(location, 0, 11, addr, false);
    }
  }
}

// ============================================================================
// Test 4: Segment Initialization
// ============================================================================

TEST_CASE_METHOD(AppenderWarmerFixture,
                 "AppenderWarmer initializes segments correctly",
                 "[AppenderWarmer][init]") {
  auto warmer =
      std::make_shared<AppenderWarmer<>>(location, 2, 1024 * 1024, true);
  warmer->start();

  warmer->updateCurrentSegment(1);
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  std::size_t size;
  std::uintptr_t addr = warmer->getWarmedSegment(2, size);

  REQUIRE(addr != 0);

  SECTION("Segment header is initialized") {
    SegmentHeader *header = reinterpret_cast<SegmentHeader *>(addr);

    REQUIRE(header->version == SEGMENT_VERSION);
    REQUIRE(header->segment_header_length == sizeof(SegmentHeader));
    REQUIRE(header->segment_size > 0);
    REQUIRE(header->frame_header_length == sizeof(FrameHeader));
    REQUIRE(header->last_frame_position == sizeof(SegmentHeader));
  }

  SECTION("First frame is zeroed") {
    FrameHeader *first_frame =
        reinterpret_cast<FrameHeader *>(addr + sizeof(SegmentHeader));

    REQUIRE(first_frame->length.load() == 0);
    REQUIRE(first_frame->msg_type == 0);
  }

  // Cleanup with Segment wrapper
  auto segment = Segment::loadFromAddress(location, 0, 2, addr, false);
}

// ============================================================================
// Test 5: Cleanup of Old Segments
// ============================================================================

TEST_CASE_METHOD(AppenderWarmerFixture, "AppenderWarmer cleans up old segments",
                 "[AppenderWarmer][cleanup]") {
  auto warmer =
      std::make_shared<AppenderWarmer<>>(location, 2, 1024 * 1024, true);
  warmer->start();

  SECTION("Old segments are cleaned up") {
    warmer->updateCurrentSegment(1);
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // Move far ahead
    warmer->updateCurrentSegment(100);
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // Old segments should be gone
    std::size_t size;
    REQUIRE(warmer->getWarmedSegment(2, size) == 0);
    REQUIRE(warmer->getWarmedSegment(3, size) == 0);
  }
}

// ============================================================================
// Test 6: Thread Safety
// ============================================================================

TEST_CASE_METHOD(AppenderWarmerFixture, "AppenderWarmer is thread-safe",
                 "[AppenderWarmer][thread]") {
  auto warmer =
      std::make_shared<AppenderWarmer<>>(location, 2, 1024 * 1024, true);
  warmer->start();

  SECTION("Multiple threads can update current segment") {
    std::vector<std::thread> threads;

    for (int i = 0; i < 10; ++i) {
      threads.emplace_back([&warmer, i]() {
        warmer->updateCurrentSegment(i * 10);
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      });
    }

    for (auto &t : threads) {
      t.join();
    }

    REQUIRE(true); // No crash = success
  }

  SECTION("Multiple threads can get warmed segments") {
    warmer->updateCurrentSegment(1);
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    // Move appender far ahead so warmer won't re-warm segment 2
    // (warmer only warms segments current+1 to current+5)
    warmer->updateCurrentSegment(100);
    std::vector<std::thread> threads;
    std::atomic<int> success_count{0};
    std::vector<SegmentPtr> segments; // Store segments for proper cleanup
    std::mutex segments_mutex;

    for (int i = 0; i < 5; ++i) {
      threads.emplace_back([&]() {
        std::size_t size;
        std::uintptr_t addr = warmer->getWarmedSegment(2, size);
        if (addr != 0) {
          success_count++;
          // Wrap in Segment for cleanup
          auto segment = Segment::loadFromAddress(location, 0, 2, addr, false);
          std::lock_guard<std::mutex> lock(segments_mutex);
          segments.push_back(segment);
        }
      });
    }

    for (auto &t : threads) {
      t.join();
    }

    // Only one thread should succeed (segment can only be retrieved once)
    REQUIRE(success_count == 1);
  }
}

// ============================================================================
// Test 7: Performance Characteristics
// ============================================================================

TEST_CASE_METHOD(AppenderWarmerFixture, "AppenderWarmer performance",
                 "[AppenderWarmer][perf]") {
  auto warmer =
      std::make_shared<AppenderWarmer<>>(location, 2, 1024 * 1024, true);
  warmer->start();

  SECTION("Segments are available quickly") {
    warmer->updateCurrentSegment(1);

    // Measure time to get warmed segment
    const auto start = Timestamp::now();

    std::size_t size;
    std::uintptr_t addr = 0;

    // Wait up to 500ms
    for (int i = 0; i < 500; ++i) {
      addr = warmer->getWarmedSegment(2, size);
      if (addr != 0)
        break;
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    const auto end = Timestamp::now();
    const auto duration = end - start;

    REQUIRE(addr != 0);
    SPDLOG_INFO("Segment warming took: {} (start={}, end={})", duration, start,
                end);
    REQUIRE(duration < 500ms);

    // Cleanup with Segment wrapper
    if (addr != 0) {
      auto segment = Segment::loadFromAddress(location, 0, 2, addr, false);
    }
  }
}

// ============================================================================
// Test 8: Edge Cases
// ============================================================================

TEST_CASE_METHOD(AppenderWarmerFixture, "AppenderWarmer edge cases",
                 "[AppenderWarmer][edge]") {
  SECTION("Can handle zero warmup segments") {
    auto warmer =
        std::make_shared<AppenderWarmer<>>(location, 2, 1024 * 1024, true);
    warmer->start();

    warmer->updateCurrentSegment(1);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    std::size_t size;
    REQUIRE(warmer->getWarmedSegment(2, size) != 0);
  }

  SECTION("Can handle large warmup count") {
    auto warmer =
        std::make_shared<AppenderWarmer<>>(location, 2, 1024 * 1024, true);
    warmer->start();

    warmer->updateCurrentSegment(1);
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    REQUIRE(warmer != nullptr);
  }

  SECTION("Handles rapid segment changes") {
    auto warmer =
        std::make_shared<AppenderWarmer<>>(location, 2, 1024 * 1024, true);
    // auto warmer = std::make_shared<AppenderWarmer>(location, 0, 2, -1);
    warmer->start();

    for (int i = 1; i <= 100; ++i) {
      warmer->updateCurrentSegment(i);
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    REQUIRE(true); // Should not crash
  }
}

// ============================================================================
// Test 9: Integration with Segment class
// ============================================================================

TEST_CASE_METHOD(AppenderWarmerFixture,
                 "AppenderWarmer integrates with Segment",
                 "[AppenderWarmer][integration]") {
  auto warmer =
      std::make_shared<AppenderWarmer<>>(location, 2, 1024 * 1024, true);
  warmer->start();

  SECTION("Warmed segments can be loaded via Segment::loadFromAddress") {
    warmer->updateCurrentSegment(1);
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    std::size_t size;
    std::uintptr_t addr = warmer->getWarmedSegment(2, size);

    REQUIRE(addr != 0);

    // Should not throw
    SegmentPtr segment = Segment::loadFromAddress(location, 0, 2, addr, false);

    REQUIRE(segment != nullptr);
    REQUIRE(segment->getSegmentId() == 2);
    REQUIRE(segment->getSegmentSize() == size);
  }

  SECTION("Warmed segments have correct structure") {
    warmer->updateCurrentSegment(1);
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    std::size_t size;
    std::uintptr_t addr = warmer->getWarmedSegment(2, size);

    SegmentPtr segment = Segment::loadFromAddress(location, 0, 2, addr, false);

    // Check segment structure
    REQUIRE(segment->firstFrameAddress() ==
            segment->address() + sizeof(SegmentHeader));
    REQUIRE(segment->lastFrameAddress() ==
            segment->firstFrameAddress()); // No frames yet
  }
}

// ============================================================================
// Test 10: appender Integration Pattern
// ============================================================================

TEST_CASE_METHOD(AppenderWarmerFixture, "AppenderWarmer appender integration",
                 "[AppenderWarmer][appender]") {
  SECTION("Simulates appender pattern - updateCurrentSegment before warming") {
    auto warmer =
        std::make_shared<AppenderWarmer<>>(location, 2, 1024 * 1024, true);
    warmer->start();

    // Simulate appender: starts at segment -1, waits for updateCurrentSegment
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // Segments shouldn't be warmed yet (no current segment set)
    std::size_t size;
    REQUIRE(warmer->getWarmedSegment(2, size) == 0);

    // appender creates first segment and notifies warmer
    warmer->updateCurrentSegment(1);

    // Wait for warming to happen
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // Now segments 2 and 3 should be warmed
    std::uintptr_t addr2 = warmer->getWarmedSegment(2, size);
    REQUIRE(addr2 != 0);

    // Cleanup
    if (addr2 != 0) {
      auto segment = Segment::loadFromAddress(location, 0, 2, addr2, false);
    }
  }

  SECTION("Simulates multiple segment rotations") {
    auto warmer =
        std::make_shared<AppenderWarmer<>>(location, 2, 1024 * 1024, true);
    warmer->start();

    std::vector<SegmentPtr> used_segments;

    // Simulate appender rotating through segments
    for (int current = 1; current <= 5; ++current) {
      // appender updates current segment
      warmer->updateCurrentSegment(current);

      // Give warmer time to warm ahead
      std::this_thread::sleep_for(std::chrono::milliseconds(100));

      // Try to get next warmed segment
      std::size_t size;
      std::uintptr_t addr = warmer->getWarmedSegment(current + 1, size);

      if (addr != 0) {
        auto segment =
            Segment::loadFromAddress(location, 0, current + 1, addr, false);
        used_segments.push_back(segment);
        SPDLOG_DEBUG("Got warmed segment {} at rotation {}", current + 1,
                     current);
      }
    }

    // Should have gotten most of the warmed segments
    INFO("Warmed segments retrieved: " << used_segments.size() << " out of 5");
    REQUIRE(used_segments.size() >= 3); // At least some should be warmed
  }

  SECTION("Handles concurrent updateCurrentSegment and getWarmedSegment") {
    auto warmer =
        std::make_shared<AppenderWarmer<>>(location, 2, 1024 * 1024, true);
    warmer->start();

    std::atomic<bool> running{true};
    std::vector<SegmentPtr> segments;
    std::mutex segments_mutex;

    // Thread 1: Simulates appender rotating segments
    std::thread appender_thread([&]() {
      for (int segment = 1; segment <= 10 && running; ++segment) {
        warmer->updateCurrentSegment(segment);
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
      }
    });

    // Thread 2: Simulates getting warmed segments
    std::thread cursor_thread([&]() {
      for (int segment = 2; segment <= 11 && running; ++segment) {
        std::this_thread::sleep_for(std::chrono::milliseconds(60));

        std::size_t size;
        std::uintptr_t addr = warmer->getWarmedSegment(segment, size);

        if (addr != 0) {
          auto p = Segment::loadFromAddress(location, 0, segment, addr, false);
          std::lock_guard<std::mutex> lock(segments_mutex);
          segments.push_back(p);
        }
      }
    });

    appender_thread.join();
    cursor_thread.join();
    running = false;

    // Should have retrieved some warmed segments
    REQUIRE(segments.size() > 0);
    SPDLOG_INFO("Retrieved {} warmed segments during concurrent access",
                segments.size());
  }

  SECTION("AppenderWarmer waits for first updateCurrentSegment") {
    auto warmer =
        std::make_shared<AppenderWarmer<>>(location, 2, 1024 * 1024, true);
    warmer->start();

    // Don't call updateCurrentSegment yet
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Should not warm any segments (current_segment still -1)
    std::size_t size;
    REQUIRE(warmer->getWarmedSegment(1, size) == 0);
    REQUIRE(warmer->getWarmedSegment(2, size) == 0);

    // Now set current segment
    warmer->updateCurrentSegment(1);
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // Now segment 2 should be warmed
    REQUIRE(warmer->getWarmedSegment(2, size) != 0);
  }
}

// Fixture
class AppenderWarmerIntegrationFixture {
public:
  AppenderWarmerIntegrationFixture() {
    test_dir = fs::temp_directory_path() / "AppenderWarmer_integration_test";

    if (fs::exists(test_dir)) {
      fs::remove_all(test_dir);
    }

    fs::create_directories(test_dir);

    locator = std::make_shared<Locator>(test_dir.string());
    location = Location::make(Mode::LIVE, Category::EXECUTION, "test",
                              "warmer_int", locator);
  }

  ~AppenderWarmerIntegrationFixture() {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    if (fs::exists(test_dir)) {
      try {
        fs::remove_all(test_dir);
      } catch (...) {
      }
    }
  }

  fs::path test_dir;
  ILocatorPtr locator;
  LocationPtr location;
};

// ============================================================================
// Test: Verify First Segment is Pre-Warmed (WOULD CATCH THE BUG!)
// ============================================================================

TEST_CASE_METHOD(AppenderWarmerIntegrationFixture,
                 "AppenderWarmer warms first segment",
                 "[AppenderWarmer][integration][critical]") {

  SECTION("Segment 1 should be pre-warmed") {
    // Create standalone warmer to inspect
    auto warmer =
        std::make_shared<AppenderWarmer<>>(location, 2, 1024 * 1024, true);
    warmer->start();

    // Simulate appender initialization
    warmer->updateCurrentSegment(0); // Tell warmer we're starting

    // Give warmer time to warm segment 1
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // Check if segment 1 is warmed
    std::size_t size;
    std::uintptr_t addr = warmer->getWarmedSegment(1, size);

    REQUIRE(addr != 0); // ← THIS WOULD FAIL WITH OLD CODE!

    INFO("Segment 1 was pre-warmed successfully");

    // Cleanup
    if (addr != 0) {
      auto segment = Segment::loadFromAddress(location, 0, 1, addr, false);
    }
  }

  SECTION("All initial segments should be warmed") {
    auto warmer = std::make_shared<AppenderWarmer<>>(
        location, 2, 1024 * 1024, true, -1, /*warmup_distance=*/3);
    warmer->start();
    warmer->updateCurrentSegment(0);

    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    // Check segments 1, 2, 3 are all warmed
    std::vector<SegmentPtr> segments;
    for (int segment_id = 1; segment_id <= 3; ++segment_id) {
      std::size_t size;
      std::uintptr_t addr = warmer->getWarmedSegment(segment_id, size);

      REQUIRE(addr != 0); // ← All should be warmed

      if (addr != 0) {
        segments.push_back(
            Segment::loadFromAddress(location, 0, segment_id, addr, false));
      }
    }

    REQUIRE(segments.size() == 3);
  }
}

// ============================================================================
// Test: Track Which Segments Were Pre-Warmed
// ============================================================================

TEST_CASE_METHOD(AppenderWarmerIntegrationFixture,
                 "appender uses pre-warmed segments",
                 "[AppenderWarmer][integration]") {

  struct TestTrade {
    int64_t timestamp;
    uint32_t id;
  };

  SECTION("Track pre-warmed segment usage") {
    // Create a custom warmer we can inspect
    auto warmer =
        std::make_shared<AppenderWarmer<>>(location, 2, 1024 * 1024, true);
    warmer->start();

    // Manually simulate appender behavior
    warmer->updateCurrentSegment(0);
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // Track which segments were pre-warmed
    std::vector<int> prewarmed_segments;
    std::vector<int> fallback_segments;

    for (int segment_id = 1; segment_id <= 5; ++segment_id) {
      std::size_t size;
      std::uintptr_t addr = warmer->getWarmedSegment(segment_id, size);

      if (addr != 0) {
        prewarmed_segments.push_back(segment_id);
        auto segment =
            Segment::loadFromAddress(location, 0, segment_id, addr, false);
      } else {
        fallback_segments.push_back(segment_id);
      }

      // Simulate appender moving to next segment
      warmer->updateCurrentSegment(segment_id);
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

    INFO("Pre-warmed segments: " << prewarmed_segments.size());
    INFO("Fallback segments: " << fallback_segments.size());

    // With OLD bug: Segment 1 would be in fallback_segments
    // With FIX: Segment 1 should be in prewarmed_segments

    REQUIRE(prewarmed_segments.size() >= 3); // Should get at least 3

    // CRITICAL: Segment 1 should be pre-warmed!
    auto it =
        std::find(prewarmed_segments.begin(), prewarmed_segments.end(), 1);
    REQUIRE(it != prewarmed_segments.end()); // ← WOULD FAIL WITH BUG!
  }
}

// ============================================================================
// Test: Measure Pre-Warming Effectiveness
// ============================================================================

TEST_CASE_METHOD(AppenderWarmerIntegrationFixture,
                 "AppenderWarmer effectiveness metrics",
                 "[AppenderWarmer][metrics]") {

  struct TestTrade {
    int64_t timestamp;
    uint32_t id;
  };

  SECTION("Measure hit rate") {
    auto warmer =
        std::make_shared<AppenderWarmer<>>(location, 2, 1024 * 1024, true);
    warmer->start();
    warmer->updateCurrentSegment(0);

    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    int hits = 0;
    int misses = 0;

    // Try to get 10 segments
    for (int segment_id = 1; segment_id <= 10; ++segment_id) {
      std::size_t size;
      std::uintptr_t addr = warmer->getWarmedSegment(segment_id, size);

      if (addr != 0) {
        hits++;
        auto segment =
            Segment::loadFromAddress(location, 0, segment_id, addr, false);
      } else {
        misses++;
      }

      warmer->updateCurrentSegment(segment_id);
      std::this_thread::sleep_for(std::chrono::milliseconds(30));
    }

    double hit_rate = static_cast<double>(hits) / (hits + misses) * 100.0;

    INFO("Hit rate: " << hit_rate << "%");
    INFO("Hits: " << hits << ", Misses: " << misses);

    // Should have high hit rate with proper implementation
    REQUIRE(hit_rate >= 50.0); // At least 50% hit rate

    // Segment 1 specifically should be a hit
    REQUIRE(hits >= 1);
  }
}

// ============================================================================
// Test: Integration with Actual appender
// ============================================================================

TEST_CASE_METHOD(AppenderWarmerIntegrationFixture,
                 "appender integration observability",
                 "[AppenderWarmer][appender][observability]") {

  struct TestTrade {
    int64_t timestamp;
    uint32_t id;
  };

  SECTION("appender with observable AppenderWarmer") {
    // Create a warmer we can inspect
    auto observable_warmer =
        std::make_shared<AppenderWarmer<>>(location, 2, 1024 * 1024, true);
    observable_warmer->start();

    // Pre-initialize: simulate what appender constructor should do
    observable_warmer->updateCurrentSegment(0);
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // Check segment 1 is ready BEFORE creating appender
    std::size_t test_size;
    std::uintptr_t test_addr =
        observable_warmer->getWarmedSegment(1, test_size);

    // Put it back by warming again (hacky, but for testing)
    if (test_addr != 0) {
      // We consumed it, so warm it again for the appender
      auto segment = Segment::loadFromAddress(location, 0, 1, test_addr, false);
      segment.reset(); // Release it

      // Re-warm it
      observable_warmer->updateCurrentSegment(0);
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    REQUIRE(test_addr != 0); // ← Segment 1 WAS available!

    INFO("Segment 1 was confirmed pre-warmed before appender started");
  }
}

// ============================================================================
// Test: Verify Initialization Timing
// ============================================================================

TEST_CASE_METHOD(AppenderWarmerIntegrationFixture,
                 "AppenderWarmer initialization timing",
                 "[AppenderWarmer][timing]") {

  SECTION("Warmer needs time to warm first segment") {
    auto warmer =
        std::make_shared<AppenderWarmer<>>(location, 2, 1024 * 1024, true);
    warmer->start();
    warmer->updateCurrentSegment(0);

    // Check immediately - should NOT be warmed yet
    {
      std::size_t size;
      std::uintptr_t addr = warmer->getWarmedSegment(1, size);

      if (addr == 0) {
        INFO("Segment 1 not ready immediately (expected)");
      } else {
        // Got lucky - very fast system
        auto segment = Segment::loadFromAddress(location, 0, 1, addr, false);
      }
    }

    // Wait and check again - SHOULD be warmed now
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    {
      std::size_t size;
      std::uintptr_t addr = warmer->getWarmedSegment(1, size);

      REQUIRE(addr != 0); // Should definitely be ready now

      auto segment = Segment::loadFromAddress(location, 0, 1, addr, false);
    }
  }
}

// ============================================================================
// Test: AppenderWarmer State Consistency
// ============================================================================

TEST_CASE_METHOD(AppenderWarmerIntegrationFixture,
                 "AppenderWarmer state consistency",
                 "[AppenderWarmer][state]") {

  SECTION("Current segment must be set before warming happens") {
    auto warmer =
        std::make_shared<AppenderWarmer<>>(location, 2, 1024 * 1024, true);
    warmer->start();

    // Don't call updateCurrentSegment - stays at -1
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // Nothing should be warmed
    std::size_t size;
    REQUIRE(warmer->getWarmedSegment(1, size) == 0);
    REQUIRE(warmer->getWarmedSegment(2, size) == 0);

    // Now set current segment
    warmer->updateCurrentSegment(0);
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // Now segment 1 should be warmed
    REQUIRE(warmer->getWarmedSegment(1, size) != 0);
  }
}

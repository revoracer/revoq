// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 RevoRacer
#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <revoq/Appender.h>
#include <revoq/Frame.h>
#include <revoq/MergingCursor.h>
#include <revoq/RevoqError.h>
#include <revoq/StreamBase.h>
#include "test_helpers.h"

using namespace revoq;
using namespace revoq::test;
namespace fs = std::filesystem;

// Test data structure
struct TestData {
  int64_t value;
  double price;
  char symbol[8];
};

namespace {

class CountingLocator final : public Locator {
public:
  explicit CountingLocator(std::string home) : Locator(std::move(home)) {}

  [[nodiscard]] std::vector<int>
  listSegmentId(LocationPtr location, std::uint32_t dest_id) const override {
    ++list_segment_id_calls;
    return Locator::listSegmentId(std::move(location), dest_id);
  }

  mutable int list_segment_id_calls{0};
};

} // namespace

TEST_CASE("Appender basic write and read", "[stream][appender][basic]") {
  TempTestDir temp_dir;
  auto location = createTestLocation(temp_dir.string());
  uint32_t dest_id = 0x12345678;

  SECTION("Write some frames and verify") {
    // Create appender without AppenderWarmer for simplicity
    auto appender = std::make_shared<Appender<>>(
        location, dest_id, AppenderOptions{.prefault = true});

    // Write some test data
    TestData data1{100, 123.45, "AAPL"};
    TestData data2{200, 234.56, "GOOGL"};
    TestData data3{300, 345.67, "MSFT"};

    publish(*appender, 1000000, kTagA, data1);
    publish(*appender, 2000000, kTagA, data2);
    publish(*appender, 3000000, kTagA, data3);

    REQUIRE(appender->getCurrentSequence() == 3);
    REQUIRE(appender->getCurrentSegmentId() == 1);
  }
}

TEST_CASE("Appender resume after restart", "[stream][appender][resume]") {
  TempTestDir temp_dir;
  auto location = createTestLocation(temp_dir.string());
  uint32_t dest_id = 0x12345678;

  uint32_t last_seq_before_restart = 0;
  int64_t last_segment_before_restart = 0;

  SECTION("First run - write initial data") {
    auto appender = std::make_shared<Appender<>>(
        location, dest_id, AppenderOptions{.prefault = false});

    // Write 100 frames
    for (int i = 0; i < 100; ++i) {
      TestData data{i, static_cast<double>(i) * 1.5, "TEST"};
      publish(*appender, static_cast<std::int64_t>(i) * 1000000, kTagA,
              data);
    }

    last_seq_before_restart = appender->getCurrentSequence();
    last_segment_before_restart = appender->getCurrentSegmentId();

    INFO("First run: last_seq=" << last_seq_before_restart << ", last_segment="
                                << last_segment_before_restart);

    REQUIRE(last_seq_before_restart == 100);

    // Destroy appender (simulates shutdown)
    appender.reset();
  }

  SECTION("Second run - resume and continue") {
    // Create new appender with resume enabled
    auto appender = std::make_shared<Appender<>>(
        location, dest_id, AppenderOptions{.prefault = false});

    // Should have resumed from previous state
    uint32_t resumed_seq = appender->getCurrentSequence();
    int64_t resumed_segment = appender->getCurrentSegmentId();

    INFO("Second run: resumed_seq=" << resumed_seq
                                    << ", resumed_segment=" << resumed_segment);

    // Sequence should continue from where we left off
    REQUIRE(resumed_seq == last_seq_before_restart);

    // Write more data
    for (int i = 0; i < 50; ++i) {
      TestData data{i + 1000, static_cast<double>(i + 1000) * 1.5, "TST2"};
      publish(*appender, (i + 1000) * 1000000, kTagA, data);
    }

    // Should have continued sequence numbering
    REQUIRE(appender->getCurrentSequence() == last_seq_before_restart + 50);
  }
}

TEST_CASE("Appender resume with multiple segments",
          "[stream][appender][resume][multisegment]") {
  TempTestDir temp_dir;
  auto location = createTestLocation(temp_dir.string());
  uint32_t dest_id = 0x12345678;

  const int FRAMES_PER_TEST =
      1'000'000; // Should trigger multiple segment rotations

  SECTION("Write many frames to trigger segment rotation") {
    auto appender = std::make_shared<Appender<>>(
        location, dest_id, AppenderOptions{.prefault = false});
    for (int i = 0; i < FRAMES_PER_TEST; ++i) {
      TestData data{i, static_cast<double>(i), "SYMB"};
      publish(*appender, i * 1'000'000, kTagA, data);
    }

    int64_t final_segment = appender->getCurrentSegmentId();
    // This is the currently available seq number for next write
    uint64_t final_seq = appender->getCurrentSequence();
    const size_t frame_size =
        framePhysicalLength(sizeof(FrameHeader) + sizeof(TestData));
    const size_t frames_per_segment =
        (findSegmentSize(location, dest_id) - sizeof(SegmentHeader) -
         FRAME_ALIGNMENT) /
        frame_size;
    const int64_t expected_segments = static_cast<int64_t>(
        (FRAMES_PER_TEST + frames_per_segment - 1) / frames_per_segment);

    INFO("Wrote " << FRAMES_PER_TEST << " frames across " << final_segment
                  << " segments, final sequence: " << final_seq);

    REQUIRE(final_segment >=
            expected_segments); // Should have rotated to multiple segments
    REQUIRE(final_seq >= FRAMES_PER_TEST);

    appender.reset();

    // Resume and verify
    auto appender2 = std::make_shared<Appender<>>(
        location, dest_id, AppenderOptions{.prefault = false});

    REQUIRE(appender2->getCurrentSequence() == final_seq);
    REQUIRE(appender2->getCurrentSegmentId() == final_segment);
  }
}

TEST_CASE("Appender resume from empty stream",
          "[stream][appender][resume][empty]") {
  TempTestDir temp_dir;
  auto location = createTestLocation(temp_dir.string());
  uint32_t dest_id = 0x12345678;

  // Create appender with resume=true on empty directory
  auto appender = std::make_shared<Appender<>>(
      location, dest_id, AppenderOptions{.prefault = false});

  // Should start from segment 1, sequence 0
  REQUIRE(appender->getCurrentSegmentId() == 1);
  REQUIRE(appender->getCurrentSequence() == 0);

  // Write some data
  TestData data{42, 99.99, "TEST"};
  publish(*appender, 1000000, kTagA, data);

  REQUIRE(appender->getCurrentSequence() == 1);
}

TEST_CASE("Appender with AppenderWarmer resume",
          "[stream][appender][resume][warmer]") {
  TempTestDir temp_dir;
  auto location = createTestLocation(temp_dir.string());
  uint32_t dest_id = 0x12345678;

  SECTION("First run with AppenderWarmer") {
    auto appender = std::make_shared<Appender<>>(location, dest_id,
                                                 AppenderOptions{
                                                     .prefault = false,
                                                     .warmup_segments = 4,
                                                     .warmer_cpu_core = -1,
                                                 });

    // Write data
    for (int i = 0; i < 1005; ++i) {
      TestData data{i, static_cast<double>(i), "WARM"};
      publish(*appender, static_cast<std::int64_t>(i) * 1000000, kTagA,
              data);
    }

    uint32_t final_seq = appender->getCurrentSequence();
    int64_t final_segment = appender->getCurrentSegmentId();

    SPDLOG_INFO("Before destruction: final_seq={}, final_segment={}", final_seq,
                final_segment);
    appender.reset();

    // Give time for AppenderWarmer cleanup
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Resume with AppenderWarmer
    auto appender2 = std::make_shared<Appender<>>(location, dest_id,
                                                  AppenderOptions{
                                                      .prefault = false,
                                                      .warmup_segments = 4,
                                                      .warmer_cpu_core = -1,
                                                  });
    SPDLOG_INFO("After resume: seq={}, segment={}",
                appender2->getCurrentSequence(),
                appender2->getCurrentSegmentId());
    REQUIRE(appender2->getCurrentSequence() == final_seq);

    // Write more data with warmer active
    for (int i = 0; i < 500; ++i) {
      TestData data{i + 2000, static_cast<double>(i + 2000), "WRM2"};
      publish(*appender2, (i + 2000) * 1000000, kTagA, data);
    }

    REQUIRE(appender2->getCurrentSequence() == final_seq + 500);
  }
}

TEST_CASE("Appender resume vs no-resume behavior",
          "[stream][appender][resume][comparison]") {
  TempTestDir temp_dir;
  auto location = createTestLocation(temp_dir.string());
  uint32_t dest_id = 0x12345678;

  SECTION("Setup: Write initial data") {
    auto appender = std::make_shared<Appender<>>(
        location, dest_id, AppenderOptions{.prefault = false});

    for (int i = 0; i < 100; ++i) {
      TestData data{i, static_cast<double>(i), "INIT"};
      publish(*appender, static_cast<std::int64_t>(i) * 1000000, kTagA,
              data);
    }

    appender.reset();
    //  }
    //
    //  SECTION("With resume=true, should continue") {
    appender = std::make_shared<Appender<>>(location, dest_id,
                                            AppenderOptions{.prefault = false});

    // Should have sequence 101 (continuing from 100)
    uint32_t seq_before = appender->getCurrentSequence();
    REQUIRE(seq_before >= 100);

    TestData data{999, 999.99, "CONT"};
    publish(*appender, 999000000, kTagA, data);

    REQUIRE(appender->getCurrentSequence() == seq_before + 1);
  }

  // The appender always resumes from existing segments now. A restart-from-zero
  // mode would need an explicit destructive/truncate API.
}

TEST_CASE("Appender sequence number continuity",
          "[stream][appender][sequence]") {
  TempTestDir temp_dir;
  auto location = createTestLocation(temp_dir.string());
  uint32_t dest_id = 0x12345678;

  std::vector<uint32_t> all_sequences;

  SECTION("Multiple restart cycles") {
    // Cycle 1
    {
      auto appender = std::make_shared<Appender<>>(
          location, dest_id, AppenderOptions{.prefault = false});

      for (int i = 0; i < 50; ++i) {
        TestData data{i, static_cast<double>(i), "CYC1"};
        publish(*appender, static_cast<std::int64_t>(i) * 1000000,
                kTagA, data);
        all_sequences.push_back(appender->getCurrentSequence() - 1);
      }
    }

    // Cycle 2
    {
      auto appender = std::make_shared<Appender<>>(
          location, dest_id, AppenderOptions{.prefault = false});

      for (int i = 0; i < 50; ++i) {
        TestData data{i + 100, static_cast<double>(i + 100), "CYC2"};
        publish(*appender, (i + 100) * 1000000, kTagA, data);
        all_sequences.push_back(appender->getCurrentSequence() - 1);
      }
    }

    // Cycle 3
    {
      auto appender = std::make_shared<Appender<>>(
          location, dest_id, AppenderOptions{.prefault = false});

      for (int i = 0; i < 50; ++i) {
        TestData data{i + 200, static_cast<double>(i + 200), "CYC3"};
        publish(*appender, (i + 200) * 1000000, kTagA, data);
        all_sequences.push_back(appender->getCurrentSequence() - 1);
      }
    }

    // Verify sequence continuity
    REQUIRE(all_sequences.size() == 150);

    for (size_t i = 0; i < all_sequences.size(); ++i) {
      INFO("Checking sequence at index " << i);
      REQUIRE(all_sequences[i] == i);
    }
  }
}

TEST_CASE("Appender marker frames with resume",
          "[stream][appender][markers][resume]") {
  TempTestDir temp_dir;
  auto location = createTestLocation(temp_dir.string());
  uint32_t dest_id = 0x12345678;

  SECTION("Write markers and data, then resume") {
    {
      auto appender = std::make_shared<Appender<>>(
          location, dest_id, AppenderOptions{.prefault = false});

      appender->mark(1000000, MsgType::SessionStart);

      for (int i = 0; i < 10; ++i) {
        TestData data{i, static_cast<double>(i), "MRK1"};
        publish(*appender, (i + 1) * 1000000, kTagA, data);
      }

      appender->mark(20000000, MsgType::SessionEnd);

      REQUIRE(appender->getCurrentSequence() ==
              12); // 1 start + 10 data + 1 end
    }

    {
      auto appender = std::make_shared<Appender<>>(
          location, dest_id, AppenderOptions{.prefault = false});

      REQUIRE(appender->getCurrentSequence() == 12); // Continue from 12

      appender->mark(30000000, MsgType::SessionStart);

      TestData data{999, 999.99, "MRK2"};
      publish(*appender, 31000000, kTagA, data);

      REQUIRE(appender->getCurrentSequence() == 14);
    }
  }
}

TEST_CASE("MergingCursor handles segment boundary edge cases",
          "[stream][cursor][segment-boundary]") {
  TempTestDir temp_dir;
  auto location = createTestLocation(temp_dir.string());
  uint32_t dest_id = 0x12345678;
  const auto segment1 = Segment::getSegmentPath(location, dest_id, 1);

  SECTION("Read-only load rejects zero-byte segment") {
    fs::create_directories(segment1.parent_path());
    std::ofstream(segment1.string()).close();

    REQUIRE_THROWS_AS(Segment::load(location, dest_id, 1, false, false),
                      RevoqError);
  }

  SECTION("Read-only load rejects full-size uninitialized segment") {
    fs::create_directories(segment1.parent_path());
    std::ofstream(segment1.string()).close();
    fs::resize_file(segment1, findSegmentSize(location, dest_id));

    REQUIRE_THROWS_AS(Segment::load(location, dest_id, 1, false, false),
                      RevoqError);
  }

  SECTION("Missing next segment after SegmentEnd is not created by cursor") {
    {
      auto appender = std::make_shared<Appender<>>(
          location, dest_id,
          AppenderOptions{.prefault = false, .background_threads = false});
      TestData data{1, 1.0, "MISS"};
      publish(*appender, 1'000'000, kTagA, data);
      appender->mark(2'000'000, MsgType::SegmentEnd);
    }

    const auto segment2 = Segment::getSegmentPath(location, dest_id, 2);
    REQUIRE_FALSE(fs::exists(segment2));

    MergingCursor cursor(
        MergingCursorOptions{.prefault = false, .background_threads = false});
    cursor.join(location, dest_id, 0);

    REQUIRE(cursor.isDataAvailable());
    cursor.next();
    REQUIRE(cursor.isDataAvailable());
    auto segment_end = cursor.tryCurrentFrame();
    REQUIRE(segment_end);
    REQUIRE(static_cast<MsgType>(segment_end.msg_type) == MsgType::SegmentEnd);
    REQUIRE_NOTHROW(cursor.next());
    REQUIRE_FALSE(cursor.isDataAvailable());
    REQUIRE_FALSE(fs::exists(segment2));
  }

  SECTION("Valid empty next segment after SegmentEnd is clean EOF") {
    {
      auto appender = std::make_shared<Appender<>>(
          location, dest_id,
          AppenderOptions{.prefault = false, .background_threads = false});
      TestData data{1, 1.0, "EMTY"};
      publish(*appender, 1'000'000, kTagA, data);
      appender->mark(2'000'000, MsgType::SegmentEnd);
    }
    Segment::load(location, dest_id, 2, true, false);

    MergingCursor cursor(
        MergingCursorOptions{.prefault = false, .background_threads = false});
    cursor.join(location, dest_id, 0);

    int count = 0;
    while (cursor.isDataAvailable()) {
      ++count;
      cursor.next();
    }
    REQUIRE(count == 2);
  }
}

TEST_CASE("MergingCursor segment rotation does not scan directory",
          "[stream][cursor][rotation]") {
  TempTestDir temp_dir;
  auto locator = std::make_shared<CountingLocator>(temp_dir.string());
  auto location = Location::make(Mode::LIVE, Category::EXECUTION, "test",
                                 "appender_test_resume", locator);
  constexpr uint32_t dest_id = 0x12345678;

  {
    auto appender = std::make_shared<Appender<>>(
        location, dest_id,
        AppenderOptions{.prefault = false, .background_threads = false});
    TestData data{1, 1.0, "FAST"};
    publish(*appender, 1'000'000, kTagA, data);
    appender->mark(2'000'000, MsgType::SegmentEnd);
  }
  Segment::load(location, dest_id, 2, true, false);

  MergingCursor cursor(
      MergingCursorOptions{.prefault = false, .background_threads = false});
  cursor.join(location, dest_id, 0);
  locator->list_segment_id_calls = 0;

  REQUIRE(cursor.isDataAvailable());
  cursor.next(); // trade
  REQUIRE(cursor.isDataAvailable());
  auto segment_end = cursor.tryCurrentFrame();
  REQUIRE(segment_end);
  REQUIRE(static_cast<MsgType>(segment_end.msg_type) == MsgType::SegmentEnd);

  cursor.next(); // load segment 2
  REQUIRE(locator->list_segment_id_calls == 0);
  REQUIRE_FALSE(cursor.isDataAvailable());
}

TEST_CASE("Appender resume appends to previous segment for cursors",
          "[stream][appender][resume][cursor]") {
  TempTestDir temp_dir;
  auto location = createTestLocation(temp_dir.string());
  uint32_t dest_id = 0x12345678;

  {
    auto appender = std::make_shared<Appender<>>(
        location, dest_id,
        AppenderOptions{.prefault = false, .background_threads = false});
    appender->mark(1'000'000, MsgType::SessionStart);
    TestData first{1, 1.0, "R1"};
    publish(*appender, 2'000'000, kTagA, first);
    appender->mark(3'000'000, MsgType::SessionEnd);
    REQUIRE(appender->getCurrentSequence() == 3);
  }

  {
    auto appender = std::make_shared<Appender<>>(
        location, dest_id,
        AppenderOptions{.prefault = false, .background_threads = false});
    REQUIRE(appender->getCurrentSequence() == 3);
    REQUIRE(appender->getCurrentSegmentId() == 1);
    appender->mark(4'000'000, MsgType::SessionStart);
    TestData second{2, 2.0, "R2"};
    publish(*appender, 5'000'000, kTagA, second);
    appender->mark(6'000'000, MsgType::SessionEnd);
    REQUIRE(appender->getCurrentSequence() == 6);
  }

  MergingCursor cursor(
      MergingCursorOptions{.prefault = false, .background_threads = false});
  cursor.join(location, dest_id, 0);

  uint64_t expected_seq = 0;
  int starts = 0;
  int trades = 0;
  int ends = 0;
  int segment_ends = 0;
  while (cursor.isDataAvailable()) {
    auto frame = cursor.tryCurrentFrame();
    REQUIRE(frame);
    REQUIRE(frame->getSequence() == expected_seq++);
    switch (static_cast<MsgType>(frame.msg_type)) {
    case MsgType::SessionStart:
      ++starts;
      break;
    case kTagA:
      ++trades;
      break;
    case MsgType::SessionEnd:
      ++ends;
      break;
    case MsgType::SegmentEnd:
      ++segment_ends;
      break;
    default:
      break;
    }
    cursor.next();
  }

  REQUIRE(expected_seq == 6);
  REQUIRE(starts == 2);
  REQUIRE(trades == 2);
  REQUIRE(ends == 2);
  REQUIRE(segment_ends == 0);
}

TEST_CASE("Appender resumes after SegmentEnd on next segment",
          "[stream][appender][resume][segment-end]") {
  TempTestDir temp_dir;
  auto location = createTestLocation(temp_dir.string());
  uint32_t dest_id = 0x12345678;

  {
    auto appender = std::make_shared<Appender<>>(
        location, dest_id,
        AppenderOptions{.prefault = false, .background_threads = false});
    TestData data{1, 1.0, "PEND"};
    publish(*appender, 1'000'000, kTagA, data);
    appender->mark(2'000'000, MsgType::SegmentEnd);
    REQUIRE(appender->getCurrentSequence() == 2);
    REQUIRE(appender->getCurrentSegmentId() == 1);
  }

  auto appender = std::make_shared<Appender<>>(
      location, dest_id,
      AppenderOptions{.prefault = false, .background_threads = false});
  REQUIRE(appender->getCurrentSequence() == 2);
  REQUIRE(appender->getCurrentSegmentId() == 2);
}

TEST_CASE("Appender edge cases", "[stream][appender][edge]") {
  TempTestDir temp_dir;
  auto location = createTestLocation(temp_dir.string());
  uint32_t dest_id = 0x12345678;

  SECTION("Resume with corrupted last segment") {
    // Write some data
    {
      auto appender = std::make_shared<Appender<>>(
          location, dest_id, AppenderOptions{.prefault = false});

      for (int i = 0; i < 100; ++i) {
        TestData data{i, static_cast<double>(i), "EDGE"};
        publish(*appender, static_cast<std::int64_t>(i) * 1000000,
                kTagA, data);
      }
    }

    // TODO: Corrupt last segment file here if we want to test recovery

    // Try to resume - should handle gracefully
    REQUIRE_NOTHROW(std::make_shared<Appender<>>(
        location, dest_id, AppenderOptions{.prefault = false}));
  }

  SECTION("Crash mid-frame: incomplete frame does not corrupt resume") {
    // Simulate a appender crash after openData() but before closeData().
    // The frame slot has header fields set but length == 0 (not published).
    // Resume stops at the length-0 slot and re-uses that sequence number for
    // the next committed frame in the same segment.
    constexpr int N = 10;

    {
      auto appender = std::make_shared<Appender<>>(
          location, dest_id,
          AppenderOptions{.prefault = false, .background_threads = false});
      for (int i = 0; i < N; ++i) {
        TestData data{i, static_cast<double>(i), "CRSH"};
        publish(*appender, i * 1'000'000, kTagA, data);
      }
      REQUIRE(static_cast<int>(appender->getCurrentSequence()) == N);

      // Open a frame but do NOT close it — simulates crash mid-publish.
      // openData() calls nextSeq() so sequence N is consumed but length stays
      // 0.
      auto &slot = appender->openData<TestData>(N * 1'000'000, kTagA);
      (void)slot;
      // appender goes out of scope here without closeData() — crash simulated.
    }

    // Resumed appender must not crash and must pick up the correct sequence.
    // Resume scans to the length-0 slot and overwrites that unpublished slot
    // with the next committed frame at sequence N.
    auto appender2 = std::make_shared<Appender<>>(
        location, dest_id,
        AppenderOptions{.prefault = false, .background_threads = false});
    REQUIRE(static_cast<int>(appender2->getCurrentSequence()) == N);
    REQUIRE(appender2->getCurrentSegmentId() == 1);

    // Write one frame to confirm the resumed appender is functional.
    TestData cont{999, 9.9, "RES"};
    publish(*appender2, (N + 1) * 1'000'000, kTagA, cont);
    REQUIRE(static_cast<int>(appender2->getCurrentSequence()) == N + 1);

    // cursor joined at segment 1 sees N committed frames and the continued
    // frame.
    appender2.reset();
    auto cursor = std::make_shared<MergingCursor>(
        MergingCursorOptions{.prefault = true, .background_threads = false});
    cursor->join(location, dest_id, 0);
    int count = 0;
    while (cursor->isDataAvailable()) {
      ++count;
      cursor->next();
    }
    REQUIRE(count == N + 1);
  }

  SECTION("Very large sequence numbers") {
    auto appender = std::make_shared<Appender<>>(
        location, dest_id, AppenderOptions{.prefault = false});

    // Simulate very large sequence by writing many frames
    for (int i = 0; i < 1000; ++i) {
      TestData data{i, static_cast<double>(i), "BIG"};
      publish(*appender, static_cast<std::int64_t>(i) * 1000000, kTagA,
              data);
    }

    uint32_t large_seq = appender->getCurrentSequence();
    REQUIRE(large_seq >= 1000);

    appender.reset();

    // Resume should handle large sequences
    auto appender2 = std::make_shared<Appender<>>(
        location, dest_id, AppenderOptions{.prefault = false});

    REQUIRE(appender2->getCurrentSequence() == large_seq);
  }
}

TEST_CASE("Appender concurrent safety", "[stream][appender][concurrent]") {
  TempTestDir temp_dir;
  auto location = createTestLocation(temp_dir.string());
  uint32_t dest_id = 0x12345678;

  SECTION("Multiple writes in sequence") {
    auto appender = std::make_shared<Appender<>>(
        location, dest_id, AppenderOptions{.prefault = false});

    const int NUM_WRITES = 1000;

    for (int i = 0; i < NUM_WRITES; ++i) {
      TestData data{i, static_cast<double>(i), "CONC"};
      publish(*appender, static_cast<std::int64_t>(i) * 1000000, kTagA,
              data);
    }

    REQUIRE(appender->getCurrentSequence() == NUM_WRITES);
  }
}

TEST_CASE("Appender performance baseline", "[stream][appender][performance]") {
  TempTestDir temp_dir;
  auto location = createTestLocation(temp_dir.string());
  uint32_t dest_id = 0x12345678;

  SECTION("Write performance without warmer") {
    auto appender = std::make_shared<Appender<>>(
        location, dest_id, AppenderOptions{.prefault = false});

    const int NUM_WRITES = 10000;
    auto start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < NUM_WRITES; ++i) {
      TestData data{i, static_cast<double>(i) * 1.5, "PERF"};
      publish(*appender, static_cast<std::int64_t>(i) * 1000000, kTagA,
              data);
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration =
        std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);

    double avg_ns = static_cast<double>(duration.count()) / NUM_WRITES;

    INFO("Average write time: " << avg_ns << " ns");
    INFO("Total writes: " << NUM_WRITES);
    INFO("Total time: " << duration.count() / 1000000.0 << " ms");

    // Performance should be reasonable (adjust threshold as needed)
    REQUIRE(avg_ns < 10000); // Less than 10 microseconds per write
  }

  SECTION("Write performance with warmer") {
    auto appender = std::make_shared<Appender<>>(location, dest_id,
                                                 AppenderOptions{
                                                     .prefault = false,
                                                     .warmup_segments = 4,
                                                     .warmer_cpu_core = -1,
                                                 });

    const int NUM_WRITES = 10000;
    auto start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < NUM_WRITES; ++i) {
      TestData data{i, static_cast<double>(i) * 1.5, "PWRM"};
      publish(*appender, static_cast<std::int64_t>(i) * 1000000, kTagA,
              data);
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration =
        std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);

    double avg_ns = static_cast<double>(duration.count()) / NUM_WRITES;

    INFO("Average write time with warmer: " << avg_ns << " ns");
    INFO("Total writes: " << NUM_WRITES);
    INFO("Total time: " << duration.count() / 1000000.0 << " ms");

    REQUIRE(avg_ns < 10000);
  }
}

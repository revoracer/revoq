// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 RevoRacer
// Direct coverage for CursorWarmer, extracted from Stream. The full
// warm->claim handoff is also exercised indirectly by the MergingCursor tests;
// these focus on the pieces the extraction owns directly: the ready-segment
// claim, lifecycle (idempotent start, safe repeated stop), and — the behavior
// that actually changed in the extraction — stop() preempting an in-flight warm
// via the warmer's own running_ flag rather than Stream's retire_running_.

#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>
#include <chrono>
#include <cstdint>
#include <thread>

#include <revoq/CursorWarmer.h>
#include "test_helpers.h"

using namespace revoq;
using namespace revoq::test;
using namespace std::chrono_literals;

namespace {

// Owns a temp dir + location so each case only spells out its distinct
// behavior. Segment files are created on demand via writeFillerSegment().
struct WarmerFixture {
  TempTestDir dir;
  LocationPtr location = createTestLocation(dir.string());
};

// Poll the warmer for segment_id until it hands one back or the deadline
// passes.
SegmentPtr claimWithin(CursorWarmer &warmer, int segment_id,
                       std::chrono::milliseconds budget) {
  const auto deadline = std::chrono::steady_clock::now() + budget;
  while (std::chrono::steady_clock::now() < deadline) {
    if (SegmentPtr p = warmer.tryLoadWarmedSegment(segment_id))
      return p;
    std::this_thread::sleep_for(2ms);
  }
  return nullptr;
}

} // namespace

TEST_CASE_METHOD(WarmerFixture,
                 "CursorWarmer warms and hands off a ready segment",
                 "[cursor][warmer]") {
  const uint32_t dest_id = 0x1001;
  writeFillerSegment(location, dest_id);

  CursorWarmer warmer(location, dest_id, findSegmentSize(location, dest_id),
                      /*warmer_timeout_ms=*/2000);
  warmer.start(-1);
  warmer.signalNext(1);

  SegmentPtr segment = claimWithin(warmer, 1, 5s);
  REQUIRE(segment != nullptr);
  REQUIRE(segment->getSegmentId() == 1);

  // A second claim for the same segment misses: the slot was consumed.
  REQUIRE(warmer.tryLoadWarmedSegment(1) == nullptr);

#ifdef REVOQ_ENABLE_DIAGNOSTICS
  const auto stats = warmer.getStats();
  REQUIRE(stats.warmer_ready >= 1);
  REQUIRE(stats.warmer_hits == 1);
#endif

  warmer.stop();
}

#ifdef __linux__
TEST_CASE_METHOD(WarmerFixture, "CursorWarmer locks warmed segments on request",
                 "[cursor][warmer]") {
  const uint32_t dest_id = 0x1001;
  writeFillerSegment(location, dest_id);
  const uint32_t segment_size = findSegmentSize(location, dest_id);
  const bool lock_pages = GENERATE(false, true);

  const std::size_t before = lockedKb();
  CursorWarmer warmer(location, dest_id, segment_size,
                      /*warmer_timeout_ms=*/2000, lock_pages);
  warmer.start(-1);
  warmer.signalNext(1);

  SegmentPtr segment = claimWithin(warmer, 1, 5s);
  REQUIRE(segment != nullptr);
  if (lock_pages)
    REQUIRE(lockedKb() >= before + segment_size / 1024);
  else
    REQUIRE(lockedKb() == before);

  // Releasing the segment drops the lock.
  segment.reset();
  REQUIRE(lockedKb() == before);
  warmer.stop();
}
#endif

TEST_CASE_METHOD(WarmerFixture, "CursorWarmer stop() preempts a pending warm",
                 "[cursor][warmer]") {
  const uint32_t dest_id = 0x2002;
  writeFillerSegment(location,
                     dest_id); // real segment size; segment 999 stays absent

  // Large timeout so a prompt stop() can only come from the running_ check
  // inside the file-wait loop, not from the warm timing out.
  CursorWarmer warmer(location, dest_id, findSegmentSize(location, dest_id),
                      /*warmer_timeout_ms=*/60000);
  warmer.start(-1);
  warmer.signalNext(999); // never created -> warmer parks in the file-wait loop
  std::this_thread::sleep_for(20ms); // ensure it is actually polling

  const auto t0 = std::chrono::steady_clock::now();
  warmer.stop();
  const auto elapsed = std::chrono::steady_clock::now() - t0;
  REQUIRE(elapsed < 1s);
}

#ifdef REVOQ_ENABLE_DIAGNOSTICS
TEST_CASE_METHOD(WarmerFixture, "CursorWarmer counts a superseding signal",
                 "[cursor][warmer]") {
  const uint32_t dest_id = 0x4004;

  // Deterministic on purpose: the warmer thread is never started, so nothing
  // consumes the pending flag. The second signal therefore always lands while
  // the first is still pending and is accounted as an overwrite.
  CursorWarmer warmer(location, dest_id, findSegmentSize(location, dest_id),
                      /*warmer_timeout_ms=*/2000);
  warmer.signalNext(1);
  warmer.signalNext(2); // supersedes the still-pending segment-1 signal

  const auto stats = warmer.getStats();
  REQUIRE(stats.warmer_signals == 2);
  REQUIRE(stats.warmer_signal_overwrites == 1);
}

TEST_CASE_METHOD(WarmerFixture, "CursorWarmer drops signals after stop()",
                 "[cursor][warmer]") {
  const uint32_t dest_id = 0x5005;

  CursorWarmer warmer(location, dest_id, findSegmentSize(location, dest_id),
                      /*warmer_timeout_ms=*/2000);
  warmer.start(-1);
  warmer.stop();
  warmer.signalNext(7); // no thread to consume it -> dropped, not queued

  REQUIRE(warmer.getStats().warmer_signals == 0);
}
#endif

TEST_CASE_METHOD(WarmerFixture, "CursorWarmer start()/stop() are idempotent",
                 "[cursor][warmer]") {
  const uint32_t dest_id = 0x3003;
  writeFillerSegment(location, dest_id);

  CursorWarmer warmer(location, dest_id, findSegmentSize(location, dest_id),
                      /*warmer_timeout_ms=*/2000);
  warmer.start(-1);
  warmer.start(-1); // must be a no-op, not assign over a live thread
  warmer.stop();
  warmer.stop(); // second stop must be safe
  SUCCEED();
}

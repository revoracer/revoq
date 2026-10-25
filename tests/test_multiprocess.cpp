// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 RevoRacer
#include <catch2/catch_test_macros.hpp>
#include <ctime>
#include <sys/wait.h>
#include <unistd.h>

#include <revoq/Appender.h>
#include <revoq/MergingCursor.h>
#include "test_helpers.h"

using namespace revoq;
using namespace revoq::test;

namespace {

struct TradeMsg {
  int64_t timestamp;
  uint32_t symbol_id;
  double price;
  uint64_t volume;
  char side;
};

double elapsed_s(const struct timespec &t0) {
  struct timespec now;
  clock_gettime(CLOCK_MONOTONIC, &now);
  return (now.tv_sec - t0.tv_sec) + (now.tv_nsec - t0.tv_nsec) * 1e-9;
}

} // namespace

TEST_CASE("Cross-process cursor visibility", "[multiprocess]") {
  SECTION("cursor in separate process sees all frames written by parent") {
    TempTestDir dir;
    auto locator = std::make_shared<Locator>(dir.string());
    auto location = Location::make(Mode::LIVE, Category::EXECUTION, "test",
                                   "mp_cursor", locator);

    // DEFAULT 2 MB segments (EXECUTION + dest_id=0 → SEGMENT_SIZE_DEFAULT).
    // TradeMsg frames stride 64 B physical.
    // Frames per segment = (2 MB - 128 B) / 64 B = 32,766.
    // 70,000 messages → 2 segment rotations, exercising the cross-process
    // segment-transition path fixed in the M1 race bugfix.
    constexpr uint64_t N = 70'000;
    constexpr uint32_t DEST_ID = 0;

    // No background threads: no live threads at fork() time, so the child's
    // address-space copy has no dangling thread handles or locked mutexes.
    // The constructor calls loadInitialSegment(), creating segment 1 on disk
    // with a valid header before we fork, so the child can safely join.
    Appender<> appender(location, DEST_ID,
                        AppenderOptions{.populate = SegmentPopulate::Lazy,
                                        .background_threads = false});

    int ready_pipe[2];
    REQUIRE(pipe(ready_pipe) == 0);

    const pid_t pid = fork();
    REQUIRE(pid >= 0);

    if (pid == 0) {
      // ── child: cursor ──────────────────────────────────────────────────
      close(ready_pipe[0]);

      // No background threads in the child: keeps the child single-threaded
      // and avoids any interaction with the inherited (but dead) parent
      // threads.
      MergingCursor cursor(MergingCursorOptions{
          .populate = SegmentPopulate::Lazy, .background_threads = false});
      cursor.join(location, DEST_ID, 0);

      // Signal parent: cursor is joined and positioned at the start of
      // segment 1.
      char rdy = 1;
      write(ready_pipe[1], &rdy, 1);
      close(ready_pipe[1]);

      struct timespec t0;
      clock_gettime(CLOCK_MONOTONIC, &t0);

      int64_t prev_seq = -1;
      uint64_t data_count = 0;

      while (data_count < N) {
        if (elapsed_s(t0) > 30.0)
          _exit(1); // timeout: parent didn't produce enough frames

        if (!cursor.isDataAvailable())
          continue;

        auto frame = cursor.tryCurrentFrame();
        int64_t seq = static_cast<int64_t>(frame->getSequence());

        // Every frame — data and SegmentEnd alike — must advance the sequence
        // by exactly one. A gap means a lost frame; a repeat means corruption.
        if (seq != prev_seq + 1)
          _exit(2); // sequence gap or duplicate detected

        prev_seq = seq;

        if (static_cast<MsgType>(frame->getMsgType()) != MsgType::SegmentEnd)
          ++data_count;

        cursor.next();
      }
      _exit(0);
    }

    // ── parent: appender ────────────────────────────────────────────────────
    close(ready_pipe[1]);

    // Block until the child has joined the stream. This ensures the cursor
    // is positioned at frame 0 before the first write, so no frames are missed.
    char rdy;
    REQUIRE(read(ready_pipe[0], &rdy, 1) == 1);
    close(ready_pipe[0]);

    for (uint64_t i = 0; i < N; ++i) {
      TradeMsg msg{static_cast<int64_t>(i), static_cast<uint32_t>(i), 100.0,
                   1000, 'B'};
      publish(appender, msg.timestamp, kTagA, msg);
    }

    int status = 0;
    REQUIRE(waitpid(pid, &status, 0) == pid);
    REQUIRE(WIFEXITED(status));
    INFO("child exit code: " << WEXITSTATUS(status)
                             << "  (0=ok, 1=timeout, 2=sequence gap)");
    REQUIRE(WEXITSTATUS(status) == 0);

    const auto rotations =
        static_cast<uint64_t>(appender.getCurrentSegmentId() - 1);
    REQUIRE(rotations >= 2);
    REQUIRE(appender.getCurrentSequence() == N + rotations);
  }
}

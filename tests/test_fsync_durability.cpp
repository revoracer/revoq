// §6.7 — Async fsync durability.
//
// Verifies that frames written to a MAP_SHARED mmap are visible to a fresh
// cursor process even without an explicit msync/fdatasync call.  Simulates a
// kill -9 by having the appender child call _exit(0): no C++ destructors, no
// retire thread, no msync.  The kernel page cache retains the written segments.

#include <catch2/catch_test_macros.hpp>
#include <sys/wait.h>
#include <unistd.h>

#include <revoq/Appender.h>
#include <revoq/MergingCursor.h>
#include "test_helpers.h"

using namespace revoq;
using namespace revoq::test;

namespace {

struct DurMsg {
  uint64_t seq;
  uint64_t check; // invariant: check == ~seq
};

} // namespace

TEST_CASE("Async fsync durability", "[durability]") {
  SECTION("Frames in mmap are visible to a fresh cursor after appender _exit") {
    TempTestDir dir;
    auto locator = std::make_shared<Locator>(dir.string());
    auto location = Location::make(Mode::LIVE, Category::EXECUTION, "test",
                                   "durability", locator);

    constexpr uint64_t N = 500;
    constexpr uint32_t DEST_ID = 0;

    // appender: background_threads=false so fork() is safe (no live threads).
    // The child exits via _exit(), skipping every Segment destructor, so no
    // msync ever runs: data lives only in the kernel page cache.
    Appender<> appender(location, DEST_ID,
                        AppenderOptions{.populate = SegmentPopulate::Lazy,
                                        .background_threads = false});

    const pid_t pid = fork();
    REQUIRE(pid >= 0);

    if (pid == 0) {
      // ── child: write N frames then exit without any cleanup ──────────────
      // _exit() skips C++ destructors → no munmap, no msync, no retire thread.
      // Data remains in the kernel page cache.
      for (uint64_t i = 0; i < N; ++i) {
        DurMsg msg{i, ~i};
        publish(appender, static_cast<int64_t>(i + 1), kTagA, msg);
      }
      _exit(0);
    }

    // ── parent: wait for child then verify all frames are readable ─────────
    int status = 0;
    REQUIRE(waitpid(pid, &status, 0) == pid);
    REQUIRE(WIFEXITED(status));
    REQUIRE(WEXITSTATUS(status) == 0);

    // Fresh cursor: new mmap of the same files — reads through page cache.
    auto cursor = std::make_shared<MergingCursor>(MergingCursorOptions{
        .populate = SegmentPopulate::Lazy, .background_threads = false});
    cursor->join(location, DEST_ID, 0);

    int64_t prev_seq = -1;
    uint64_t data_count = 0;
    bool torn = false;

    while (cursor->isDataAvailable()) {
      auto frame = cursor->tryCurrentFrame();
      int64_t seq = static_cast<int64_t>(frame->getSequence());
      REQUIRE(seq == prev_seq + 1);
      prev_seq = seq;

      if (static_cast<MsgType>(frame->getMsgType()) != MsgType::SegmentEnd) {
        if (frame->getDataLength() >= sizeof(DurMsg)) {
          const auto &msg = frame->data<DurMsg>();
          if (msg.check != ~msg.seq)
            torn = true;
        }
        ++data_count;
      }
      cursor->next();
    }

    REQUIRE(!torn);
    REQUIRE(data_count == N);
  }
}

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 RevoRacer
//
// Scenario 1 (always): stream on a normal filesystem uses regular segments.
// Scenario 2 (conditional): stream on a hugetlbfs mount uses huge pages.
//   Skipped when /dev/hugepages is not mounted.  On hugetlbfs, the kernel
//   automatically backs MAP_SHARED file mmaps with 2MB huge pages; no
//   MAP_HUGETLB flag is needed in MmapBuffer.  fallocate fails on hugetlbfs
//   (EOPNOTSUPP) and MmapBuffer falls back to ftruncate, which works.

#include <catch2/catch_test_macros.hpp>
#include <filesystem>
#include <string>
#include <unistd.h>

#include <revoq/Appender.h>
#include <revoq/MergingCursor.h>
#include "test_helpers.h"

using namespace revoq;
using namespace revoq::test;
namespace fs = std::filesystem;

namespace {

struct HpMsg {
  uint64_t id;
};

void write_and_verify(const LocationPtr &location, uint32_t dest_id,
                      int n_frames) {
  {
    Appender<> appender(
        location, dest_id,
        AppenderOptions{.prefault = false, .background_threads = false});
    for (int i = 0; i < n_frames; ++i) {
      HpMsg msg{static_cast<uint64_t>(i)};
      publish(appender, static_cast<int64_t>(i + 1), kTagA, msg);
    }
  }

  auto cursor = std::make_shared<MergingCursor>(
      MergingCursorOptions{.prefault = false, .background_threads = false});
  cursor->join(location, dest_id, 0);
  int count = 0;
  while (cursor->isDataAvailable()) {
    auto frame = cursor->tryCurrentFrame();
    if (static_cast<MsgType>(frame->getMsgType()) != MsgType::SegmentEnd)
      ++count;
    cursor->next();
  }
  // Catch2 REQUIRE is callable from a free function in the same translation
  // unit
  REQUIRE(count == n_frames);
}

} // namespace

TEST_CASE("Hugepage path with fallback", "[hugepage]") {
  SECTION("Regular segments used when stream is on a normal filesystem") {
    TempTestDir dir;
    auto locator = std::make_shared<Locator>(dir.string());
    auto location = Location::make(Mode::LIVE, Category::EXECUTION, "test",
                                   "hp_normal", locator);
    // Just verify the appender/cursor work; all existing tests cover this path.
    write_and_verify(location, 0, 10);
  }

  SECTION("Hugetlbfs: appender and cursor work when stream is on hugetlbfs") {
    const fs::path hugepages_root = "/dev/hugepages";
    if (!fs::is_directory(hugepages_root) ||
        access(hugepages_root.c_str(), W_OK) != 0) {
      SKIP("hugetlbfs not writable at /dev/hugepages (not mounted or no "
           "permission)");
    }

    // Use a unique subdirectory so parallel test runs don't collide and
    // cleanup is scoped to this test.
    const fs::path test_dir =
        hugepages_root / ("revoq_test_" + std::to_string(getpid()));
    fs::create_directories(test_dir);

    struct Cleanup {
      fs::path p;
      ~Cleanup() {
        try {
          fs::remove_all(p);
        } catch (...) {
        }
      }
    } cleanup{test_dir};

    auto locator = std::make_shared<Locator>(test_dir.string());
    // DEFAULT 2 MB segment (EXECUTION + dest_id=0) == one 2MB huge page.
    auto location = Location::make(Mode::LIVE, Category::EXECUTION, "test",
                                   "hp_huge", locator);

    // 10 frames well within one 2MB segment; no rotation, no warmer needed.
    write_and_verify(location, 0, 10);
  }
}

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 RevoRacer
#pragma once

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <memory>
#include <thread>
#include <type_traits>

#include <revoq/Appender.h>
#include <revoq/Frame.h>
#include <revoq/MergingCursor.h>
#include <revoq/StreamBase.h>
#include <revobase/TimeUtils.h>

using namespace racer::revoq;
namespace fs = std::filesystem;

namespace racer::revoq::test {
// Helper to create a temporary test directory
class TempTestDir {
public:
  TempTestDir() {
    base_path_ =
        fs::temp_directory_path() / "journal_test" /
        std::to_string(
            std::chrono::system_clock::now().time_since_epoch().count());
    fs::create_directories(base_path_);
  }

  ~TempTestDir() {
    try {
      fs::remove_all(base_path_);
    } catch (...) {
      // Ignore cleanup errors in destructor
    }
  }

  fs::path path() const { return base_path_; }

  std::string string() const { return base_path_.string(); }

private:
  fs::path base_path_;
};

// Helper to create a test location
inline LocationPtr createTestLocation(const std::string &path) {
  const auto locator = std::make_shared<Locator>(path);
  auto loc = Location::make(Mode::LIVE, Category::EXECUTION, "test",
                            "appender_test_resume", locator);
  return loc;
}

template <typename Appender, typename T>
void publish(Appender &appender, int64_t timestamp, MsgType msg_type,
             const T &data, uint16_t flags = FRAME_NORMAL) {
  static_assert(std::is_trivially_copyable_v<T>,
                "stream test messages must be trivially copyable");
  auto &slot = appender.template openData<T>(timestamp, msg_type, flags);
  std::memcpy(&slot, &data, sizeof(T));
  appender.template closeData<T>();
}

// Trivially-copyable filler payload for tests that only need segments to exist.
struct FillerMessage {
  int64_t ts;
  uint32_t id;
};

// Create a prefault appender and publish `count` filler trades, then let the
// appender fall out of scope so the resulting segment file(s) persist on disk
// for a cursor/warmer to map. A handful of frames stays well within segment 1.
inline void writeFillerSegment(const LocationPtr &location, uint32_t dest_id,
                               int count = 8) {
  auto appender = std::make_shared<Appender<>>(
      location, dest_id, AppenderOptions{.prefault = true});
  const int64_t base = revobase::Timestamp::now().nsec();
  for (int i = 0; i < count; ++i) {
    const FillerMessage m{base + i, static_cast<uint32_t>(i)};
    publish(*appender, m.ts, MsgType::Trade, m);
  }
}
} // namespace racer::revoq::test

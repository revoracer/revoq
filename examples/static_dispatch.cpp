// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 RevoRacer
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <stdexcept>

#include <spdlog/spdlog.h>

#include <revoq/StreamBase.h>
#include <revoq/MergingCursor.h>
#include <revoq/Appender.h>

namespace fs = std::filesystem;
using namespace revoq;

namespace {

// revoq::MsgType only carries the transport markers (SegmentEnd,
// SessionStart/End); payload tags like Trade belong to the registry that
// owns the payload (revomd::MsgType for real market data). This example
// doesn't use real market-data types, so it claims its own tag enum instead
// of inventing nonexistent revoq::MsgType members.
enum class ExampleMsgType : std::uint16_t {
  Trade = 100,
  BBO = 106,
  BookReset = 108,
};

struct Trade {
  std::int64_t timestamp;
  std::uint32_t symbol_id;
  double price;
};

struct Bbo {
  std::int64_t timestamp;
  std::uint32_t bid_size;
  std::uint32_t ask_size;
};

class TempDir {
public:
  explicit TempDir(const char *name) {
    const auto stamp =
        std::chrono::steady_clock::now().time_since_epoch().count();
    path_ = fs::temp_directory_path() / (std::string("revoq-example-") + name +
                                         "-" + std::to_string(stamp));
    fs::create_directories(path_);
  }

  ~TempDir() {
    std::error_code ignored;
    fs::remove_all(path_, ignored);
  }

  [[nodiscard]] std::string string() const { return path_.string(); }

private:
  fs::path path_;
};

template <typename T, FrameTag Tag>
void publish(Appender<> &appender, std::int64_t timestamp, Tag msg_type,
             const T &message) {
  auto &slot = appender.openData<T>(timestamp, msg_type);
  slot = message;
  appender.closeData<T>();
}

} // namespace

int main() {
  try {
    spdlog::set_level(spdlog::level::warn);

    TempDir dir("static-dispatch");
    auto locator = std::make_shared<Locator>(dir.string());
    auto location = Location::make(Mode::LIVE, Category::EXECUTION, "examples",
                                   "static-dispatch", locator);
    constexpr std::uint32_t dest_id = 0;
    constexpr int messages = 1000;

    {
      Appender<> appender(
          location, dest_id,
          AppenderOptions{.prefault = false, .background_threads = false});
      for (int i = 0; i < messages; ++i) {
        const auto ts = static_cast<std::int64_t>(i + 1);
        if (i % 3 == 0) {
          publish(appender, ts, ExampleMsgType::Trade,
                  Trade{ts, 101, 100.0 + static_cast<double>(i)});
        } else if (i % 3 == 1) {
          publish(appender, ts, ExampleMsgType::BBO,
                  Bbo{ts, static_cast<std::uint32_t>(i),
                      static_cast<std::uint32_t>(i + 1)});
        } else {
          appender.mark(ts, ExampleMsgType::BookReset);
        }
      }
    }

    MergingCursor cursor(
        MergingCursorOptions{.prefault = false, .background_threads = false});
    cursor.join(location, dest_id, 0);

    int handled = 0;
    std::uint64_t checksum = 0;
    auto dispatcher = [&](MergingCursor::FrameView current) {
      switch (static_cast<std::int32_t>(current.msg_type)) {
      case static_cast<std::int32_t>(ExampleMsgType::Trade): {
        const auto &trade = current->data<Trade>();
        checksum ^= static_cast<std::uint64_t>(trade.symbol_id) +
                    static_cast<std::uint64_t>(trade.price);
        ++handled;
        break;
      }
      case static_cast<std::int32_t>(ExampleMsgType::BBO): {
        const auto &bbo = current->data<Bbo>();
        checksum ^= bbo.bid_size + bbo.ask_size;
        ++handled;
        break;
      }
      case static_cast<std::int32_t>(ExampleMsgType::BookReset):
        ++handled;
        break;
      case static_cast<std::int32_t>(MsgType::SegmentEnd):
        break;
      default:
        throw std::runtime_error("unexpected message type");
      }
    };

    while (cursor.nextStatic(dispatcher)) {
    }

    if (handled != messages)
      throw std::runtime_error("static dispatch verification failed");

    std::cout << "ok static_dispatch handled=" << handled
              << " checksum=" << checksum << '\n';
    return 0;
  } catch (const std::exception &e) {
    std::cerr << "error: " << e.what() << '\n';
    return 1;
  }
}

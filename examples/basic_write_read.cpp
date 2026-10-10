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
};

struct Trade {
  std::int64_t timestamp;
  std::uint32_t symbol_id;
  double price;
  std::uint64_t quantity;
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

    TempDir dir("basic-write-read");
    auto locator = std::make_shared<Locator>(dir.string());
    auto location = Location::make(Mode::LIVE, Category::EXECUTION, "examples",
                                   "basic-write-read", locator);
    constexpr std::uint32_t dest_id = 0;

    {
      Appender<> appender(
          location, dest_id,
          AppenderOptions{.prefault = false, .background_threads = false});
      publish(appender, 1, ExampleMsgType::Trade, Trade{1, 101, 100.25, 10});
      publish(appender, 2, ExampleMsgType::Trade, Trade{2, 101, 100.50, 20});
      publish(appender, 3, ExampleMsgType::Trade, Trade{3, 101, 100.75, 30});
    }

    MergingCursor cursor(
        MergingCursorOptions{.prefault = false, .background_threads = false});
    cursor.join(location, dest_id, 0);

    int count = 0;
    double last_price = 0.0;
    while (const auto current = cursor.tryCurrentFrame()) {
      if (static_cast<ExampleMsgType>(current.msg_type) ==
          ExampleMsgType::Trade) {
        const auto &trade = current->data<Trade>();
        ++count;
        last_price = trade.price;
      }
      cursor.advance(current);
    }

    if (count != 3 || last_price != 100.75)
      throw std::runtime_error("basic write/read verification failed");

    std::cout << "ok basic_write_read messages=" << count << '\n';
    return 0;
  } catch (const std::exception &e) {
    std::cerr << "error: " << e.what() << '\n';
    return 1;
  }
}

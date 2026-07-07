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
#include <revoq/TypedDispatcher.h>

namespace fs = std::filesystem;
using namespace racer::revoq;

namespace {

struct Trade {
  static constexpr MsgType kMsgType = MsgType::Trade;

  std::int64_t timestamp;
  std::uint32_t symbol_id;
  double price;
};

struct Bbo {
  static constexpr MsgType kMsgType = MsgType::BBO;

  std::int64_t timestamp;
  std::uint32_t bid_size;
  std::uint32_t ask_size;
};

struct BookReset {
  static constexpr MsgType kMsgType = MsgType::BookReset;
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

template <typename T> void publish(Appender<> &appender, const T &message) {
  auto &slot = appender.openData<T>(message.timestamp, T::kMsgType);
  slot = message;
  appender.closeData<T>();
}

} // namespace

int main() {
  try {
    spdlog::set_level(spdlog::level::warn);

    TempDir dir("typed-dispatch");
    auto locator = std::make_shared<Locator>(dir.string());
    auto location = Location::make(Mode::LIVE, Category::EXECUTION, "examples",
                                   "typed-dispatch", locator);
    constexpr std::uint32_t dest_id = 0;

    {
      Appender<> appender(
          location, dest_id,
          AppenderOptions{.prefault = false, .background_threads = false});
      publish(appender, Trade{1, 101, 100.25});
      publish(appender, Bbo{2, 500, 700});
      appender.mark(3, BookReset::kMsgType);
      publish(appender, Trade{4, 101, 100.50});
    }

    MergingCursor cursor(
        MergingCursorOptions{.prefault = false, .background_threads = false});
    cursor.join(location, dest_id, 0);

    int trades = 0;
    int bbo = 0;
    int resets = 0;
    double last_trade = 0.0;
    std::uint32_t bbo_size = 0;

    TypedDispatcher dispatcher(cursor);
    dispatcher
        .on<Trade>([&](const Trade &trade) {
          ++trades;
          last_trade = trade.price;
        })
        .on<Bbo>([&](const Bbo &quote) {
          ++bbo;
          bbo_size = quote.bid_size + quote.ask_size;
        })
        .on<BookReset>([&](const BookReset &, const Frame &frame) {
          ++resets;
          if (frame.getDataLength() != 0)
            throw std::runtime_error("BookReset should have no payload");
        });

    const auto dispatched = dispatcher.run();
    if (dispatched != 4 || trades != 2 || bbo != 1 || resets != 1 ||
        last_trade != 100.50 || bbo_size != 1200) {
      throw std::runtime_error("typed dispatch verification failed");
    }

    std::cout << "ok typed_dispatch trades=" << trades << " bbo=" << bbo
              << " resets=" << resets << '\n';
    return 0;
  } catch (const std::exception &e) {
    std::cerr << "error: " << e.what() << '\n';
    return 1;
  }
}

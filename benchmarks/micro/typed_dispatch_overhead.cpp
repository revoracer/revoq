// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 RevoRacer
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>

#include <spdlog/spdlog.h>

#include <revoq/StreamBase.h>
#include <revoq/MergingCursor.h>
#include <revoq/Appender.h>
#include <revoq/TypedDispatcher.h>

namespace fs = std::filesystem;
using namespace revoq;

// revoq::MsgType only carries transport markers; payload tags belong to the
// payload owner.
enum class BenchMsgType : std::uint16_t {
  Trade = 100,
  BookTop20 = 105,
  BBO = 106,
  BookReset = 108,
  SegmentEnd = static_cast<std::uint16_t>(revoq::MsgType::SegmentEnd),
};

namespace {

constexpr std::uint64_t kDefaultMessages = 1'000'000;
constexpr std::uint32_t kDestId = 0;
constexpr std::uint64_t kChecksumMultiplier = 11400714819323198485ull;

struct Trade {
  static constexpr revoq::MsgType kMsgType =
      static_cast<revoq::MsgType>(BenchMsgType::Trade);
  std::int64_t timestamp;
  std::uint32_t symbol_id;
  double price;
};

struct Bbo {
  static constexpr revoq::MsgType kMsgType =
      static_cast<revoq::MsgType>(BenchMsgType::BBO);
  std::int64_t timestamp;
  std::uint32_t bid_size;
  std::uint32_t ask_size;
};

struct BookReset {
  static constexpr revoq::MsgType kMsgType =
      static_cast<revoq::MsgType>(BenchMsgType::BookReset);
};

class TempDir {
public:
  explicit TempDir(std::string_view name) {
    const auto stamp =
        std::chrono::steady_clock::now().time_since_epoch().count();
    path_ =
        fs::temp_directory_path() /
        ("revoq-benchmark-" + std::string(name) + "-" + std::to_string(stamp));
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

std::uint64_t parseU64(std::string_view value, std::string_view name) {
  std::size_t pos = 0;
  auto parsed = std::stoull(std::string(value), &pos);
  if (pos != value.size())
    throw std::invalid_argument(std::string(name) + " expects an integer");
  return parsed;
}

std::uint64_t parseMessages(int argc, char **argv) {
  std::uint64_t messages = kDefaultMessages;
  for (int i = 1; i < argc; ++i) {
    const std::string_view arg(argv[i]);
    if (arg == "--help" || arg == "-h") {
      std::cout << "Usage: " << argv[0] << " [--messages N]\n";
      std::exit(0);
    }
    if (arg == "--messages" && i + 1 < argc) {
      messages = parseU64(argv[++i], "--messages");
      continue;
    }
    if (arg.starts_with("--messages=")) {
      messages = parseU64(arg.substr(std::string_view("--messages=").size()),
                          "--messages");
      continue;
    }
    throw std::invalid_argument("unknown argument: " + std::string(arg));
  }
  if (messages == 0)
    throw std::invalid_argument("--messages must be greater than zero");
  return messages;
}

template <typename T>
void publish(Appender<> &appender, std::int64_t timestamp, BenchMsgType msg_type,
             const T &message) {
  auto &slot = appender.openData<T>(timestamp, msg_type);
  slot = message;
  appender.closeData<T>();
}

void writeStream(const LocationPtr &location, std::uint64_t messages) {
  Appender<> appender(
      location, kDestId,
      AppenderOptions{.prefault = false, .background_threads = false});

  for (std::uint64_t i = 0; i < messages; ++i) {
    const auto ts = static_cast<std::int64_t>(i + 1);
    switch (i % 3) {
    case 0:
      publish(appender, ts, BenchMsgType::Trade,
              Trade{ts, 101, 100.0 + static_cast<double>(i)});
      break;
    case 1:
      publish(appender, ts, BenchMsgType::BBO,
              Bbo{ts, static_cast<std::uint32_t>(i),
                  static_cast<std::uint32_t>(i + 1)});
      break;
    default:
      appender.mark(ts, BenchMsgType::BookReset);
      break;
    }
  }
}

struct Result {
  const char *name;
  std::uint64_t messages;
  double seconds;
  double messages_per_second;
  double ns_per_message;
  const char *guard_kind;
  std::uint64_t guard;
};

MergingCursor makeCursor(const LocationPtr &location) {
  MergingCursor cursor(
      MergingCursorOptions{.prefault = false, .background_threads = false});
  cursor.join(location, kDestId, 0);
  return cursor;
}

Result runRawFrameLoop(const LocationPtr &location, std::uint64_t expected) {
  auto cursor = makeCursor(location);
  std::uint64_t count = 0;
  std::uint64_t checksum = 0;

  const auto start = std::chrono::steady_clock::now();
  while (const auto current = cursor.tryCurrentFrame()) {
    if (static_cast<BenchMsgType>(current.msg_type) != BenchMsgType::SegmentEnd) {
      ++count;
      checksum += (current->getSequence() + 1) * kChecksumMultiplier;
      checksum ^= static_cast<std::uint64_t>(current.frame_length) << 32;
    }
    cursor.advance(current);
  }
  const auto end = std::chrono::steady_clock::now();

  if (count != expected)
    throw std::runtime_error("raw frame loop count verification failed");
  const double seconds = std::chrono::duration<double>(end - start).count();
  return {"raw_frame_loop",
          count,
          seconds,
          static_cast<double>(count) / seconds,
          seconds * 1e9 / static_cast<double>(count),
          "frame",
          checksum};
}

Result runStaticSwitch(const LocationPtr &location, std::uint64_t expected) {
  auto cursor = makeCursor(location);
  std::uint64_t count = 0;
  std::uint64_t checksum = 0;
  auto dispatcher = [&](MergingCursor::FrameView current) {
    switch (static_cast<BenchMsgType>(current.msg_type)) {
    case BenchMsgType::Trade: {
      const auto &trade = current->data<Trade>();
      checksum ^= trade.symbol_id + static_cast<std::uint64_t>(trade.price);
      ++count;
      break;
    }
    case BenchMsgType::BBO: {
      const auto &bbo = current->data<Bbo>();
      checksum ^= bbo.bid_size + bbo.ask_size;
      ++count;
      break;
    }
    case BenchMsgType::BookReset:
      checksum ^= current->getSequence();
      ++count;
      break;
    case BenchMsgType::SegmentEnd:
      break;
    default:
      throw std::runtime_error("unexpected message type");
    }
  };

  const auto start = std::chrono::steady_clock::now();
  while (cursor.nextStatic(dispatcher)) {
  }
  const auto end = std::chrono::steady_clock::now();

  if (count != expected)
    throw std::runtime_error("static switch count verification failed");
  const double seconds = std::chrono::duration<double>(end - start).count();
  return {"static_switch",
          count,
          seconds,
          static_cast<double>(count) / seconds,
          seconds * 1e9 / static_cast<double>(count),
          "payload",
          checksum};
}

Result runTypedDispatcher(const LocationPtr &location, std::uint64_t expected) {
  auto cursor = makeCursor(location);
  std::uint64_t count = 0;
  std::uint64_t checksum = 0;

  TypedDispatcher dispatcher(cursor);
  dispatcher
      .on<Trade>([&](const Trade &trade) {
        checksum ^= trade.symbol_id + static_cast<std::uint64_t>(trade.price);
        ++count;
      })
      .on<Bbo>([&](const Bbo &bbo) {
        checksum ^= bbo.bid_size + bbo.ask_size;
        ++count;
      })
      .on<BookReset>([&](const BookReset &, const Frame &frame) {
        checksum ^= frame.getSequence();
        ++count;
      });

  const auto start = std::chrono::steady_clock::now();
  dispatcher.run();
  const auto end = std::chrono::steady_clock::now();

  if (count != expected)
    throw std::runtime_error("typed dispatcher count verification failed");
  const double seconds = std::chrono::duration<double>(end - start).count();
  return {"typed_dispatcher",
          count,
          seconds,
          static_cast<double>(count) / seconds,
          seconds * 1e9 / static_cast<double>(count),
          "payload",
          checksum};
}

void printResult(const Result &result) {
  std::cout << std::left << std::setw(18) << result.name << std::right
            << std::setw(12) << result.messages << std::setw(13) << std::fixed
            << std::setprecision(6) << result.seconds << std::setw(13)
            << std::fixed << std::setprecision(2)
            << result.messages_per_second / 1e6 << std::setw(12) << std::fixed
            << std::setprecision(2) << result.ns_per_message << std::setw(12)
            << result.guard_kind << std::setw(22) << result.guard << '\n';
}

void printHeader() {
  std::cout << std::left << std::setw(18) << "row" << std::right
            << std::setw(12) << "messages" << std::setw(13) << "seconds"
            << std::setw(13) << "Mmsg/s" << std::setw(12) << "ns/msg"
            << std::setw(12) << "guard" << std::setw(22) << "value" << '\n';
  std::cout << std::string(102, '-') << '\n';
}

} // namespace

int main(int argc, char **argv) {
  try {
    spdlog::set_level(spdlog::level::warn);
    const auto messages = parseMessages(argc, argv);

    TempDir dir("typed-dispatch-overhead");
    auto locator = std::make_shared<Locator>(dir.string());
    auto location = Location::make(Mode::LIVE, Category::EXECUTION, "bench",
                                   "typed-dispatch-overhead", locator);
    writeStream(location, messages);

    std::cout << "benchmark=revoq_typed_dispatch_overhead\n";
    std::cout << "messages=" << messages << '\n';
    printHeader();
    printResult(runRawFrameLoop(location, messages));
    printResult(runStaticSwitch(location, messages));
    printResult(runTypedDispatcher(location, messages));
    return 0;
  } catch (const std::exception &e) {
    std::cerr << "error: " << e.what() << '\n';
    return 1;
  }
}

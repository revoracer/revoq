// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 RevoRacer
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include <spdlog/spdlog.h>

#include <revoq/StreamBase.h>
#include <revoq/Appender.h>

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

constexpr std::uint64_t kDefaultMessages = 200'000;
constexpr std::uint32_t kDestId = 0;

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

struct Result {
  std::size_t payload_bytes;
  std::uint64_t messages;
  double seconds;
  double messages_per_second;
  std::uint64_t final_sequence;
};

Result runCase(std::uint64_t messages, std::size_t payload_bytes) {
  TempDir dir("write-throughput");
  auto locator = std::make_shared<Locator>(dir.string());
  auto location = Location::make(Mode::LIVE, Category::EXECUTION, "bench",
                                 "write-throughput", locator);
  Appender<> appender(
      location, kDestId,
      AppenderOptions{.prefault = false, .background_threads = false});

  std::vector<std::byte> payload(payload_bytes);
  for (std::size_t i = 0; i < payload.size(); ++i)
    payload[i] = static_cast<std::byte>(i);

  const auto start = std::chrono::steady_clock::now();
  for (std::uint64_t i = 0; i < messages; ++i) {
    appender.writeBytes(static_cast<std::int64_t>(i + 1), BenchMsgType::Trade,
                      payload.data(), payload.size());
  }
  const auto end = std::chrono::steady_clock::now();

  const double seconds = std::chrono::duration<double>(end - start).count();
  return Result{
      .payload_bytes = payload_bytes,
      .messages = messages,
      .seconds = seconds,
      .messages_per_second = static_cast<double>(messages) / seconds,
      .final_sequence = appender.getCurrentSequence(),
  };
}

void printHeader() {
  std::cout << std::left << std::setw(14) << "payload_bytes" << std::right
            << std::setw(13) << "seconds" << std::setw(13) << "Mmsg/s"
            << std::setw(12) << "ns/msg" << std::setw(14) << "frames"
            << std::setw(12) << "segment_end" << '\n';
  std::cout << std::string(80, '-') << '\n';
}

void printResult(const Result &result) {
  const double mmsg_per_second = result.messages_per_second / 1e6;
  const double ns_per_message =
      result.seconds * 1e9 / static_cast<double>(result.messages);
  const auto segment_end_frames = result.final_sequence - result.messages;

  std::cout << std::right << std::setw(14) << result.payload_bytes
            << std::setw(13) << std::fixed << std::setprecision(6)
            << result.seconds << std::setw(13) << std::fixed
            << std::setprecision(2) << mmsg_per_second << std::setw(12)
            << std::fixed << std::setprecision(2) << ns_per_message
            << std::setw(14) << result.final_sequence << std::setw(12)
            << segment_end_frames << '\n';
}

} // namespace

int main(int argc, char **argv) {
  try {
    spdlog::set_level(spdlog::level::warn);
    const auto messages = parseMessages(argc, argv);

    std::cout << "benchmark=revoq_journal_write_throughput\n";
    std::cout << "messages=" << messages << '\n';
    printHeader();

    for (const auto payload_bytes :
         std::array<std::size_t, 4>{0, 32, 128, 512}) {
      const auto result = runCase(messages, payload_bytes);
      if (result.final_sequence < result.messages)
        throw std::runtime_error("appender sequence verification failed");
      printResult(result);
    }
    return 0;
  } catch (const std::exception &e) {
    std::cerr << "error: " << e.what() << '\n';
    return 1;
  }
}

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 RevoRacer
// Minimal application-path latency benchmark for revoq journal dispatch.
//
// This public benchmark uses portable defaults rather than machine-specific
// tuning. It measures the path from a message's intended send time to the
// cursor handler endpoint using a deterministic mix of small fixed-size
// messages. When paced (--interval-ns > 0) the frame timestamp is the message's
// scheduled departure time, not the moment it actually left, so producer stalls
// are reflected in the tail (i.e. free of coordinated omission). Unpaced runs
// (--interval-ns 0) stamp the actual send time and should be read as
// throughput/saturation runs, not latency percentiles.

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <filesystem>
#include <numeric>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <type_traits>
#include <vector>

#ifdef __linux__
#include <csignal>
#include <linux/magic.h>
#include <sys/mman.h>
#include <sys/vfs.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

#include <fmt/format.h>
#include <spdlog/spdlog.h>

#include <hdr/hdr_histogram.h>
#include <hdr/hdr_histogram_log.h>

#include <revoq/Appender.h>
#include <revoq/Frame.h>
#include <revoq/MergingCursor.h>
#include <revoq/StreamBase.h>
#include <revobase/CpuAffinity.h>
#include <revobase/RealtimePriority.h>
#include <revobase/TimeUtils.h>
#include <revobase/portable_intrinsics.h>

#ifndef HUGETLBFS_MAGIC
#define HUGETLBFS_MAGIC 0x958458f6
#endif

namespace {

using namespace racer::revoq;
using revobase::Timestamp;

constexpr std::uint32_t kDestId = 0;
constexpr std::uint64_t kDefaultMessages = 1'000'000;
constexpr std::uint64_t kDefaultWarmupMessages = 50'000;
constexpr std::uint64_t kDefaultIntervalNs = 1'000;
constexpr std::size_t kTunedAppenderWarmupSegments = 8;
constexpr std::size_t kTunedAppenderRetainedSegments = 10;

// A benchmark message is a 32-byte header of hot fields plus optional padding to
// reach the requested wire size. Tag gives the four message kinds distinct C++
// types so the typed dispatch is a real 4-way switch; Bytes selects the payload
// size (uniform within a run). [[no_unique_address]] collapses the empty pad so
// the 32-byte case stays exactly 32 bytes.
constexpr std::size_t kBenchHeaderBytes = 32;

template <std::uint64_t Tag, std::size_t Bytes> struct BenchMessage {
  static_assert(Bytes > kBenchHeaderBytes && Bytes % 8 == 0,
                "payload bytes must be >= 32 and 8-byte aligned");
  std::uint64_t sequence;
  std::uint64_t value0;
  std::uint64_t value1;
  std::uint64_t value2;
  unsigned char pad[Bytes - kBenchHeaderBytes];
};

// The 32-byte case has no padding, so specialize it away (a zero-length array is
// not standard C++ and an empty std::array is not guaranteed to occupy no space).
template <std::uint64_t Tag> struct BenchMessage<Tag, kBenchHeaderBytes> {
  std::uint64_t sequence;
  std::uint64_t value0;
  std::uint64_t value1;
  std::uint64_t value2;
};

// Supported payload sizes for --payload-bytes. Each maps to a compile-time
// instantiation of the write/read paths.
inline constexpr std::array<std::size_t, 4> kSupportedPayloadBytes{32, 64, 256,
                                                                   1024};

using TradeMessage = BenchMessage<0, 32>;

static_assert(sizeof(TradeMessage) == 32);
static_assert(sizeof(BenchMessage<0, 64>) == 64);
static_assert(sizeof(BenchMessage<0, 256>) == 256);
static_assert(sizeof(BenchMessage<0, 1024>) == 1024);
static_assert(std::is_trivially_copyable_v<TradeMessage>);
static_assert(std::is_trivially_copyable_v<BenchMessage<0, 1024>>);
static_assert(framePhysicalLength(sizeof(FrameHeader) + sizeof(TradeMessage)) ==
              FRAME_ALIGNMENT);

struct BenchmarkConfig {
  std::uint64_t messages{kDefaultMessages};
  std::uint64_t warmup_messages{kDefaultWarmupMessages};
  std::uint64_t interval_ns{kDefaultIntervalNs};
  std::size_t payload_bytes{kBenchHeaderBytes};
  std::filesystem::path path;
  bool keep_files{false};
  bool path_set{false};
  bool tuned{false};
  bool fork_cursor{false};
  int appender_cpu{-1};
  int cursor_cpu{-1};
  int appender_warmer_cpu{-1};
  int cursor_warmer_cpu{-1};
  int rt_priority{80};
};

struct LatencyStats {
  std::uint64_t samples{0};
  std::int64_t min_ns{0};
  std::int64_t p50_ns{0};
  std::int64_t p90_ns{0};
  std::int64_t p99_ns{0};
  std::int64_t p999_ns{0};
  std::int64_t p9999_ns{0};
  std::int64_t p99999_ns{0};
  std::int64_t max_ns{0};
  double mean_ns{0.0};
};

struct BenchmarkResult {
  LatencyStats latency;
  std::uint64_t handled_messages{0};
  std::uint64_t checksum{0};
};
static_assert(std::is_trivially_copyable_v<BenchmarkResult>);

// BenchmarkResult is trivially copyable because it is memcpy'd across the
// fork-cursor pipe. The HdrHistogram encoding is variable length, so it travels
// alongside the result rather than inside it (streamed length-prefixed over the
// pipe in the forked path).
struct BenchmarkOutput {
  BenchmarkResult result;
  std::string hdr_b64;
  double achieved_mps{0.0}; // producer's sustained rate over the measured window
};

std::filesystem::path defaultPath() {
  const char *tmpdir = std::getenv("TMPDIR");
  const auto base =
      tmpdir ? std::filesystem::path(tmpdir) : std::filesystem::path("/tmp");
  const auto stamp =
      std::chrono::steady_clock::now().time_since_epoch().count();
  return base / fmt::format("revoq-journal-handler-p99-{}", stamp);
}

std::uint64_t parseU64(std::string_view text, std::string_view name) {
  std::uint64_t value = 0;
  const char *begin = text.data();
  const char *end = begin + text.size();
  const auto parsed = std::from_chars(begin, end, value);
  if (text.empty() || parsed.ec != std::errc{} || parsed.ptr != end) {
    throw std::invalid_argument(
        fmt::format("{} expects an unsigned integer", name));
  }
  return value;
}

int parseInt(std::string_view text, std::string_view name, int min_value,
             int max_value) {
  int value = 0;
  const char *begin = text.data();
  const char *end = begin + text.size();
  const auto parsed = std::from_chars(begin, end, value);
  if (text.empty() || parsed.ec != std::errc{} || parsed.ptr != end ||
      value < min_value || value > max_value) {
    throw std::invalid_argument(fmt::format("{} expects an integer in [{}, {}]",
                                            name, min_value, max_value));
  }
  return value;
}

std::string_view readOptionValue(std::string_view arg, std::string_view name,
                                 int argc, char **argv, int &index) {
  if (arg == name) {
    if (index + 1 >= argc)
      throw std::invalid_argument(fmt::format("{} requires a value", name));
    return argv[++index];
  }

  const std::string prefix = std::string(name) + "=";
  if (arg.starts_with(prefix))
    return arg.substr(prefix.size());

  return {};
}

bool optionMatches(std::string_view arg, std::string_view name) {
  const std::string prefix = std::string(name) + "=";
  return arg == name || arg.starts_with(prefix);
}

void printUsage(const char *exe) {
  fmt::print(stderr,
             "Usage: {} [--messages N] [--warmup N] [--interval-ns N] "
             "[--path DIR] [--keep-files]\n",
             exe);
  fmt::print(stderr,
             "       {} --tuned --path DIR --appender-cpu N --cursor-cpu N "
             "--appender-warmer-cpu N --cursor-warmer-cpu N "
             "[--rt-priority N] [--fork]\n",
             exe);
  fmt::print(stderr, "Defaults: --messages {} --warmup {} --interval-ns {}\n",
             kDefaultMessages, kDefaultWarmupMessages, kDefaultIntervalNs);
  fmt::print(stderr,
             "Use --interval-ns 0 for an unpaced appender. The default path is "
             "a temporary directory under /tmp.\n");
  fmt::print(stderr,
             "Use --payload-bytes N (32, 64, 256, or 1024; default 32) to set "
             "the message wire size.\n");
  fmt::print(stderr,
             "Tuned mode requires Linux, a hugetlbfs --path, four explicit "
             "CPU assignments, and usually root or CAP_SYS_NICE.\n");
}

BenchmarkConfig parseArgs(int argc, char **argv) {
  BenchmarkConfig cfg;
  cfg.path = defaultPath();

  for (int i = 1; i < argc; ++i) {
    const std::string_view arg(argv[i]);

    if (arg == "--help" || arg == "-h") {
      printUsage(argv[0]);
      std::exit(0);
    }
    if (arg == "--keep-files") {
      cfg.keep_files = true;
      continue;
    }
    if (arg == "--tuned") {
      cfg.tuned = true;
      cfg.fork_cursor = true;
      continue;
    }
    if (arg == "--fork") {
      cfg.fork_cursor = true;
      continue;
    }
    if (optionMatches(arg, "--messages")) {
      cfg.messages = parseU64(readOptionValue(arg, "--messages", argc, argv, i),
                              "--messages");
      if (cfg.messages == 0)
        throw std::invalid_argument("--messages must be greater than zero");
      continue;
    }
    if (optionMatches(arg, "--warmup")) {
      cfg.warmup_messages =
          parseU64(readOptionValue(arg, "--warmup", argc, argv, i), "--warmup");
      continue;
    }
    if (optionMatches(arg, "--interval-ns")) {
      cfg.interval_ns =
          parseU64(readOptionValue(arg, "--interval-ns", argc, argv, i),
                   "--interval-ns");
      continue;
    }
    if (optionMatches(arg, "--payload-bytes")) {
      const auto requested = static_cast<std::size_t>(parseU64(
          readOptionValue(arg, "--payload-bytes", argc, argv, i),
          "--payload-bytes"));
      if (std::find(kSupportedPayloadBytes.begin(),
                    kSupportedPayloadBytes.end(),
                    requested) == kSupportedPayloadBytes.end()) {
        throw std::invalid_argument(
            "--payload-bytes must be one of 32, 64, 256, 1024");
      }
      cfg.payload_bytes = requested;
      continue;
    }
    if (optionMatches(arg, "--path")) {
      cfg.path = std::filesystem::path(
          std::string(readOptionValue(arg, "--path", argc, argv, i)));
      cfg.path_set = true;
      continue;
    }
    if (optionMatches(arg, "--appender-cpu")) {
      cfg.appender_cpu =
          parseInt(readOptionValue(arg, "--appender-cpu", argc, argv, i),
                   "--appender-cpu", 0, 4096);
      continue;
    }
    if (optionMatches(arg, "--cursor-cpu")) {
      cfg.cursor_cpu =
          parseInt(readOptionValue(arg, "--cursor-cpu", argc, argv, i),
                   "--cursor-cpu", 0, 4096);
      continue;
    }
    if (optionMatches(arg, "--appender-warmer-cpu")) {
      cfg.appender_warmer_cpu =
          parseInt(readOptionValue(arg, "--appender-warmer-cpu", argc, argv, i),
                   "--appender-warmer-cpu", 0, 4096);
      continue;
    }
    if (optionMatches(arg, "--cursor-warmer-cpu")) {
      cfg.cursor_warmer_cpu =
          parseInt(readOptionValue(arg, "--cursor-warmer-cpu", argc, argv, i),
                   "--cursor-warmer-cpu", 0, 4096);
      continue;
    }
    if (optionMatches(arg, "--rt-priority")) {
      cfg.rt_priority =
          parseInt(readOptionValue(arg, "--rt-priority", argc, argv, i),
                   "--rt-priority", 1, 99);
      continue;
    }

    throw std::invalid_argument(fmt::format("unknown argument: {}", arg));
  }

  if (!cfg.tuned) {
    if (cfg.fork_cursor || cfg.appender_cpu >= 0 || cfg.cursor_cpu >= 0 ||
        cfg.appender_warmer_cpu >= 0 || cfg.cursor_warmer_cpu >= 0) {
      throw std::invalid_argument(
          "--fork and CPU options are tuned-mode options; add --tuned");
    }
    return cfg;
  }

#ifndef __linux__
  throw std::invalid_argument("--tuned is currently supported only on Linux");
#endif

  cfg.fork_cursor = true;
  if (!cfg.path_set) {
    throw std::invalid_argument(
        "--tuned requires an explicit --path on hugetlbfs, for example "
        "--path /dev/hugepages/revoq-bench");
  }
  if (cfg.appender_cpu < 0 || cfg.cursor_cpu < 0 ||
      cfg.appender_warmer_cpu < 0 || cfg.cursor_warmer_cpu < 0) {
    throw std::invalid_argument(
        "--tuned requires --appender-cpu, --cursor-cpu, "
        "--appender-warmer-cpu, and --cursor-warmer-cpu");
  }
  if (cfg.appender_cpu == cfg.cursor_cpu ||
      cfg.appender_cpu == cfg.appender_warmer_cpu ||
      cfg.appender_cpu == cfg.cursor_warmer_cpu ||
      cfg.cursor_cpu == cfg.appender_warmer_cpu ||
      cfg.cursor_cpu == cfg.cursor_warmer_cpu ||
      cfg.appender_warmer_cpu == cfg.cursor_warmer_cpu) {
    throw std::invalid_argument(
        "tuned CPUs must be distinct: appender, cursor, appender warmer, "
        "and cursor warmer");
  }
  return cfg;
}

void validateTunedPath(const std::filesystem::path &path) {
#ifdef __linux__
  struct statfs fs{};
  if (statfs(path.c_str(), &fs) != 0) {
    throw std::runtime_error(
        fmt::format("statfs({}) failed: {}", path.string(), strerror(errno)));
  }
  if (static_cast<unsigned long>(fs.f_type) !=
      static_cast<unsigned long>(HUGETLBFS_MAGIC)) {
    throw std::runtime_error(fmt::format(
        "{} is not on hugetlbfs. Mount huge pages first, for example "
        "at /dev/hugepages, then pass a subdirectory with --path.",
        path.string()));
  }
#else
  (void)path;
  throw std::runtime_error("tuned benchmark path validation requires Linux");
#endif
}

struct BenchmarkFixture {
  explicit BenchmarkFixture(const BenchmarkConfig &cfg)
      : path(cfg.path), keep_files(cfg.keep_files) {
    if (std::filesystem::exists(path) && !std::filesystem::is_directory(path)) {
      throw std::runtime_error(
          fmt::format("benchmark path is not a directory: {}", path.string()));
    }
    if (std::filesystem::exists(path) && !std::filesystem::is_empty(path)) {
      throw std::runtime_error(fmt::format(
          "benchmark path is not empty: {}. Pick a new --path or remove it.",
          path.string()));
    }

    std::filesystem::create_directories(path);
    try {
      if (cfg.tuned)
        validateTunedPath(path);
      locator = std::make_shared<Locator>(path.string());
      location = Location::make(Mode::LIVE, Category::MARKETDATA, "bench",
                                "journal-handler-p99", locator);
    } catch (...) {
      if (!keep_files) {
        std::error_code ignored;
        std::filesystem::remove_all(path, ignored);
      }
      throw;
    }
  }

  ~BenchmarkFixture() {
    if (!keep_files) {
      std::error_code ignored;
      std::filesystem::remove_all(path, ignored);
    }
  }

  std::filesystem::path path;
  bool keep_files{false};
  ILocatorPtr locator;
  LocationPtr location;
};

std::uint64_t pseudoRandom(std::uint64_t value) {
  value ^= value >> 30U;
  value *= 0xbf58476d1ce4e5b9ULL;
  value ^= value >> 27U;
  value *= 0x94d049bb133111ebULL;
  return value ^ (value >> 31U);
}

MsgType messageTypeFor(std::uint64_t sequence) {
  static constexpr std::array<MsgType, 4> types{
      MsgType::Trade,
      MsgType::BookTop20,
      MsgType::BBO,
      MsgType::BookReset,
  };
  return types[pseudoRandom(sequence) & 3U];
}

template <typename T>
void writeTypedFrame(Appender<> &appender, std::int64_t gen_time,
                     MsgType msg_type, std::uint64_t sequence) {
  auto &message = appender.openData<T>(gen_time, msg_type);
  message = T{
      .sequence = sequence,
      .value0 = sequence ^ 0x9e3779b97f4a7c15ULL,
      .value1 = sequence + 0xbf58476d1ce4e5b9ULL,
      .value2 = sequence * 0x94d049bb133111ebULL,
  };
  appender.closeData<T>();
}

// Writes one frame of the given payload size, dispatching on the message kind so
// the four tags become four distinct types (realistic typed dispatch).
template <std::size_t Bytes>
void writeFrameSized(Appender<> &appender, std::uint64_t sequence,
                     std::int64_t gen_time) {
  const auto msg_type = messageTypeFor(sequence);
  switch (msg_type) {
  case MsgType::Trade:
    return writeTypedFrame<BenchMessage<0, Bytes>>(appender, gen_time, msg_type,
                                                   sequence);
  case MsgType::BookTop20:
    return writeTypedFrame<BenchMessage<1, Bytes>>(appender, gen_time, msg_type,
                                                   sequence);
  case MsgType::BBO:
    return writeTypedFrame<BenchMessage<2, Bytes>>(appender, gen_time, msg_type,
                                                   sequence);
  case MsgType::BookReset:
    return writeTypedFrame<BenchMessage<3, Bytes>>(appender, gen_time, msg_type,
                                                   sequence);
  default:
    throw std::runtime_error("unsupported benchmark message type");
  }
}

// gen_time is the message's intended (scheduled) send time when paced, so the
// consumer measures response time against the schedule rather than service time
// from the actual departure. See the header comment on coordinated omission.
// payload_bytes selects a compile-time message size (uniform within a run).
void writeFrame(Appender<> &appender, std::uint64_t sequence,
                std::int64_t gen_time, std::size_t payload_bytes) {
  switch (payload_bytes) {
  case 32:
    return writeFrameSized<32>(appender, sequence, gen_time);
  case 64:
    return writeFrameSized<64>(appender, sequence, gen_time);
  case 256:
    return writeFrameSized<256>(appender, sequence, gen_time);
  case 1024:
    return writeFrameSized<1024>(appender, sequence, gen_time);
  default:
    throw std::runtime_error("unsupported payload_bytes");
  }
}

void prepareLatencyBuffer(std::vector<std::int64_t> &latencies,
                          std::uint64_t samples, bool prefault);

class HandlerCollector {
public:
  HandlerCollector(std::uint64_t warmup_messages, std::uint64_t messages,
                   bool prefault_latencies)
      : warmup_messages_(warmup_messages),
        prefault_latencies_(prefault_latencies) {
    prepareLatencyBuffer(latencies_, messages, prefault_latencies_);
  }

  template <typename T> void handle(const T &message, const Frame &frame) {
    checksum_ = (checksum_ << 7U) ^ (checksum_ >> 3U) ^ message.sequence ^
                message.value0 ^ message.value1 ^ message.value2;
    std::atomic_signal_fence(std::memory_order_seq_cst);

    if (handled_messages_ >= warmup_messages_) {
      // Clamp into [0, 1s] and record every measured sample — never drop one.
      // The comparative Chronicle/Aeron drivers clamp identically; dropping
      // out-of-range samples here instead would silently shed tail entries the
      // JVM drivers keep, biasing revoq's tail downward under a saturated run.
      auto latency = Timestamp::now().nsec() - frame.getGenTime();
      if (latency < 0)
        latency = 0;
      else if (latency > 1'000'000'000LL)
        latency = 1'000'000'000LL;
      if (prefault_latencies_) {
        if (latency_count_ < latencies_.size())
          latencies_[latency_count_++] = latency;
      } else {
        latencies_.push_back(latency);
      }
    }
    ++handled_messages_;
  }

  [[nodiscard]] std::uint64_t handledMessages() const {
    return handled_messages_;
  }

  [[nodiscard]] std::uint64_t checksum() const { return checksum_; }

  std::vector<std::int64_t> takeLatencies() {
    if (prefault_latencies_)
      latencies_.resize(latency_count_);
    return std::move(latencies_);
  }

private:
  const std::uint64_t warmup_messages_;
  const bool prefault_latencies_;
  std::vector<std::int64_t> latencies_;
  std::uint64_t handled_messages_{0};
  std::size_t latency_count_{0};
  std::uint64_t checksum_{0};
};

class StaticDispatcher {
public:
  StaticDispatcher(HandlerCollector &collector, std::size_t payload_bytes)
      : collector_(collector), payload_bytes_(payload_bytes) {}

  void operator()(MergingCursor::FrameView current) {
    switch (payload_bytes_) {
    case 32:
      return dispatch<32>(current);
    case 64:
      return dispatch<64>(current);
    case 256:
      return dispatch<256>(current);
    case 1024:
      return dispatch<1024>(current);
    default:
      throw std::runtime_error("unsupported payload_bytes");
    }
  }

private:
  template <std::size_t Bytes>
  void dispatch(MergingCursor::FrameView current) {
    switch (static_cast<MsgType>(current.msg_type)) {
    case MsgType::Trade:
      return handle<BenchMessage<0, Bytes>>(current);
    case MsgType::BookTop20:
      return handle<BenchMessage<1, Bytes>>(current);
    case MsgType::BBO:
      return handle<BenchMessage<2, Bytes>>(current);
    case MsgType::BookReset:
      return handle<BenchMessage<3, Bytes>>(current);
    case MsgType::SegmentEnd:
      return;
    default:
      throw std::runtime_error("unexpected frame type in benchmark");
    }
  }

  template <typename T> void handle(MergingCursor::FrameView current) {
    if (current->getDataLength(current.frame_length) != sizeof(T)) {
      throw std::runtime_error("unexpected payload size in benchmark");
    }
    collector_.handle(current->data<T>(), *current);
  }

  HandlerCollector &collector_;
  std::size_t payload_bytes_;
};

std::int64_t percentile(const std::vector<std::int64_t> &values,
                        std::uint64_t numerator, std::uint64_t denominator) {
  if (values.empty())
    return 0;
  std::uint64_t index = (static_cast<std::uint64_t>(values.size()) * numerator +
                         denominator - 1) /
                            denominator -
                        1;
  index = std::min<std::uint64_t>(index, values.size() - 1);
  return values[static_cast<std::size_t>(index)];
}

LatencyStats computeStats(std::vector<std::int64_t> values) {
  LatencyStats stats{};
  if (values.empty())
    return stats;

  std::sort(values.begin(), values.end());
  stats.samples = values.size();
  stats.min_ns = values.front();
  stats.p50_ns = percentile(values, 50, 100);
  stats.p90_ns = percentile(values, 90, 100);
  stats.p99_ns = percentile(values, 99, 100);
  stats.p999_ns = percentile(values, 999, 1000);
  stats.p9999_ns = percentile(values, 9999, 10000);
  stats.p99999_ns = percentile(values, 99999, 100000);
  stats.max_ns = values.back();
  const auto sum = std::accumulate(values.begin(), values.end(), 0.0L);
  stats.mean_ns = static_cast<double>(sum / values.size());
  return stats;
}

// Encode the full latency distribution as an HdrHistogram base64 "V2" string —
// the same lossless, standard interchange format Chronicle/Aeron emit — so a
// reader can recompute any percentile and compare across systems. Range and
// precision (1 ns .. 1 s, 3 significant figures) match the collector's clamp.
std::string encodeLatencyHistogram(const std::vector<std::int64_t> &values) {
  hdr_histogram *hist = nullptr;
  if (hdr_init(1, 1'000'000'000LL, 3, &hist) != 0 || hist == nullptr)
    return {};
  for (const auto v : values)
    hdr_record_value(hist, v < 0 ? 0 : v);

  char *encoded = nullptr;
  std::string out;
  if (hdr_log_encode(hist, &encoded) == 0 && encoded != nullptr)
    out.assign(encoded);
  std::free(encoded);
  hdr_close(hist);
  return out;
}

void prepareLatencyBuffer(std::vector<std::int64_t> &latencies,
                          std::uint64_t samples, bool prefault) {
  const auto count = static_cast<std::size_t>(samples);
  if (!prefault) {
    latencies.reserve(count);
    return;
  }

  latencies.resize(count);
  if (latencies.empty())
    return;

#ifdef __linux__
  const std::size_t bytes = latencies.size() * sizeof(latencies[0]);
  const long sys_page_size = sysconf(_SC_PAGESIZE);
  const auto page_size =
      static_cast<std::uintptr_t>(sys_page_size > 0 ? sys_page_size : 4096);
  const auto begin = reinterpret_cast<std::uintptr_t>(latencies.data());
  const auto end = begin + bytes;
  const auto aligned_begin = begin - (begin % page_size);
  const auto aligned_end = ((end + page_size - 1) / page_size) * page_size;
  const auto advise_size =
      static_cast<std::size_t>(aligned_end - aligned_begin);
  if (madvise(reinterpret_cast<void *>(aligned_begin), advise_size,
              MADV_NOHUGEPAGE) != 0) {
    fmt::print(stderr, "cursor: MADV_NOHUGEPAGE latency buffer failed: {}\n",
               strerror(errno));
  }
#endif

  constexpr std::size_t kStride = 4096 / sizeof(std::int64_t);
  for (std::size_t i = 0; i < latencies.size(); i += kStride)
    latencies[i] = 0;
  latencies.back() = 0;

#ifdef __linux__
  if (mlock(latencies.data(), bytes) != 0) {
    fmt::print(stderr, "cursor: mlock latency buffer failed: {}\n",
               strerror(errno));
  }
#endif
}

enum class BenchmarkRole { Appender, Cursor };

const char *roleName(BenchmarkRole role) {
  return role == BenchmarkRole::Appender ? "appender" : "cursor";
}

int roleCpu(const BenchmarkConfig &cfg, BenchmarkRole role) {
  return role == BenchmarkRole::Appender ? cfg.appender_cpu : cfg.cursor_cpu;
}

void applyTunedSetup(const BenchmarkConfig &cfg, BenchmarkRole role) {
  if (!cfg.tuned)
    return;

  const int cpu = roleCpu(cfg, role);
  if (!revobase::system::CpuAffinity::pinToCore(cpu)) {
    throw std::runtime_error(
        fmt::format("{} failed to pin to CPU {}", roleName(role), cpu));
  }

  revobase::system::RealtimePriority::prefaultStack();
  if (!revobase::system::RealtimePriority::lockCurrentMemory()) {
    throw std::runtime_error(fmt::format(
        "{} failed to lock current memory; check ulimit -l", roleName(role)));
  }
  if (!revobase::system::RealtimePriority::setRealtimePriority(
          cfg.rt_priority, revobase::system::SchedulerPolicy::FIFO)) {
    throw std::runtime_error(fmt::format(
        "{} failed to set real-time priority {}; run as root or grant "
        "CAP_SYS_NICE",
        roleName(role), cfg.rt_priority));
  }
}

AppenderOptions appenderOptions(const BenchmarkConfig &cfg) {
  if (!cfg.tuned) {
    return AppenderOptions{
        .prefault = false,
        .warmup_segments = 0,
        .warmer_cpu_core = -1,
        .background_threads = false,
        .retained_segment_limit = 2,
    };
  }

  return AppenderOptions{
      .prefault = true,
      .warmup_segments = kTunedAppenderWarmupSegments,
      .warmer_cpu_core = cfg.appender_warmer_cpu,
      .background_threads = true,
      .retained_segment_limit = kTunedAppenderRetainedSegments,
  };
}

MergingCursorOptions cursorOptions(const BenchmarkConfig &cfg) {
  if (!cfg.tuned) {
    return MergingCursorOptions{
        .prefault = false,
        .background_threads = false,
        .retained_segment_limit = 0,
        .warmer_timeout_ms = 0,
        .warmer_cpu = -1,
    };
  }

  return MergingCursorOptions{
      .prefault = false,
      .background_threads = true,
      .retained_segment_limit = 0,
      .warmer_timeout_ms = 5000,
      .warmer_cpu = cfg.cursor_warmer_cpu,
  };
}

// window_ns_out, if non-null, receives the wall-clock nanoseconds spent sending
// the measured (post-warmup) messages. achieved_rate = messages / window; for a
// paced run it equals the offered rate unless the system saturated (the pacer
// stops waiting and sends fall behind schedule), which is exactly the signal
// needed to flag saturation.
void runAppender(Appender<> &appender, const BenchmarkConfig &cfg,
               const std::atomic<bool> &cursor_ready,
               const std::atomic<bool> &stop_requested,
               std::int64_t *window_ns_out = nullptr) {
  while (!cursor_ready.load(std::memory_order_acquire) &&
         !stop_requested.load(std::memory_order_acquire)) {
    PORTABLE_PAUSE();
  }
  if (stop_requested.load(std::memory_order_acquire))
    return;

  const auto total_messages = cfg.warmup_messages + cfg.messages;
  std::int64_t measured_start_ns = 0;
  auto next_write_time = Timestamp::now().nsec();
  for (std::uint64_t i = 0; i < total_messages; ++i) {
    if (stop_requested.load(std::memory_order_acquire))
      return;
    // Stamp the frame with its intended (scheduled) departure time when paced,
    // so a late producer shows up in the consumer's tail instead of being
    // silently erased (coordinated omission). Unpaced runs have no schedule, so
    // the actual send time is the honest stamp.
    std::int64_t gen_time;
    if (cfg.interval_ns > 0) {
      gen_time = next_write_time;
      while (Timestamp::now().nsec() < next_write_time &&
             !stop_requested.load(std::memory_order_acquire)) {
        PORTABLE_PAUSE();
      }
      next_write_time += static_cast<std::int64_t>(cfg.interval_ns);
    } else {
      gen_time = Timestamp::now().nsec();
    }
    if (i == cfg.warmup_messages)
      measured_start_ns = Timestamp::now().nsec();
    writeFrame(appender, i, gen_time, cfg.payload_bytes);
  }
  if (window_ns_out != nullptr && cfg.messages > 0)
    *window_ns_out = Timestamp::now().nsec() - measured_start_ns;
}

BenchmarkResult runCursor(const LocationPtr &location,
                          const BenchmarkConfig &cfg,
                          std::atomic<bool> &cursor_ready,
                          const std::atomic<bool> &appender_done,
                          const std::atomic<bool> &stop_requested,
                          std::string &hdr_out) {
  applyTunedSetup(cfg, BenchmarkRole::Cursor);
  MergingCursor cursor(cursorOptions(cfg));
  cursor.join(location, kDestId, 0);

  HandlerCollector collector(cfg.warmup_messages, cfg.messages, cfg.tuned);
  StaticDispatcher dispatcher(collector, cfg.payload_bytes);
  const auto total_messages = cfg.warmup_messages + cfg.messages;
  cursor_ready.store(true, std::memory_order_release);

  while (collector.handledMessages() < total_messages) {
    if (cursor.nextStatic(dispatcher))
      continue;
    if (appender_done.load(std::memory_order_acquire) ||
        stop_requested.load(std::memory_order_acquire)) {
      throw std::runtime_error("cursor stopped before consuming all messages");
    }
    PORTABLE_PAUSE();
  }

  auto latencies = collector.takeLatencies();
  hdr_out = encodeLatencyHistogram(latencies);
  BenchmarkResult result{
      .latency = computeStats(std::move(latencies)),
      .handled_messages = collector.handledMessages(),
      .checksum = collector.checksum(),
  };
  return result;
}

#ifdef __linux__
void closeFd(int &fd) noexcept {
  if (fd >= 0) {
    close(fd);
    fd = -1;
  }
}

bool writeAll(int fd, const void *data, std::size_t size) {
  const auto *bytes = static_cast<const char *>(data);
  std::size_t written = 0;
  while (written < size) {
    const ssize_t n = write(fd, bytes + written, size - written);
    if (n < 0 && errno == EINTR)
      continue;
    if (n <= 0)
      return false;
    written += static_cast<std::size_t>(n);
  }
  return true;
}

bool readAll(int fd, void *data, std::size_t size) {
  auto *bytes = static_cast<char *>(data);
  std::size_t received = 0;
  while (received < size) {
    const ssize_t n = read(fd, bytes + received, size - received);
    if (n < 0 && errno == EINTR)
      continue;
    if (n <= 0)
      return false;
    received += static_cast<std::size_t>(n);
  }
  return true;
}

void writeSignal(int fd) {
  const char ready = 'R';
  if (!writeAll(fd, &ready, sizeof(ready)))
    throw std::runtime_error("failed to write pipe signal");
}

bool readSignal(int fd) {
  char ready = 0;
  return readAll(fd, &ready, sizeof(ready)) && ready == 'R';
}

template <typename T> void writePipeValue(int fd, const T &value) {
  static_assert(std::is_trivially_copyable_v<T>);
  if (!writeAll(fd, &value, sizeof(value)))
    throw std::runtime_error("failed to write pipe value");
}

template <typename T> T readPipeValue(int fd) {
  static_assert(std::is_trivially_copyable_v<T>);
  T value{};
  if (!readAll(fd, &value, sizeof(value)))
    throw std::runtime_error("failed to read pipe value");
  return value;
}

void writePipeString(int fd, const std::string &value) {
  writePipeValue(fd, static_cast<std::uint64_t>(value.size()));
  if (!value.empty() && !writeAll(fd, value.data(), value.size()))
    throw std::runtime_error("failed to write pipe string");
}

std::string readPipeString(int fd) {
  const auto size = readPipeValue<std::uint64_t>(fd);
  std::string value(static_cast<std::size_t>(size), '\0');
  if (size != 0 && !readAll(fd, value.data(), value.size()))
    throw std::runtime_error("failed to read pipe string");
  return value;
}

BenchmarkResult runCursorProcess(const LocationPtr &location,
                                 const BenchmarkConfig &cfg, int ready_fd,
                                 std::string &hdr_out) {
  applyTunedSetup(cfg, BenchmarkRole::Cursor);
  MergingCursor cursor(cursorOptions(cfg));
  cursor.join(location, kDestId, 0);

  HandlerCollector collector(cfg.warmup_messages, cfg.messages, cfg.tuned);
  StaticDispatcher dispatcher(collector, cfg.payload_bytes);
  const auto total_messages = cfg.warmup_messages + cfg.messages;
  writeSignal(ready_fd);
  closeFd(ready_fd);

  std::uint64_t no_data_spins = 0;
  constexpr std::uint64_t kMaxNoDataSpins = 500'000'000;
  while (collector.handledMessages() < total_messages) {
    if (cursor.nextStatic(dispatcher)) {
      no_data_spins = 0;
      continue;
    }
    if (++no_data_spins >= kMaxNoDataSpins) {
      throw std::runtime_error("cursor timed out waiting for messages");
    }
    PORTABLE_PAUSE();
  }

  auto latencies = collector.takeLatencies();
  hdr_out = encodeLatencyHistogram(latencies);
  return BenchmarkResult{
      .latency = computeStats(std::move(latencies)),
      .handled_messages = collector.handledMessages(),
      .checksum = collector.checksum(),
  };
}

BenchmarkOutput runForkedBenchmark(const BenchmarkConfig &cfg) {
  BenchmarkFixture fixture(cfg);

  int start_pipe[2] = {-1, -1};
  int ready_pipe[2] = {-1, -1};
  int result_pipe[2] = {-1, -1};
  if (pipe(start_pipe) != 0 || pipe(ready_pipe) != 0 ||
      pipe(result_pipe) != 0) {
    closeFd(start_pipe[0]);
    closeFd(start_pipe[1]);
    closeFd(ready_pipe[0]);
    closeFd(ready_pipe[1]);
    closeFd(result_pipe[0]);
    closeFd(result_pipe[1]);
    throw std::runtime_error(fmt::format("pipe failed: {}", strerror(errno)));
  }

  std::fflush(stdout);
  const pid_t pid = fork();
  if (pid < 0) {
    closeFd(start_pipe[0]);
    closeFd(start_pipe[1]);
    closeFd(ready_pipe[0]);
    closeFd(ready_pipe[1]);
    closeFd(result_pipe[0]);
    closeFd(result_pipe[1]);
    throw std::runtime_error(fmt::format("fork failed: {}", strerror(errno)));
  }

  if (pid == 0) {
    closeFd(start_pipe[1]);
    closeFd(ready_pipe[0]);
    closeFd(result_pipe[0]);
    try {
      if (!readSignal(start_pipe[0]))
        throw std::runtime_error("cursor did not receive appender-ready signal");
      closeFd(start_pipe[0]);
      std::string hdr_b64;
      const auto result =
          runCursorProcess(fixture.location, cfg, ready_pipe[1], hdr_b64);
      writePipeValue(result_pipe[1], result);
      writePipeString(result_pipe[1], hdr_b64);
      closeFd(result_pipe[1]);
      std::fflush(stdout);
      std::fflush(stderr);
      _exit(0);
    } catch (const std::exception &e) {
      fmt::print(stderr, "cursor error: {}\n", e.what());
      std::fflush(stderr);
      _exit(1);
    }
  }

  closeFd(start_pipe[0]);
  closeFd(ready_pipe[1]);
  closeFd(result_pipe[1]);

  bool child_reaped = false;
  try {
    applyTunedSetup(cfg, BenchmarkRole::Appender);
    Appender<> appender(fixture.location, kDestId, appenderOptions(cfg));
    writeSignal(start_pipe[1]);
    closeFd(start_pipe[1]);
    if (!readSignal(ready_pipe[0]))
      throw std::runtime_error("cursor did not signal ready");
    closeFd(ready_pipe[0]);

    std::atomic<bool> cursor_ready{true};
    std::atomic<bool> stop_requested{false};
    std::int64_t window_ns = 0;
    runAppender(appender, cfg, cursor_ready, stop_requested, &window_ns);

    // Drain the result pipe BEFORE reaping the child. The child blocks writing
    // the variable-length histogram (hdr_b64) until we read it, so waiting on
    // the child first would deadlock whenever hdr_b64 exceeds the pipe buffer
    // (~64 KB): child stuck in writeAll(), parent stuck in waitpid().
    auto result = readPipeValue<BenchmarkResult>(result_pipe[0]);
    auto hdr_b64 = readPipeString(result_pipe[0]);
    closeFd(result_pipe[0]);

    int status = 0;
    if (waitpid(pid, &status, 0) < 0) {
      throw std::runtime_error(
          fmt::format("waitpid failed: {}", strerror(errno)));
    }
    child_reaped = true;
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
      throw std::runtime_error("cursor process failed");
    }
    if (cfg.keep_files)
      fmt::print("journal_path={}\n", fixture.path.string());
    const double achieved_mps =
        window_ns > 0
            ? static_cast<double>(cfg.messages) * 1e9 / static_cast<double>(window_ns)
            : 0.0;
    return BenchmarkOutput{.result = result,
                           .hdr_b64 = std::move(hdr_b64),
                           .achieved_mps = achieved_mps};
  } catch (...) {
    closeFd(start_pipe[1]);
    closeFd(ready_pipe[0]);
    closeFd(result_pipe[0]);
    if (!child_reaped) {
      kill(pid, SIGTERM);
      waitpid(pid, nullptr, 0);
    }
    throw;
  }
}
#endif

BenchmarkOutput runBenchmark(const BenchmarkConfig &cfg) {
#ifdef __linux__
  if (cfg.tuned && cfg.fork_cursor)
    return runForkedBenchmark(cfg);
#endif

  BenchmarkFixture fixture(cfg);

  if (cfg.tuned)
    applyTunedSetup(cfg, BenchmarkRole::Appender);
  Appender<> appender(fixture.location, kDestId, appenderOptions(cfg));

  std::atomic<bool> cursor_ready{false};
  std::atomic<bool> appender_done{false};
  std::atomic<bool> stop_requested{false};
  std::exception_ptr appender_error;
  std::exception_ptr cursor_error;
  BenchmarkResult result;
  std::string hdr_b64;
  std::int64_t window_ns = 0;

  std::thread cursor_thread([&] {
    try {
      result = runCursor(fixture.location, cfg, cursor_ready, appender_done,
                         stop_requested, hdr_b64);
    } catch (...) {
      cursor_error = std::current_exception();
      stop_requested.store(true, std::memory_order_release);
      cursor_ready.store(true, std::memory_order_release);
    }
  });

  std::thread appender_thread([&] {
    try {
      runAppender(appender, cfg, cursor_ready, stop_requested, &window_ns);
    } catch (...) {
      appender_error = std::current_exception();
      stop_requested.store(true, std::memory_order_release);
    }
    appender_done.store(true, std::memory_order_release);
  });

  appender_thread.join();
  cursor_thread.join();

  if (appender_error)
    std::rethrow_exception(appender_error);
  if (cursor_error)
    std::rethrow_exception(cursor_error);

  if (cfg.keep_files)
    fmt::print("journal_path={}\n", fixture.path.string());

  const double achieved_mps =
      window_ns > 0
          ? static_cast<double>(cfg.messages) * 1e9 / static_cast<double>(window_ns)
          : 0.0;
  return BenchmarkOutput{.result = result,
                         .hdr_b64 = std::move(hdr_b64),
                         .achieved_mps = achieved_mps};
}

void printSummary(const BenchmarkConfig &cfg, const BenchmarkOutput &output) {
  const auto &result = output.result;
  const auto &s = result.latency;
  const double rate_mps = cfg.interval_ns == 0
                              ? 0.0
                              : 1000.0 / static_cast<double>(cfg.interval_ns);

  fmt::print("benchmark=revoq_appender_to_handler_p99\n");
  fmt::print("mode={}\n", cfg.tuned ? "tuned" : "portable");
  if (cfg.tuned) {
    fmt::print("path={} appender_cpu={} cursor_cpu={} "
               "appender_warmer_cpu={} cursor_warmer_cpu={} "
               "rt_priority={} fork={}\n",
               cfg.path.string(), cfg.appender_cpu, cfg.cursor_cpu,
               cfg.appender_warmer_cpu, cfg.cursor_warmer_cpu, cfg.rt_priority,
               cfg.fork_cursor ? "yes" : "no");
  }
  fmt::print("messages={} warmup={} interval_ns={}", cfg.messages,
             cfg.warmup_messages, cfg.interval_ns);
  if (cfg.interval_ns > 0)
    fmt::print(" rate_mps={:.3f}", rate_mps);
  fmt::print("\n");
  fmt::print("payload_bytes={} message_types=4 dispatch=static_switch\n",
             cfg.payload_bytes);
  fmt::print("samples={} min_ns={} p50_ns={} p90_ns={} p99_ns={} p999_ns={} "
             "p9999_ns={} p99999_ns={} max_ns={} mean_ns={:.1f}\n",
             s.samples, s.min_ns, s.p50_ns, s.p90_ns, s.p99_ns, s.p999_ns,
             s.p9999_ns, s.p99999_ns, s.max_ns, s.mean_ns);
  fmt::print("handled_messages={} checksum={}\n", result.handled_messages,
             result.checksum);
  // Producer's sustained send rate over the measured window. For a paced run,
  // achieved < offered means the system saturated (the pacer could not keep the
  // schedule); for an unpaced run this is the raw throughput.
  fmt::print("achieved_mps={:.1f}\n", output.achieved_mps);
  // HdrHistogram base64 "V2" encoding of the full latency distribution: the
  // lossless, standard interchange format for cross-system comparison. Decode
  // with any HdrHistogram tooling to recompute arbitrary percentiles.
  if (!output.hdr_b64.empty())
    fmt::print("latency_hdr_b64={}\n", output.hdr_b64);
}

} // namespace

int main(int argc, char **argv) {
  try {
    spdlog::set_level(spdlog::level::warn);

    const auto cfg = parseArgs(argc, argv);
    const auto output = runBenchmark(cfg);
    printSummary(cfg, output);
    return 0;
  } catch (const std::exception &e) {
    fmt::print(stderr, "error: {}\n", e.what());
    printUsage(argv[0]);
    return 1;
  }
}

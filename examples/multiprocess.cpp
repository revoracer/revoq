// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 RevoRacer
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <stdexcept>

#ifdef __linux__
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#endif

#include <spdlog/spdlog.h>

#include <revoq/StreamBase.h>
#include <revoq/MergingCursor.h>
#include <revoq/Appender.h>

namespace fs = std::filesystem;
using namespace racer::revoq;

namespace {

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

template <typename T>
void publish(Appender<> &appender, std::int64_t timestamp, MsgType msg_type,
             const T &message) {
  auto &slot = appender.openData<T>(timestamp, msg_type);
  slot = message;
  appender.closeData<T>();
}

#ifdef __linux__
double elapsedSeconds(const timespec &start) {
  timespec now{};
  clock_gettime(CLOCK_MONOTONIC, &now);
  return static_cast<double>(now.tv_sec - start.tv_sec) +
         static_cast<double>(now.tv_nsec - start.tv_nsec) * 1e-9;
}

void writeSignal(int fd) {
  const char value = 1;
  if (write(fd, &value, sizeof(value)) != sizeof(value))
    _exit(10);
}

bool readSignal(int fd) {
  char value = 0;
  return read(fd, &value, sizeof(value)) == sizeof(value) && value == 1;
}
#endif

} // namespace

int main(int argc, char **argv) {
#ifndef __linux__
  (void)argc;
  (void)argv;
  std::cerr << "multiprocess example requires Linux\n";
  return 1;
#else
  try {
    spdlog::set_level(spdlog::level::warn);

    int messages = 1000;
    for (int i = 1; i < argc; ++i) {
      const std::string arg = argv[i];
      if (arg == "--messages" && i + 1 < argc) {
        messages = std::stoi(argv[++i]);
        continue;
      }
      std::cerr << "Usage: " << argv[0] << " [--messages N]\n";
      return 2;
    }

    TempDir dir("multiprocess");
    auto locator = std::make_shared<Locator>(dir.string());
    auto location = Location::make(Mode::LIVE, Category::EXECUTION, "examples",
                                   "multiprocess", locator);
    constexpr std::uint32_t dest_id = 0;

    Appender<> appender(
        location, dest_id,
        AppenderOptions{.prefault = false, .background_threads = false});

    int ready_pipe[2] = {-1, -1};
    if (pipe(ready_pipe) != 0)
      throw std::runtime_error("pipe failed");

    const pid_t pid = fork();
    if (pid < 0)
      throw std::runtime_error("fork failed");

    if (pid == 0) {
      close(ready_pipe[0]);

      MergingCursor cursor(
          MergingCursorOptions{.prefault = false, .background_threads = false});
      cursor.join(location, dest_id, 0);
      writeSignal(ready_pipe[1]);
      close(ready_pipe[1]);

      timespec start{};
      clock_gettime(CLOCK_MONOTONIC, &start);
      int count = 0;
      std::int64_t prev_sequence = -1;

      while (count < messages) {
        if (elapsedSeconds(start) > 10.0)
          _exit(11);

        const auto current = cursor.tryCurrentFrame();
        if (!current)
          continue;

        const auto sequence = static_cast<std::int64_t>(current->getSequence());
        if (sequence != prev_sequence + 1)
          _exit(12);
        prev_sequence = sequence;

        if (static_cast<MsgType>(current.msg_type) == MsgType::Trade)
          ++count;

        cursor.advance(current);
      }

      _exit(0);
    }

    close(ready_pipe[1]);
    if (!readSignal(ready_pipe[0]))
      throw std::runtime_error("child cursor did not signal ready");
    close(ready_pipe[0]);

    for (int i = 0; i < messages; ++i) {
      const auto ts = static_cast<std::int64_t>(i + 1);
      publish(appender, ts, MsgType::Trade,
              Trade{ts, 101, 100.0 + static_cast<double>(i), 10});
    }

    int status = 0;
    if (waitpid(pid, &status, 0) != pid)
      throw std::runtime_error("waitpid failed");
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0)
      throw std::runtime_error("child cursor verification failed");

    std::cout << "ok multiprocess messages=" << messages << '\n';
    return 0;
  } catch (const std::exception &e) {
    std::cerr << "error: " << e.what() << '\n';
    return 1;
  }
#endif
}

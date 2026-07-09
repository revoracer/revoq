// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 RevoRacer
#include "revoq/TimeUtils.h"

#include <chrono>
#include <iomanip>
#include <sstream>

#ifdef __APPLE__
#include <time.h>
#endif

namespace racer {

Timestamp Timestamp::now() noexcept { return TimeUtils::now(); }

Timestamp TimeUtils::now() noexcept {
#ifdef __APPLE__
  return Timestamp(clock_gettime_nsec_np(CLOCK_REALTIME));
#else
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  return Timestamp(static_cast<uint64_t>(ts.tv_sec) * 1'000'000'000ULL +
                   ts.tv_nsec);
#endif
}

TimeUtils::TimeUtils() {
  using namespace std::chrono;

  // Initialize both time references atomically
  //  start_time_steady_ = steady_clock::now().time_since_epoch().count();
  //  start_time_since_epoch_ = duration_cast<nanoseconds>(
  //          system_clock::now().time_since_epoch()).count();
}

const TimeUtils &TimeUtils::getInstance() noexcept {
  // Thread-safe static initialization (C++11 magic statics)
  static TimeUtils t;
  return t;
}

std::string TimeUtils::toString(const Timestamp &ts) {
  static constexpr auto NANOSECONDS_PER_SECOND = 1'000'000'000LL;

  char buf[64]{0};
  auto seconds = static_cast<time_t>(ts.nsec() / NANOSECONDS_PER_SECOND);
  struct tm tm_time{};
  gmtime_r(&seconds, &tm_time);
  int nano = static_cast<int>(ts.nsec() % NANOSECONDS_PER_SECOND);

  snprintf(buf, sizeof(buf), "%4d-%02d-%02d %02d:%02d:%02d.%09d",
           tm_time.tm_year + 1900, tm_time.tm_mon + 1, tm_time.tm_mday,
           tm_time.tm_hour, tm_time.tm_min, tm_time.tm_sec, nano);
  return buf;
}

std::string Timestamp::toString(const Timestamp &ts) {
  return TimeUtils::toString(ts);
}

std::string Timestamp::toDateString(const Timestamp &ts) {
  static constexpr auto NANOSECONDS_PER_SECOND = 1'000'000'000LL;

  char buf[64]{0};
  auto seconds = static_cast<time_t>(ts.nsec() / NANOSECONDS_PER_SECOND);
  struct tm tm_time{};
  gmtime_r(&seconds, &tm_time);

  snprintf(buf, sizeof(buf), "%4d-%02d-%02d", tm_time.tm_year + 1900,
           tm_time.tm_mon + 1, tm_time.tm_mday);
  return buf;
}

std::size_t Timestamp::toHHMMSS(const Timestamp &ts) {
  const auto s = TimeUtils::toString(ts);
  // Assuming format is fixed: YYYY-MM-DD HH:MM:SS.nnnnnnnnn
  // Extract HH:MM:SS and remove colons
  std::string hhmmss = s.substr(11, 2) + s.substr(14, 2) + s.substr(17, 2);
  return std::stoul(hhmmss);
}

Timestamp Timestamp::fromYYYYmmddHHMMSS(const std::string &str_date_time) {
  if (str_date_time.size() != LEN_YYYY_MM_DD_HH_MM_SS) {
    throw std::runtime_error("invalid datetime format: " + str_date_time);
  }

  std::tm timeinfo{};
  std::istringstream ss(str_date_time);
  ss >> std::get_time(&timeinfo, "%Y-%m-%d %H:%M:%S");

  if (ss.fail()) {
    throw std::runtime_error("failed to parse datetime: " + str_date_time);
  }

  std::time_t epochTime = timegm(&timeinfo);
  return Timestamp(epochTime * 1'000'000'000LL);
}

Timestamp Timestamp::fromYYYYmmdd(const std::string &str_date) {
  if (str_date.size() != LEN_YYYY_MM_DD) {
    throw std::runtime_error("invalid date format: " + str_date);
  }
  return Timestamp::fromYYYYmmddHHMMSS(str_date + " 00:00:00");
}

} // namespace racer

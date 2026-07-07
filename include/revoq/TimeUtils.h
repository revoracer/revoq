// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 RevoRacer
#pragma once

#include "Typedef.h"
#include <cstdint>
#include <string>

#include <fmt/core.h>

namespace racer {

class Duration;

class Timestamp {
public:
  static inline constexpr std::size_t LEN_YYYY_MM_DD{10};
  static inline constexpr std::size_t LEN_YYYY_MM_DD_HH_MM_SS{19};

  constexpr Timestamp() noexcept = default;
  explicit constexpr Timestamp(std::int64_t ts) noexcept : nsec_(ts) {}

  [[nodiscard]] constexpr std::int64_t nsec() const noexcept { return nsec_; }
  [[nodiscard]] constexpr std::int64_t microsec() const noexcept {
    return static_cast<int64_t>(nsec_ / 1'000);
  }
  [[nodiscard]] constexpr std::int64_t millisec() const noexcept {
    return static_cast<int64_t>(nsec_ / 1'000'000);
  }
  [[nodiscard]] constexpr std::int64_t sec() const noexcept {
    return static_cast<int64_t>(nsec_ / 1'000'000'000);
  }

  static Timestamp now() noexcept;
  static std::string toString(const Timestamp &);
  static std::string toDateString(const Timestamp &ts);
  static std::size_t toHHMMSS(const Timestamp &ts);
  static Timestamp fromYYYYmmddHHMMSS(const std::string &str_date_time);
  static Timestamp fromYYYYmmdd(const std::string &str_date);

  static constexpr Timestamp min() noexcept { return Timestamp(0); }
  static constexpr Timestamp invalid() noexcept {
    static_assert(std::numeric_limits<std::int64_t>::min() < min().nsec_,
                  "invalid timestamp is valid!");
    return Timestamp(std::numeric_limits<std::int64_t>::min());
  }

  static constexpr Timestamp MAX_TIME() noexcept {
    return Timestamp(+4102444800000000000L);
  }

  [[nodiscard]] constexpr bool isValid() const noexcept { return nsec_ >= 0; }

  [[nodiscard]] constexpr bool operator==(const Timestamp &rhs) const noexcept {
    return nsec_ == rhs.nsec_;
  }

  [[nodiscard]] constexpr bool operator!=(const Timestamp &rhs) const noexcept {
    return nsec_ != rhs.nsec_;
  }

  [[nodiscard]] constexpr bool operator<(const Timestamp &rhs) const noexcept {
    return nsec_ < rhs.nsec_;
  }

  [[nodiscard]] constexpr bool operator<=(const Timestamp &rhs) const noexcept {
    return nsec_ <= rhs.nsec_;
  }

  [[nodiscard]] constexpr bool operator>(const Timestamp &rhs) const noexcept {
    return nsec_ > rhs.nsec_;
  }

  [[nodiscard]] constexpr bool operator>=(const Timestamp &rhs) const noexcept {
    return nsec_ >= rhs.nsec_;
  }

  friend class Duration;
  friend Timestamp operator-(const Timestamp &lhs,
                             const Duration &rhs) noexcept;
  friend Timestamp operator+(const Timestamp &lhs,
                             const Duration &rhs) noexcept;
  friend Timestamp &operator+=(Timestamp &self,
                               const Duration &duration) noexcept;
  friend Timestamp &operator-=(Timestamp &self,
                               const Duration &duration) noexcept;
  friend Duration operator-(const Timestamp &lhs,
                            const Timestamp &rhs) noexcept;

private:
  std::int64_t nsec_{};
};
HFT_POD_STRUCT(Timestamp);
static_assert(sizeof(Timestamp) == 8, "sizeof(Timestamp) != 8");

class TimeUtils {
public:
  static Timestamp now() noexcept;
  static std::string toString(const Timestamp &ts);

private:
  TimeUtils();
  static const TimeUtils &getInstance() noexcept;
};

class Duration {
public:
  constexpr Duration() noexcept : nsec_(0) {}
  constexpr Duration(const std::int64_t &nsec) noexcept : nsec_(nsec) {}

  [[nodiscard]] static constexpr Duration
  from_msec(const double &msec) noexcept {
    return Duration{static_cast<std::int64_t>(msec * 1e6)};
  }

  [[nodiscard]] static constexpr Duration
  from_nsec(const std::int64_t &nsec) noexcept {
    return Duration(nsec);
  }

  [[nodiscard]] static constexpr Duration from_sec(const double &sec) noexcept {
    return Duration(static_cast<std::int64_t>(sec * 1e9));
  }

  [[nodiscard]] static constexpr Duration
  from_minute(const double &minute) noexcept {
    return Duration(static_cast<std::int64_t>(minute * 1e9 * 60));
  }

  [[nodiscard]] static constexpr Duration
  from_hour(const double &hour) noexcept {
    return Duration(static_cast<std::int64_t>(hour * 1e9 * 3600));
  }

  [[nodiscard]] static constexpr Duration from_day(const double &day) noexcept {
    return Duration(static_cast<std::int64_t>(day * 1e9 * 3600 * 24));
  }

  [[nodiscard]] constexpr double sec() const noexcept { return nsec_ * 1e-9; }
  [[nodiscard]] constexpr double msec() const noexcept { return nsec_ * 1e-6; }
  [[nodiscard]] constexpr std::int64_t nsec() const noexcept { return nsec_; }

  [[nodiscard]] constexpr bool operator==(const Duration &rhs) const noexcept {
    return nsec_ == rhs.nsec_;
  }

  [[nodiscard]] constexpr bool operator!=(const Duration &rhs) const noexcept {
    return nsec_ != rhs.nsec_;
  }

  [[nodiscard]] constexpr bool operator<(const Duration &rhs) const noexcept {
    return nsec_ < rhs.nsec_;
  }

  [[nodiscard]] constexpr bool operator<=(const Duration &rhs) const noexcept {
    return nsec_ <= rhs.nsec_;
  }

  [[nodiscard]] constexpr bool operator>(const Duration &rhs) const noexcept {
    return nsec_ > rhs.nsec_;
  }

  [[nodiscard]] constexpr bool operator>=(const Duration &rhs) const noexcept {
    return nsec_ >= rhs.nsec_;
  }

  [[nodiscard]] constexpr Duration
  operator+(const Duration &rhs) const noexcept {
    return Duration(nsec_ + rhs.nsec_);
  }

  [[nodiscard]] constexpr Duration
  operator-(const Duration &rhs) const noexcept {
    return Duration(nsec_ - rhs.nsec_);
  }

  constexpr Duration &operator+=(const Duration &duration) noexcept {
    nsec_ += duration.nsec_;
    return *this;
  }

  constexpr Duration &operator-=(const Duration &duration) noexcept {
    nsec_ -= duration.nsec_;
    return *this;
  }

  [[nodiscard]] constexpr Duration operator-() const noexcept {
    return Duration(-nsec_);
  }

  friend class Timestamp;
  friend Timestamp operator-(const Timestamp &lhs,
                             const Duration &rhs) noexcept;
  friend Timestamp operator+(const Timestamp &lhs,
                             const Duration &rhs) noexcept;
  friend Timestamp &operator+=(Timestamp &self,
                               const Duration &duration) noexcept;
  friend Timestamp &operator-=(Timestamp &self,
                               const Duration &duration) noexcept;
  friend Duration operator-(const Timestamp &lhs,
                            const Timestamp &rhs) noexcept;

private:
  std::int64_t nsec_{};
};

inline Timestamp operator-(const Timestamp &lhs, const Duration &rhs) noexcept {
  return Timestamp(lhs.nsec_ - rhs.nsec_);
}

inline Timestamp operator+(const Timestamp &lhs, const Duration &rhs) noexcept {
  return Timestamp(lhs.nsec_ + rhs.nsec_);
}

inline Timestamp &operator+=(Timestamp &self,
                             const Duration &duration) noexcept {
  self.nsec_ += duration.nsec_;
  return self;
}

inline Timestamp &operator-=(Timestamp &self,
                             const Duration &duration) noexcept {
  self.nsec_ -= duration.nsec_;
  return self;
}

inline Duration operator-(const Timestamp &lhs, const Timestamp &rhs) noexcept {
  return Duration(lhs.nsec_ - rhs.nsec_);
}

namespace duration::literals {
constexpr Duration operator""_ns(const unsigned long long nsec) noexcept {
  return Duration::from_nsec(nsec);
}
constexpr Duration operator""_ms(const long double msec) noexcept {
  return Duration::from_msec(msec);
}
constexpr Duration operator""_ms(const unsigned long long msec) noexcept {
  return Duration::from_msec(msec);
}
constexpr Duration operator""_s(const long double sec) noexcept {
  return Duration::from_sec(sec);
}
constexpr Duration operator""_s(const unsigned long long sec) noexcept {
  return Duration::from_sec(sec);
}
constexpr Duration operator""_m(const long double minute) noexcept {
  return Duration::from_minute(minute);
}
constexpr Duration operator""_m(const unsigned long long minute) noexcept {
  return Duration::from_minute(minute);
}
constexpr Duration operator""_h(const long double hour) noexcept {
  return Duration::from_hour(hour);
}
constexpr Duration operator""_h(const unsigned long long hour) noexcept {
  return Duration::from_hour(hour);
}
constexpr Duration operator""_d(const long double day) noexcept {
  return Duration::from_day(day);
}
constexpr Duration operator""_d(const unsigned long long day) noexcept {
  return Duration::from_day(day);
}
} // namespace duration::literals

static constexpr Timestamp MAX_TIME = Timestamp::MAX_TIME();
} // namespace racer

namespace std {
template <> struct hash<racer::Timestamp> {
  size_t operator()(const racer::Timestamp &t) const noexcept {
    return hash<std::int64_t>()(t.nsec());
  }
};

template <> struct hash<racer::Duration> {
  size_t operator()(const racer::Duration &d) const noexcept {
    return hash<std::int64_t>()(d.nsec());
  }
};
} // namespace std

using namespace racer::duration::literals;

template <> struct fmt::formatter<racer::Timestamp> {
  template <typename ParseContext> constexpr auto parse(ParseContext &ctx) {
    return ctx.begin();
  }
  template <typename FormatContext>
  constexpr auto format(const racer::Timestamp &ts, FormatContext &ctx) const {
    return fmt::format_to(ctx.out(), "{}", racer::TimeUtils::toString(ts));
  }
};

template <> struct fmt::formatter<racer::Duration> {
  template <typename ParseContext> constexpr auto parse(ParseContext &ctx) {
    return ctx.begin();
  }
  template <typename FormatContext>
  constexpr auto format(const racer::Duration &d, FormatContext &ctx) const {
    return fmt::format_to(ctx.out(), "{} nano", d.nsec());
  }
};

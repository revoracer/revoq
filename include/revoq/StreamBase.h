// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 RevoRacer
#pragma once

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <type_traits>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <revobase/FileUtils.h>
#include <revobase/HashUtils.h>
#include <revobase/StringUtils.hpp>

namespace revoq {
enum class Mode : std::int8_t { UNKNOWN = 0, LIVE, REPLAY };

static const std::unordered_map<std::string, Mode> str2mode{
    {"live", Mode::LIVE}, {"replay", Mode::REPLAY}};

enum class Category : std::int8_t {
  UNKNOWN = 0,
  MARKETDATA,
  RESEARCHDATA,
  EXECUTION,
  LOG,
  SYSTEM,
  STRATEGY
};

static const std::unordered_map<std::string, Category> str2category{
    {"marketdata", Category::MARKETDATA},
    {"researchdata", Category::RESEARCHDATA},
    {"executiondata", Category::EXECUTION},
    {"log", Category::LOG},
    {"system", Category::SYSTEM},
    {"strategy", Category::STRATEGY}
  };

enum class Layout : std::int8_t { UNKNOWN = 0, STREAM, LOG };

// Instead of runtime maps, use constexpr functions
constexpr const char *categoryToStr(Category c) noexcept {
  switch (c) {
  case Category::MARKETDATA:
    return "marketdata";
  case Category::RESEARCHDATA:
    return "researchdata";
  case Category::EXECUTION:
    return "executiondata";
  case Category::LOG:
    return "log";
  case Category::SYSTEM:
    return "system";
  case Category::STRATEGY:
    return "strategy";
  default:
    return "unknown";
  }
}

constexpr const char *modeToStr(Mode m) noexcept {
  switch (m) {
  case Mode::LIVE:
    return "live";
  case Mode::REPLAY:
    return "replay";
  default:
    return "unknown";
  }
}

constexpr const char *layoutToStr(Layout l) noexcept {
  switch (l) {
  case Layout::STREAM:
    return "stream";
  case Layout::LOG:
    return "log";
  default:
    return "unknown";
  }
}

// Any 16-bit scoped enum is accepted as a frame tag.
//
// revoq stores msg_type and hands it back without interpreting it, so the
// write API is not typed to any one registry: pass revomd::MsgType for market
// data, revoq::MsgType for the session/segment markers revoq itself
// acts on, or your own enum. The constraint is what keeps a bare int or a
// wrong-width enum from silently becoming a tag.
template <class E>
concept FrameTag =
    std::is_enum_v<E> &&
    sizeof(std::underlying_type_t<E>) == sizeof(std::uint16_t);

// Frame type tag, persisted as uint16_t in FrameHeader.
//
// revoq is payload-agnostic: it moves opaque frames and never interprets a
// payload. What it declares here is only the values IT acts on — the segment
// marker its own appender/cursor logic reads. Payload registries belong to the
// schema that owns the payload; the market-data registry is revomd::MsgType
// (revomd/wire.h). A journal writer casts whichever registry it is emitting
// from into the uint16 field.
//
// NUMERIC ALLOCATION — THIS TABLE IS AN APPEND-ONLY LEDGER.
//
// Values are persisted in every capture on disk, so an allocated number can
// never be reused for a different meaning and a block can never be narrowed.
// Widen and append only. Two people independently allocating the same value
// six months apart is the failure this table exists to prevent, so claim a
// block here BEFORE writing the enum that fills it.
//
//     0            Invalid — also what a zero-filled frame decodes as
//     1-99         permanently unallocated (see below)
//     100-9999     normalized market data ......... revomd::MsgType
//     10000-10199  stream/system markers .......... this enum
//     10200-19999  execution / OMS ................ RESERVED, unallocated
//     20000-39999  strategy / risk / telemetry .... RESERVED, unallocated
//     40000-65535  reserved / private / experimental
//
// 1-99 is deliberately never allocated. A truncated or garbage frame carrying
// a small integer should fail to decode rather than land on a real type — the
// same reasoning that keeps 0 reserved-invalid in revomd::Side.
//
// Block sizes are deliberately lopsided. Market data grows slowly: each value
// is a normalized payload layout somebody must write, journal, decode and
// support forever, and 28 of them cover four venues across L1/L2/L3, feed
// health, reference data and raw preservation. Per-venue variants consume NO
// values here — they ride MsgType::NativeEvent with venue + native_template_id
// inside NativeEventHeader, which is why venue count does not enter this
// budget. Strategy/telemetry gets the largest block because it grows fastest:
// those records are cheap to add, and there is one family per strategy.
//
// The 10000-10199 block sits mid-space rather than at the top because its
// values are frozen — see below. Routing a merged stream is a range test
// either way, and system frames are rare on any hot path.
enum class MsgType : std::uint16_t {
  Invalid = 0,

  // Stream/system markers: 10000-10199.
  //
  // FROZEN. These three are the most heavily persisted values in the scheme
  // and cannot move.
  //
  // SegmentEnd is genuine transport: rotateSegment() writes it itself on every
  // rotation without any caller asking, and revoq acts on it internally (EOF
  // in Stream.h, resume-after-rotation in Appender.h, sentinel filtering in
  // MergingCursor.h). It is in every segment ever written.
  //
  // SessionStart/SessionEnd are different in kind — an APPLICATION convention
  // that happens to live in revoq's enum. revoq never interprets them; they go
  // through the generic mark() API, are written by the capture apps, and are
  // read only by tools (peek treats a missing SessionEnd as a truncated
  // capture). They stay here because they are already on disk, not because
  // transport owns them.
  SegmentEnd = 10000,
  SessionStart = 10001,
  SessionEnd = 10002,

  // 10100-10199 previously held SimulationStart / SimulationEnd. Nothing ever
  // wrote or read them, and simulation boundaries are the backtest harness's
  // vocabulary rather than the journal transport's — the same argument that
  // keeps the market-data registry out of this enum. Removed while still free;
  // the range stays open for the replay harness to claim.
};

class Location;
using LocationPtr = std::shared_ptr<Location>;
class ILocator {
public:
  ILocator() = default;
  virtual ~ILocator() = default;
  [[nodiscard]] virtual bool hasEnv(const std::string &name) const = 0;
  [[nodiscard]] virtual std::string getEnv(const std::string &name) const = 0;
  [[nodiscard]] virtual bool exists(LocationPtr location, Layout l) const = 0;
  [[nodiscard]] virtual std::filesystem::path layoutDir(LocationPtr location,
                                                        Layout l) const = 0;
  [[nodiscard]] virtual std::filesystem::path
  makeLayoutDir(LocationPtr location, Layout l) const = 0;
  [[nodiscard]] virtual std::filesystem::path
  makeLayoutFile(LocationPtr location, Layout l,
                 const std::string &name) const = 0;
  [[nodiscard]] virtual std::vector<int>
  listSegmentId(LocationPtr location, std::uint32_t dest_id) const = 0;
  [[nodiscard]] virtual std::filesystem::path getHome() const = 0;
};

using ILocatorPtr = std::shared_ptr<ILocator>;

class Location : public std::enable_shared_from_this<Location> {
public:
  Location(Mode m, Category c, std::string g, std::string n, ILocatorPtr l)
      : mode(m), category(c), group(std::move(g)), name(std::move(n)),
        category_str(categoryToStr(c)), mode_str(modeToStr(m)),
        uname(makeUname(category_str, group, name, mode_str)),
        uid(revobase::HashUtils::hashStr32(uname)), locator(std::move(l)) {}

  virtual ~Location() = default;

  const Mode mode;
  const Category category;
  const std::string group;
  const std::string name;
  const char *category_str;
  const char *mode_str;
  const std::string uname;
  const std::uint32_t uid;
  const ILocatorPtr locator;

  static inline LocationPtr make(Mode m, Category c, const std::string &g,
                                 const std::string &n, const ILocatorPtr &l) {
    assert(!g.empty() && !n.empty());
    return std::make_shared<Location>(m, c, g, n, l);
  }

private:
  static std::string makeUname(const char *cat_str, const std::string &g,
                               const std::string &n, const char *mod_str) {
    // Reserve exact size to avoid reallocations
    size_t cat_len = std::strlen(cat_str);
    size_t mod_len = std::strlen(mod_str);
    std::string result;
    result.reserve(cat_len + g.size() + n.size() + mod_len + 3);

    result = cat_str;
    result += '/';
    result += g;
    result += '/';
    result += n;
    result += '/';
    result += mod_str;

    return result;
  }
};

class Locator : public ILocator {
public:
  explicit Locator(std::string h) : home_path_(std::move(h)) {}

  ~Locator() override = default;
  [[nodiscard]] bool hasEnv(const std::string &name) const override {
    return std::getenv(name.c_str()) != nullptr;
  }

  [[nodiscard]] std::string getEnv(const std::string &name) const override {
    const char *v = std::getenv(name.c_str());
    return !v ? "" : v;
  }

  [[nodiscard]] bool exists(LocationPtr location, Layout l) const override {
    return std::filesystem::exists(layoutDir(location, l));
  }

  [[nodiscard]] std::filesystem::path layoutDir(LocationPtr location,
                                                Layout l) const override {
    return home_path_ / categoryToStr(location->category) / location->group /
           location->name / layoutToStr(l) / modeToStr(location->mode);
  }

  [[nodiscard]] std::filesystem::path makeLayoutDir(LocationPtr location,
                                                    Layout l) const override {
    auto p = layoutDir(location, l);
    if (!std::filesystem::exists(p)) {
      // like mkdir -p and exists_ok = true
      std::filesystem::create_directories(p);
    }
    return p;
  }

  [[nodiscard]] std::filesystem::path
  makeLayoutFile(LocationPtr location, Layout l,
                 const std::string &name) const override {
    auto p = makeLayoutDir(location, l);
    auto p2 = p / fmt::format("{}.{}", name, layoutToStr(l));
    revobase::FileUtils::touch(p2); // creates if missing, never truncates
    return p2;
  }

  [[nodiscard]] std::vector<int>
  listSegmentId(LocationPtr location, std::uint32_t dest_id) const override {
    std::string hex_dest_id = fmt::format("{:08x}", dest_id);
    auto p = makeLayoutDir(location, Layout::STREAM);
    std::vector<int> segment_ids;
    for (const auto &entry : std::filesystem::directory_iterator(p)) {
      auto parts = revobase::StringUtils::split(entry.path().filename().string(), '.');
      if (parts[0] == hex_dest_id) {
        segment_ids.push_back(std::stoi(parts[1]));
      }
    }
    std::sort(segment_ids.begin(), segment_ids.end(),
              [](int a, int b) { return a < b; });
    return segment_ids;
  }

  [[nodiscard]] std::filesystem::path getHome() const override {
    return home_path_;
  }

private:
  std::filesystem::path home_path_;
};

} // namespace revoq

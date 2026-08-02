// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 RevoRacer
#pragma once

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
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

namespace racer::revoq {
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
    {"log", Category::LOG}};

enum class Layout : std::int8_t { UNKNOWN = 0, STREAM, LOG };

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

enum class MsgType : std::uint16_t {
  Invalid = 0,

  Trade = 100,
  SnapshotDepth = 101,
  DiffDepth = 102,
  BookTop5 = 103,
  BookTop10 = 104,
  BookTop20 = 105,
  BBO = 106,
  BookReset = 108,

  FeedStatus = 110,
  FeedGap = 111,
  FeedRecovery = 112,
  Heartbeat = 113,

  MarkPrice = 150,
  FundingRate = 151,
  OpenInterest = 152,
  Liquidation = 153,

  MboAdd = 200,
  MboModify = 201,
  MboDelete = 202,
  MboExecute = 203,
  MboSnapshot = 204,

  InstrumentDefinition = 300,
  SourceDefinition = 301,
  TradingStatus = 302,
  TickTable = 303,

  RawPacket = 400,
  RawMessage = 401,
  NativeEvent = 402,

  OrderUpdate = 1000,
  PositionUpdate = 1001,
  AccountBalance = 1002,
  ExecutionReport = 1003,

  TradingSignal = 1100,
  StrategyState = 1101,
  MarketDataRequest = 1102,

  SegmentEnd = 10000,
  SessionStart = 10001,
  SessionEnd = 10002,

  SimulationStart = 10100,
  SimulationEnd = 10101
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

      std::filesystem::create_directories(p);
    }
    return p;
  }

  [[nodiscard]] std::filesystem::path
  makeLayoutFile(LocationPtr location, Layout l,
                 const std::string &name) const override {
    auto p = makeLayoutDir(location, l);
    auto p2 = p / fmt::format("{}.{}", name, layoutToStr(l));
    if (!std::filesystem::exists(p2)) {
      revobase::FileUtils::touch(p2);
    }
    return p2;
  }

  [[nodiscard]] std::vector<int>
  listSegmentId(LocationPtr location, std::uint32_t dest_id) const override {
    std::string hex_dest_id = fmt::format("{:08x}", dest_id);
    auto p = makeLayoutDir(location, Layout::STREAM);
    std::vector<int> segment_ids;
    for (const auto &entry : std::filesystem::directory_iterator(p)) {
      auto parts =
          revobase::StringUtils::split(entry.path().filename().string(), '.');
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

} // namespace racer::revoq

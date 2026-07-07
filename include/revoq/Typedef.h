// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 RevoRacer
#pragma once

#include <array>
#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>

#include <fmt/format.h>
#include <spdlog/spdlog.h>

#include "HashUtils.h"

namespace racer {
using ExchOrderId = std::uint64_t;
using ClientOrderId = std::uint64_t;
using TradeId = std::uint64_t;

#define HFT_POD_STRUCT(T)                                                      \
  static_assert(std::is_standard_layout_v<T>);                                 \
  static_assert(std::is_trivially_copyable_v<T>);                              \
  static_assert(std::is_trivially_destructible_v<T>);

template <size_t N> struct FixedString {
  std::array<char, N> data{};
  uint8_t len{0};

  constexpr FixedString() = default;
  constexpr explicit FixedString(std::string_view sv)
      : len(static_cast<uint8_t>(std::min(sv.size(), N))) {
    assert(N >= sv.size());
    if (len > 0) {
      std::memcpy(data.data(), sv.data(), len);
    }
    if (len < N) {
      data[len] = '\0';
    }
  }

  constexpr explicit FixedString(const char *str)
      : FixedString(std::string_view(str)) {}

  constexpr FixedString &operator=(std::string_view sv) {
    len = static_cast<uint8_t>(std::min(sv.size(), N));
    if (len > 0) {
      std::memcpy(data.data(), sv.data(), len);
    }
    if (len < N) {
      data[len] = '\0';
    }
    return *this;
  }

  constexpr FixedString &operator=(const char *str) {
    *this = std::string_view(str);
    return *this;
  }

  constexpr FixedString &operator=(const FixedString &other) {
    if (this != &other) {
      len = other.len;
      if (len > 0) {
        std::memcpy(data.data(), other.data.data(), len);
      }
      if (len < N) {
        data[len] = '\0';
      }
    }
    return *this;
  }
  constexpr std::string_view view() const noexcept {
    return {data.data(), len};
  }

  constexpr const char *c_str() const noexcept { return data.data(); }
  constexpr size_t size() const noexcept { return len; }
  constexpr bool empty() const noexcept { return len == 0; }
  constexpr bool operator==(const FixedString &other) const noexcept {
    return len == other.len &&
           (len == 0 || std::memcmp(data.data(), other.data.data(), len) == 0);
  }

  constexpr bool operator==(std::string_view sv) const noexcept {
    return view() == sv;
  }
};

enum class Exchange : std::uint8_t {
  BINANCE = 1,
  OKX = 2,
  UNKNOWN = 255,
};

constexpr const char *exchangeToStr(Exchange e) noexcept {
  switch (e) {
  case Exchange::BINANCE:
    return "binance";
  case Exchange::OKX:
    return "okx";
  default:
    return "unknown";
  }
}

inline Exchange strToExchange(std::string_view s) {
  static const std::unordered_map<std::string_view, Exchange> map = {
      {"binance", Exchange::BINANCE},
      {"okx", Exchange::OKX},
  };

  auto it = map.find(s);
  return it != map.end() ? it->second : Exchange::UNKNOWN;
}

enum class ContractType : std::uint8_t {
  SPOT = 0,
  PERPETUAL = 1,
  FUTURE = 2,
  OPTION = 3,
  UNKNOWN = 255
};

enum class PositionSide : std::uint8_t { BOTH = 0, SHORT = 1, LONG = 2 };

inline ContractType strToContractType(std::string_view s) {
  static const std::unordered_map<std::string_view, ContractType> map = {
      {"spot", ContractType::SPOT},
      {"perp", ContractType::PERPETUAL},
      {"future", ContractType::FUTURE},
      {"option", ContractType::OPTION},
  };

  auto it = map.find(s);
  return it != map.end() ? it->second : ContractType::UNKNOWN;
}

inline PositionSide strToPositionSide(std::string_view s) {
  static const std::unordered_map<std::string_view, PositionSide> map = {
      {"BOTH", PositionSide::BOTH},
      {"SHORT", PositionSide::SHORT},
      {"LONG", PositionSide::LONG},
  };
  auto it = map.find(s);
  return it != map.end() ? it->second : PositionSide::BOTH;
}

constexpr const char *contractTypeToStr(ContractType ct) noexcept {
  switch (ct) {
  case ContractType::SPOT:
    return "spot";
  case ContractType::PERPETUAL:
    return "perpetual";
  case ContractType::FUTURE:
    return "future";
  case ContractType::OPTION:
    return "option";
  default:
    return "unknown";
  }
}

constexpr const char *to_string(PositionSide pos) noexcept {
  switch (pos) {
  case PositionSide::BOTH:
    return "BOTH";
  case PositionSide::LONG:
    return "LONG";
  case PositionSide::SHORT:
    return "SHORT";
  default:
    return "BOTH";
  }
}

enum class MarginType : uint8_t { LINEAR = 0, INVERSE = 1, NONE = 2 };

enum class SettlementCurrency : uint8_t {
  USDT = 0,
  USDC = 1,
  BUSD = 2,
  USD = 3,
  BTC = 4,
  ETH = 5,
  UNKNOWN = 255
};

enum class OptionType : uint8_t { CALL = 0, PUT = 1, NONE = 255 };

enum class InstrumentStatus : uint8_t {
  ACTIVE = 0,
  INACTIVE = 1,
  EXPIRED = 2,
  DELISTED = 3,
  UNKNOWN = 255
};

enum class Side : uint8_t { Bid, Ask };
inline Side strToOrderSide(std::string_view s) {
  return s.at(0) == 'B' ? Side::Bid : Side::Ask;
}

namespace side {
template <typename T, typename = std::enable_if_t<std::is_same_v<T, int> ||
                                                  std::is_same_v<T, double>>>
inline T sign(Side s, T v) {
  return (s == Side::Ask) ? -v : v;
}
} // namespace side
enum class OrderType : uint8_t { LIMIT = 0, MARKET = 1, LIMIT_MAKER = 2 };

constexpr const char *to_string(Side side) noexcept {
  switch (side) {
  case Side::Bid:
    return "BUY";
  case Side::Ask:
    return "SELL";
  }
  return "UNKNOWN";
}

constexpr const char *to_string(OrderType order_type) noexcept {
  switch (order_type) {
  case OrderType::LIMIT:
    return "LIMIT";
  case OrderType::MARKET:
    return "MARKET";
  case OrderType::LIMIT_MAKER:
    return "LIMIT_MAKER";
  }
  return "UNKNOWN";
}

enum class OrderStatus : uint8_t {
  INVALID = 0,
  PENDING,
  SUBMITTED,
  ACKNOWLEDGED,
  PARTIALLY_FILLED,
  FILLED,
  CANCELLING,
  CANCELLED,
  REJECTED,
  EXPIRED
};

constexpr const char *to_string(OrderStatus status) noexcept {
  switch (status) {
  case OrderStatus::PENDING:
    return "PENDING";
  case OrderStatus::SUBMITTED:
    return "SUBMITTED";
  case OrderStatus::ACKNOWLEDGED:
    return "ACKNOWLEDGED";
  case OrderStatus::PARTIALLY_FILLED:
    return "PARTIALLY_FILLED";
  case OrderStatus::FILLED:
    return "FILLED";
  case OrderStatus::CANCELLING:
    return "CANCELLING";
  case OrderStatus::CANCELLED:
    return "CANCELLED";
  case OrderStatus::REJECTED:
    return "REJECTED";
  case OrderStatus::EXPIRED:
    return "EXPIRED";
  case OrderStatus::INVALID:
    return "INVALID";
  }
  return "UNKNOWN";
}

constexpr OrderStatus toOrderStatus(std::string_view v) {
  switch (HashUtils::fnv1a_32(v)) {
  case HashUtils::fnv1a_32("NEW"):
    return OrderStatus::ACKNOWLEDGED;
  case HashUtils::fnv1a_32("PARTIALLY_FILLED"):
    return OrderStatus::PARTIALLY_FILLED;
  case HashUtils::fnv1a_32("FILLED"):
    return OrderStatus::FILLED;
  case HashUtils::fnv1a_32("CANCELLING"):
    return OrderStatus::CANCELLING;
  case HashUtils::fnv1a_32("CANCELED"):
    return OrderStatus::CANCELLED;
  case HashUtils::fnv1a_32("EXPIRED"):
    return OrderStatus::EXPIRED;
  case HashUtils::fnv1a_32("EXPIRED_IN_MATCH"):
    return OrderStatus::EXPIRED;
  default:
    return OrderStatus::REJECTED;
  }
}

enum class UDSEventType : uint8_t {
  INVALID = 0,
  ORDER_TRADE_UPDATE,
  ACCOUNT_UPDATE,
  MARGIN_CALL,
  TRADE_LITE,

  outboundAccountPosition,
  balanceUpdate,
  executionReport
};

constexpr UDSEventType toEventType(std::string_view v) {
  switch (HashUtils::fnv1a_32(v)) {
  case HashUtils::fnv1a_32("ORDER_TRADE_UPDATE"):
    return UDSEventType::ORDER_TRADE_UPDATE;
  case HashUtils::fnv1a_32("TRADE_LITE"):
    return UDSEventType::TRADE_LITE;
  case HashUtils::fnv1a_32("ACCOUNT_UPDATE"):
    return UDSEventType::ACCOUNT_UPDATE;
  case HashUtils::fnv1a_32("MARGIN_CALL"):
    return UDSEventType::MARGIN_CALL;
  case HashUtils::fnv1a_32("outboundAccountPosition"):
    return UDSEventType::outboundAccountPosition;
  case HashUtils::fnv1a_32("balanceUpdate"):
    return UDSEventType::balanceUpdate;
  case HashUtils::fnv1a_32("executionReport"):
    return UDSEventType::executionReport;
  }
  return UDSEventType::INVALID;
}

struct BookLevel {
  double price{};
  double quantity{};
};

enum class TimeInForce : uint8_t {
  INVALID = 0,
  GTC,
  IOC,
  FOK,
  GTX,

};

constexpr const char *to_string(TimeInForce tif) noexcept {
  switch (tif) {
  case TimeInForce::GTC:
    return "GTC";
  case TimeInForce::IOC:
    return "IOC";
  case TimeInForce::FOK:
    return "FOK";
  case TimeInForce::GTX:
    return "GTX";
  default:
    return "UNKNOWN";
  }
}

enum class ExecutionType : uint8_t {
  INVALID = 0,
  NEW,
  CANCELED,
  CALCULATED,
  EXPIRED,
  TRADE,
  TRADE_PREVENTION,
  AMENDMENT,
  REPLACED,
  REJECTED,
};

enum class AccountUpdateReason : uint8_t {
  INVALID,
  ORDER,
  FUNDING_FEE,
  DEPOSIT,
  WITHDRAW,
  INSURANCE_CLEAR,
  MARGIN_TRANFER
};

constexpr AccountUpdateReason toAccountUpdateReason(std::string_view s) {
  switch (HashUtils::fnv1a_32(s)) {
  case HashUtils::fnv1a_32("ORDER"):
    return AccountUpdateReason::ORDER;
  case HashUtils::fnv1a_32("FUNDING_FEE"):
    return AccountUpdateReason::FUNDING_FEE;
  case HashUtils::fnv1a_32("DEPOSIT"):
    return AccountUpdateReason::DEPOSIT;
  case HashUtils::fnv1a_32("WITHDRAW"):
    return AccountUpdateReason::WITHDRAW;
  case HashUtils::fnv1a_32("INSURANCE_CLEAR"):
    return AccountUpdateReason::INSURANCE_CLEAR;
  case HashUtils::fnv1a_32("MARGIN_TRANFER"):
    return AccountUpdateReason::MARGIN_TRANFER;
  }
  return AccountUpdateReason::INVALID;
}

enum class CryptoAsset : uint16_t {
  INVALID = 0,
  BNB,
  BTC,
  DOGE,
  ETH,
  SHIB,
  SOL,
  USDT,
  XRP,
};

constexpr CryptoAsset toCryptoAsset(std::string_view s) {
  switch (HashUtils::fnv1a_32(s)) {
  case HashUtils::fnv1a_32("USDT"):
    return CryptoAsset::USDT;
  case HashUtils::fnv1a_32("BTC"):
    return CryptoAsset::BTC;
  case HashUtils::fnv1a_32("ETH"):
    return CryptoAsset::ETH;
  case HashUtils::fnv1a_32("SOL"):
    return CryptoAsset::SOL;
  case HashUtils::fnv1a_32("BNB"):
    return CryptoAsset::BNB;
  case HashUtils::fnv1a_32("DOGE"):
    return CryptoAsset::DOGE;
  case HashUtils::fnv1a_32("SHIB"):
    return CryptoAsset::SHIB;
  case HashUtils::fnv1a_32("XRP"):
    return CryptoAsset::XRP;
  }
  return CryptoAsset::INVALID;
}

constexpr const char *to_string(CryptoAsset asset) noexcept {
  switch (asset) {
  case CryptoAsset::USDT:
    return "USDT";
  case CryptoAsset::BTC:
    return "BTC";
  case CryptoAsset::ETH:
    return "ETH";
  case CryptoAsset::SOL:
    return "SOL";
  case CryptoAsset::BNB:
    return "BNB";
  case CryptoAsset::DOGE:
    return "DOGE";
  case CryptoAsset::SHIB:
    return "SHIB";
  case CryptoAsset::XRP:
    return "XRP";
  default:
    return "INVALID";
  }
}
} // namespace racer

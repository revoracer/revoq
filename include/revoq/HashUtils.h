// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 RevoRacer
#pragma once

#include <cstdint>
#include <string>
#include <string_view>

namespace racer {
class HashUtils {
public:
  static constexpr uint32_t fnv1a_32(std::string_view str) noexcept {
    uint32_t hash = 2166136261u;
    for (char c : str) {
      hash ^= static_cast<uint8_t>(c);
      hash *= 16777619u;
    }
    return hash;
  }

  static uint32_t hash32(const unsigned char *key, int32_t length,
                         uint32_t seed = 0);

  static inline uint32_t hashStr32(std::string_view key, uint32_t seed = 0) {
    return hash32(reinterpret_cast<const unsigned char *>(key.data()),
                  static_cast<int32_t>(key.length()), seed);
  }

  static inline uint32_t hashCStr32(const char *key, size_t length,
                                    uint32_t seed = 0) {
    return hash32(reinterpret_cast<const unsigned char *>(key),
                  static_cast<int32_t>(length), seed);
  }

  template <typename T>
  static inline uint32_t hashStruct32(const T &data, uint32_t seed = 0) {
    static_assert(std::is_trivially_copyable_v<T>,
                  "Type must be trivially copyable");
    return hash32(reinterpret_cast<const unsigned char *>(&data),
                  static_cast<int32_t>(sizeof(T)), seed);
  }
};
} // namespace racer

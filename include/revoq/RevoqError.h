// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 RevoRacer
#pragma once

#include <stdexcept>

namespace racer {
namespace revoq {
class RevoqError : public std::runtime_error {
public:
  explicit RevoqError(const std::string &message)
      : std::runtime_error(message) {}
};
} // namespace revoq
} // namespace racer

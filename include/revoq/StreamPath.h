// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 RevoRacer
#pragma once

#include <filesystem>
#include <spdlog/spdlog.h>
#include <string>
#include <string_view>
#include <sys/vfs.h>
#include <unistd.h>

namespace revoq {

inline std::string resolveStreamBase(std::string_view app_name,
                                     std::string_view app_directory,
                                     std::string_view configured_directory) {
  if (!configured_directory.empty()) {
    std::filesystem::create_directories(configured_directory);
    SPDLOG_INFO("stream directory: {} (from config)", configured_directory);
    return std::string(configured_directory);
  }
  static constexpr unsigned long kHugetlbfsMagic = 0x958458f6UL;
  static constexpr const char *kHugetlbfsRoot = "/dev/hugepages";
  struct statfs sfs{};
  if (statfs(kHugetlbfsRoot, &sfs) == 0 &&
      static_cast<unsigned long>(sfs.f_type) == kHugetlbfsMagic &&
      access(kHugetlbfsRoot, W_OK) == 0) {
    std::string dir = std::string(kHugetlbfsRoot) + "/" + std::string(app_name);
    std::filesystem::create_directories(dir);
    SPDLOG_INFO("stream directory: {} (hugetlbfs, guaranteed 2MB pages)", dir);
    return dir;
  }
  SPDLOG_INFO("stream directory: {} (regular fs, THP best-effort)",
              app_directory);
  return std::string(app_directory);
}

} // namespace revoq

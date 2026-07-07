// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 RevoRacer
#pragma once

#include <cassert>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include <fmt/core.h>

#include "Typedef.h"

namespace racer {
class FileUtils {
public:
  static inline void untarFile(const std::filesystem::path &file_tar_gz,
                               const std::filesystem::path &dest_dir) {
    assert(exists(file_tar_gz));
    assert(exists(dest_dir));
    auto ret = std::system(fmt::format("tar -xzvf {} -C {}",
                                       file_tar_gz.string(), dest_dir.string())
                               .c_str());
    assert(ret == 0);
  }

  static inline bool rmDir(const std::filesystem::path &dir) {
    namespace fs = std::filesystem;
    assert(fs::exists(dir) && fs::is_directory(dir));
    return fs::remove_all(dir) != 0;
  }

  static inline bool mkDir(const std::filesystem::path &directory) {
    return std::filesystem::create_directory(directory);
  }

  static inline bool exists(const std::filesystem::path &path) {
    return std::filesystem::exists(path);
  }

  static inline bool touch(const std::filesystem::path &path) {
    std::ofstream f(path);
    return f.is_open();
  }

  static inline std::vector<std::filesystem::path>
  listDirs(const std::filesystem::path &path) {
    std::vector<std::filesystem::path> dirs;
    for (const auto &entry : std::filesystem::directory_iterator(path)) {
      if (entry.is_directory()) {
        dirs.emplace_back(entry.path());
      }
    }
    return dirs;
  }

  [[nodiscard]] static inline bool hasEnv(const std::string &name) {
    return std::getenv(name.c_str()) != nullptr;
  }

  [[nodiscard]] static inline std::string getEnv(const std::string &name) {
    const char *v = std::getenv(name.c_str());
    return !v ? "" : v;
  }
};

} // namespace racer

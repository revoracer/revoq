// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 RevoRacer
#include <revoq/MmapBuffer.h>

#include <cerrno>
#include <cstring>
#include <sys/fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <spdlog/spdlog.h>

#ifdef __APPLE__
#include <TargetConditionals.h>
#define IS_MACOS 1
#else
#define IS_MACOS 0
#endif

#ifdef __linux__
#define IS_LINUX 1
#else
#define IS_LINUX 0
#endif

#if IS_LINUX && !defined(MADV_POPULATE_READ)
#define MADV_POPULATE_READ 22
#endif

namespace racer {
namespace os {

std::uintptr_t MmapBuffer::loadMmapBuffer(const std::string &path,
                                          std::size_t size, bool is_writing,
                                          bool prefault) {

  struct stat st{};
  bool file_exists = false;
  if (is_writing)
    file_exists = (stat(path.c_str(), &st) == 0);

  const int open_flags =
      (is_writing ? (O_RDWR | O_CREAT) : O_RDONLY) | O_CLOEXEC;
  int fd = open(path.c_str(), open_flags, (mode_t)0600);

  if (fd < 0) {
    throw MmapError("failed to open file: " + path +
                    " errno: " + std::to_string(errno));
  }

  if (!is_writing) {
    struct stat opened_st{};
    if (fstat(fd, &opened_st) != 0) {
      int err = errno;
      close(fd);
      throw MmapError("failed to stat file: " + path +
                      " errno: " + std::to_string(err));
    }
    if (opened_st.st_size != static_cast<off_t>(size)) {
      close(fd);
      throw MmapError("unexpected file size: " + path +
                      " required: " + std::to_string(size) +
                      " found: " + std::to_string(opened_st.st_size));
    }
  }

  if (is_writing && (!file_exists || st.st_size != static_cast<off_t>(size))) {
#if IS_LINUX

    if (fallocate(fd, 0, 0, size) != 0) {

      if (ftruncate(fd, size) != 0) {
        int err = errno;
        close(fd);
        if (!file_exists)
          unlink(path.c_str());
        throw MmapError("failed to allocate file: " + path +
                        " errno: " + std::to_string(err));
      }
    }
#else

    if (ftruncate(fd, size) != 0) {
      int err = errno;
      close(fd);
      if (!file_exists)
        unlink(path.c_str());
      throw MmapError("failed to allocate file: " + path +
                      " errno: " + std::to_string(err));
    }
#endif
  }

  int prot = is_writing ? (PROT_READ | PROT_WRITE) : PROT_READ;
  int flags = MAP_SHARED;

  if (prefault) {
#if IS_LINUX

    flags |= MAP_POPULATE;
#endif
  }

  void *buffer = mmap(nullptr, size, prot, flags, fd, 0);
  int err = errno;
  close(fd);

  if (buffer == MAP_FAILED) {
    if (is_writing && !file_exists)
      unlink(path.c_str());
    throw MmapError("mmap failed: " + path + " errno: " + std::to_string(err) +
                    " (" + std::string(strerror(err)) + ")");
  }

  if (prefault) {
#if IS_LINUX

    if (madvise(buffer, size, MADV_SEQUENTIAL) != 0) {
      SPDLOG_WARN("madvise MADV_SEQUENTIAL failed for {}: {}", path,
                  strerror(errno));
    }

    if (madvise(buffer, size, MADV_HUGEPAGE) != 0) {
      SPDLOG_DEBUG("MADV_HUGEPAGE failed for {}: {}", path, strerror(errno));
    }
#elif IS_MACOS

    if (madvise(buffer, size, MADV_SEQUENTIAL) != 0) {
      SPDLOG_WARN("madvise MADV_SEQUENTIAL failed for {}: {}", path,
                  strerror(errno));
    }

    if (madvise(buffer, size, MADV_WILLNEED) != 0) {
      SPDLOG_DEBUG("madvise MADV_WILLNEED failed for {}: {}", path,
                   strerror(errno));
    }
#endif

    if (mlock(buffer, size) != 0) {
      int lock_err = errno;
#if IS_LINUX
      munmap(buffer, size);
      throw MmapError(
          "mlock failed: " + path + " errno: " + std::to_string(lock_err) +
          " (" + std::string(strerror(lock_err)) + ")" + " - check ulimit -l");
#else
      SPDLOG_WARN(
          "mlock failed: {} - errno: {} ({}). Continuing without locking.",
          path, lock_err, strerror(lock_err));
#endif
    }

    if (is_writing && !file_exists) {

      std::memset(buffer, 0, size);
    } else {

      volatile char *ptr = static_cast<volatile char *>(buffer);
#if IS_MACOS

      std::size_t page_size = 16384;
#else

      std::size_t page_size = 4096;
#endif

      for (std::size_t i = 0; i < size; i += page_size) {

        (void)ptr[i];
      }
    }
  }

  SPDLOG_TRACE("mapped {} - {} - {} - {} bytes", path, is_writing ? "rw" : "r",
               prefault ? "locked" : "lazy", size);

  return reinterpret_cast<std::uintptr_t>(buffer);
}

std::uintptr_t MmapBuffer::loadExistingReadOnlyMmapBuffer(
    const std::string &path, std::size_t size, ReadOnlyMmapOptions options) {
  int fd = open(path.c_str(), O_RDONLY | O_CLOEXEC);
  if (fd < 0) {
    throw MmapError("failed to open existing file: " + path +
                    " errno: " + std::to_string(errno));
  }

  struct stat st{};
  if (fstat(fd, &st) != 0) {
    int err = errno;
    close(fd);
    throw MmapError("failed to stat existing file: " + path +
                    " errno: " + std::to_string(err));
  }
  if (st.st_size != static_cast<off_t>(size)) {
    close(fd);
    throw MmapError("unexpected file size: " + path +
                    " required: " + std::to_string(size) +
                    " found: " + std::to_string(st.st_size));
  }

  void *buffer = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
  int err = errno;
  close(fd);
  if (buffer == MAP_FAILED) {
    throw MmapError("read-only mmap failed: " + path +
                    " errno: " + std::to_string(err) + " (" +
                    std::string(strerror(err)) + ")");
  }

  try {
    prepareReadOnlyMmapBuffer(path, reinterpret_cast<std::uintptr_t>(buffer),
                              size, options);
  } catch (...) {
    munmap(buffer, size);
    throw;
  }

  SPDLOG_TRACE("mapped existing {} - r - {} bytes", path, size);
  return reinterpret_cast<std::uintptr_t>(buffer);
}

void MmapBuffer::prepareReadOnlyMmapBuffer(const std::string &path,
                                           std::uintptr_t address,
                                           std::size_t size,
                                           ReadOnlyMmapOptions options) {
  void *buffer = reinterpret_cast<void *>(address);
  if (buffer == nullptr) {
    throw MmapError("cannot prepare null read-only mmap: " + path);
  }

#if IS_LINUX
  if (options.advise_hugepage && madvise(buffer, size, MADV_HUGEPAGE) != 0) {
    SPDLOG_DEBUG("MADV_HUGEPAGE failed for {}: {}", path, strerror(errno));
  }

  if (options.populate_read && madvise(buffer, size, MADV_POPULATE_READ) != 0) {
    int err = errno;
    throw MmapError("MADV_POPULATE_READ failed: " + path +
                    " errno: " + std::to_string(err) + " (" +
                    std::string(strerror(err)) + ")");
  }
#else
  if (options.populate_read) {
    throw MmapError("MADV_POPULATE_READ is unsupported for: " + path);
  }
#endif
}

bool MmapBuffer::releaseMmapBuffer(std::uintptr_t address, std::size_t size,
                                   bool prefault, MsyncMode msync_mode) {
  void *buffer = reinterpret_cast<void *>(address);
  if (buffer == nullptr) {
    SPDLOG_ERROR("Attempted to release null buffer");
    return false;
  }
  if (prefault) {

    if (munlock(buffer, size) != 0) {
      SPDLOG_ERROR("munlock failed: {}", strerror(errno));
    }
  }
  if (msync_mode == MsyncMode::SYNC) {

    if (msync(buffer, size, MS_SYNC) != 0) {
      SPDLOG_ERROR("msync failed: {}", strerror(errno));
      return false;
    }
  } else if (msync_mode == MsyncMode::ASYNC) {
    msync(buffer, size, MS_ASYNC);
  }

  if (munmap(buffer, size) != 0) {
    SPDLOG_ERROR("munmap failed: {}", strerror(errno));
    return false;
  }
  SPDLOG_TRACE("released buffer at {} - {} bytes",
               reinterpret_cast<void *>(address), size);
  return true;
}

} // namespace os
} // namespace racer

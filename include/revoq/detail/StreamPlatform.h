// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 RevoRacer
#pragma once

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>

namespace revoq::detail {

// Lost-notify-safe worker shutdown, shared by the cursor and appender warmers.
// Flips `running` false under `m` — the same lock the worker's cv predicate
// uses — so the notify below cannot fall into the gap between the worker
// evaluating its predicate as false and atomically releasing the lock to sleep.
// Returns whether the worker had been running (false => already stopped).
inline bool stopAndJoin(std::atomic<bool> &running, std::mutex &m,
                        std::condition_variable &cv, std::thread &t) {
  bool was_running;
  {
    std::lock_guard<std::mutex> lock(m);
    was_running = running.exchange(false, std::memory_order_release);
  }
  if (!was_running)
    return false;
  cv.notify_all();
  if (t.joinable())
    t.join();
  return true;
}

} // namespace revoq::detail

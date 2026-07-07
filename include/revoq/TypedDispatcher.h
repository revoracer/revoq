// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 RevoRacer
#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>

#include "MergingCursor.h"

namespace racer::revoq {

template <typename...>
inline constexpr bool typed_dispatch_always_false_v = false;

class TypedDispatcher {
public:
  using FrameView = MergingCursor::FrameView;
  using TypedFrameHandler = std::function<void(FrameView)>;
  using UnknownFrameHandler = std::function<void(FrameView)>;

  explicit TypedDispatcher(MergingCursor &cursor) : cursor_(cursor) {}

  template <typename T, typename Handler>
  TypedDispatcher &on(Handler &&handler) {
    return on<T::kMsgType, T>(std::forward<Handler>(handler));
  }

  template <MsgType Msg, typename T, typename Handler>
  TypedDispatcher &on(Handler &&handler) {
    using HandlerT = std::decay_t<Handler>;
    static_assert(std::is_invocable_v<HandlerT &, const T &, const Frame &> ||
                      std::is_invocable_v<HandlerT &, const T &>,
                  "typed stream handler must accept (const T&) or "
                  "(const T&, const Frame&)");

    typed_handlers_[static_cast<std::int32_t>(Msg)] =
        [handler = HandlerT(std::forward<Handler>(handler))](
            FrameView current) mutable {
          const T &message = typedPayload<T>(*current, current.frame_length);
          invokeTypedHandler(handler, message, *current);
        };
    return *this;
  }

  template <typename Handler> TypedDispatcher &onUnknown(Handler &&handler) {
    using HandlerT = std::decay_t<Handler>;
    static_assert(std::is_invocable_v<HandlerT &, const Frame &>,
                  "unknown stream handler must accept const Frame&");
    unknown_handler_ = [handler = HandlerT(std::forward<Handler>(handler))](
                           FrameView current) mutable { handler(*current); };
    return *this;
  }

  void clearHandlers() {
    typed_handlers_.clear();
    unknown_handler_ = nullptr;
  }

  bool dispatchCurrent() {
    const FrameView current = cursor_.tryCurrentFrame();
    return current && dispatchFrame(current);
  }

  bool next() {
    const FrameView current = cursor_.tryCurrentFrame();
    if (!current)
      return false;
    dispatchFrame(current);
    cursor_.advance(current);
    return true;
  }

  std::size_t
  run(std::size_t max_frames = std::numeric_limits<std::size_t>::max()) {
    std::size_t count = 0;
    while (count < max_frames && next())
      ++count;
    return count;
  }

private:
  bool dispatchFrame(FrameView current) {
    const auto it = typed_handlers_.find(current.msg_type);
    if (it != typed_handlers_.end()) {
      it->second(current);
      return true;
    }

    if (unknown_handler_) {
      unknown_handler_(current);
      return true;
    }
    return false;
  }

  template <typename Handler, typename T>
  static void invokeTypedHandler(Handler &handler, const T &message,
                                 const Frame &frame) {
    if constexpr (std::is_invocable_v<Handler &, const T &, const Frame &>) {
      handler(message, frame);
    } else if constexpr (std::is_invocable_v<Handler &, const T &>) {
      handler(message);
    } else {
      static_assert(typed_dispatch_always_false_v<Handler, T>,
                    "typed stream handler has unsupported signature");
    }
  }

  template <typename T>
  static const T &typedPayload(const Frame &frame, std::uint32_t frame_length) {
    const std::uint32_t data_length = frame.getDataLength(frame_length);
    if constexpr (std::is_empty_v<T>) {
      if (data_length == 0) {
        static const T empty{};
        return empty;
      }
    }

    if (data_length != sizeof(T)) [[unlikely]] {
      throw std::runtime_error(
          "typed stream handler payload size mismatch for msg_type " +
          std::to_string(frame.getMsgType()) + ": expected " +
          std::to_string(sizeof(T)) + " bytes, got " +
          std::to_string(data_length) + " bytes");
    }
    return frame.data<T>();
  }

  MergingCursor &cursor_;
  std::unordered_map<std::int32_t, TypedFrameHandler> typed_handlers_;
  UnknownFrameHandler unknown_handler_;
};

} // namespace racer::revoq

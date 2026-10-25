// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 RevoRacer
#include <chrono>
#include <cstddef>
#include <string>
#include <thread>

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <revoq/Appender.h>
#include <revoq/Frame.h>
#include <revoq/MergingCursor.h>
#include <revoq/StreamBase.h>

namespace py = pybind11;
using namespace revoq;

// ── FrameSnap ────────────────────────────────────────────────────────────────
// Snapshot of a Frame taken before advancing the cursor.  The live Frame
// object is mutated by next() (its internal pointer moves to the next slot),
// so we must copy fields out before calling next().  One copy per frame —
// irrelevant at Python speeds.
struct FrameSnap {
  uint64_t sequence;
  int32_t msg_type;
  int64_t gen_time;
  int64_t event_time;
  uint16_t flags;
  std::string payload; // copy of raw payload bytes

  static FrameSnap from(const Frame &f) {
    const auto n = f.getDataLength();
    return {f.getSequence(),
            f.getMsgType(),
            f.getGenTime(),
            f.getEventTime(),
            f.getFlags(),
            n > 0 ? std::string(f.dataAsBytes(), n) : std::string{}};
  }
};

// ── helpers ──────────────────────────────────────────────────────────────────

static Mode parse_mode(const std::string &s) {
  auto it = str2mode.find(s);
  if (it == str2mode.end())
    throw py::value_error("Unknown mode '" + s + "'. Valid: live, replay");
  return it->second;
}

static Category parse_category(const std::string &s) {
  auto it = str2category.find(s);
  if (it == str2category.end())
    throw py::value_error(
        "Unknown category '" + s +
        "'. Valid: marketdata, researchdata, executiondata, log");
  return it->second;
}

static FrameSnap snap_and_advance(MergingCursor &r) {
  const auto current = r.tryCurrentFrame();
  if (!current)
    throw py::stop_iteration();
  FrameSnap s = FrameSnap::from(*current);
  r.advance(current);
  return s;
}

// ── module ───────────────────────────────────────────────────────────────────

PYBIND11_MODULE(revoq, m) {
  m.doc() = "revoq stream — MergingCursor and Appender Python bindings";

  // ── Location ──────────────────────────────────────────────────────────────
  py::class_<Location, std::shared_ptr<Location>>(m, "Location")
      .def_readonly("mode", &Location::mode_str)
      .def_readonly("category", &Location::category_str)
      .def_readonly("group", &Location::group)
      .def_readonly("name", &Location::name);

  // ── make_location ─────────────────────────────────────────────────────────
  m.def(
      "make_location",
      [](const std::string &path, const std::string &mode,
         const std::string &category, const std::string &group,
         const std::string &name) -> LocationPtr {
        auto locator = std::make_shared<Locator>(path);
        return Location::make(parse_mode(mode), parse_category(category), group,
                              name, locator);
      },
      py::arg("path"), py::arg("mode"), py::arg("category"), py::arg("group"),
      py::arg("name"),
      R"(Create a stream Location.

Args:
    path:     Filesystem root for stream files.
    mode:     "live" or "replay".
    category: "marketdata", "researchdata", "executiondata", or "log".
    group:    Exchange / venue group name.
    name:     Instrument or stream name.
)");

  // ── Frame (snapshot) ──────────────────────────────────────────────────────
  // Immutable snapshot taken at read time.  Fields are stable for the
  // lifetime of the Frame Python object.
  py::class_<FrameSnap>(m, "Frame")
      .def_readonly("sequence", &FrameSnap::sequence,
                    "Appender-local monotone sequence number.")
      .def_readonly("msg_type", &FrameSnap::msg_type,
                    "Application message type (int32).")
      .def_readonly("gen_time", &FrameSnap::gen_time,
                    "Publish timestamp (ns since epoch).")
      .def_readonly("event_time", &FrameSnap::event_time,
                    "Optional causal timestamp (0 if unused).")
      .def_readonly("flags", &FrameSnap::flags, "Frame flags.")
      .def_property_readonly(
          "data_length", [](const FrameSnap &s) { return s.payload.size(); },
          "Payload length in bytes.")
      .def_property_readonly(
          "data",
          [](const FrameSnap &s) -> py::object {
            // Zero-copy view into the snapshot's own payload buffer.
            // Valid for the lifetime of this Frame object.
            if (s.payload.empty())
              return py::bytes("", 0);
            return py::memoryview::from_memory(
                const_cast<void *>(static_cast<const void *>(s.payload.data())),
                static_cast<py::ssize_t>(s.payload.size()),
                /*readonly=*/true);
          },
          "Payload as a memoryview (zero-copy into the snapshot buffer).");

  py::enum_<SegmentPopulate>(m, "SegmentPopulate",
                             "When segment pages get established.")
      .value("Lazy", SegmentPopulate::Lazy,
             "Map only; first touch of each page faults.")
      .value("Inline", SegmentPopulate::Inline,
             "Populate the whole segment when it is mapped, at rotation.")
      .value("Background", SegmentPopulate::Background,
             "A warmer thread populates upcoming segments ahead of rotation. "
             "Requires background_threads.");

  // ── MergingCursor ─────────────────────────────────────────────────────────
  py::class_<MergingCursor>(m, "MergingCursor")
      .def(py::init([](SegmentPopulate populate, bool lock_pages,
                       bool background_threads,
                       std::size_t retained_segment_limit,
                       int warmer_timeout_ms, int warmer_cpu, int retire_cpu) {
             return std::make_unique<MergingCursor>(MergingCursorOptions{
                 .populate = populate,
                 .lock_pages = lock_pages,
                 .background_threads = background_threads,
                 .retained_segment_limit = retained_segment_limit,
                 .warmer_timeout_ms = warmer_timeout_ms,
                 .warmer_cpu = warmer_cpu,
                 .retire_cpu = retire_cpu});
           }),
           py::kw_only(), py::arg("populate") = SegmentPopulate::Background,
           py::arg("lock_pages") = false, py::arg("background_threads") = true,
           py::arg("retained_segment_limit") = 0,
           py::arg("warmer_timeout_ms") = 5000, py::arg("warmer_cpu") = -1,
           py::arg("retire_cpu") = -1)
      .def("join", &MergingCursor::join, py::arg("location"),
           py::arg("dest_id"), py::arg("from_time") = 0,
           "Join a stream. from_time=0 reads from the beginning.")
      .def("is_data_available", &MergingCursor::isDataAvailable,
           "True if the current frame has been published (non-blocking).")
      .def("next", &MergingCursor::next, "Advance to the next frame.")
      .def(
          "current_frame",
          [](MergingCursor &r) -> py::object {
            auto f = r.tryCurrentFrame();
            if (!f)
              return py::none();
            return py::cast(FrameSnap::from(*f));
          },
          "Snapshot of the current frame, or None if not joined.")
      // Replay iterator: raises StopIteration when no frame is ready.
      .def("__iter__", [](MergingCursor &r) -> MergingCursor & { return r; })
      .def("__next__",
           [](MergingCursor &r) -> FrameSnap {
             if (!r.isDataAvailable())
               throw py::stop_iteration();
             return snap_and_advance(r);
           })
      // Live read with GIL released during spin.
      .def(
          "read",
          [](MergingCursor &r, int64_t timeout_ms) -> py::object {
            if (timeout_ms == 0) {
              if (!r.isDataAvailable())
                return py::none();
            } else {
              auto deadline = std::chrono::steady_clock::now() +
                              std::chrono::milliseconds(timeout_ms);
              bool ready = false;
              {
                py::gil_scoped_release release;
                while (!r.isDataAvailable()) {
                  if (std::chrono::steady_clock::now() >= deadline)
                    break;
                  std::this_thread::yield();
                }
                ready = r.isDataAvailable();
              }
              if (!ready)
                return py::none();
            }
            return py::cast(snap_and_advance(r));
          },
          py::arg("timeout_ms") = 0,
          R"(Return the next frame as a snapshot, or None.

Args:
    timeout_ms: 0 = non-blocking; >0 = spin up to this many ms (GIL released).
)");

  // ── Appender ─────────────────────────────────────────────────────────
  py::class_<Appender<>>(m, "Appender")
      .def(py::init([](LocationPtr location, uint32_t dest_id,
                       SegmentPopulate populate, bool lock_pages,
                       bool background_threads, std::size_t warmup_segments,
                       int warmer_cpu_core, int retire_cpu,
                       std::size_t retained_segment_limit) {
             return std::make_unique<Appender<>>(
                 std::move(location), dest_id,
                 AppenderOptions{.populate = populate,
                                 .lock_pages = lock_pages,
                                 .warmup_segments = warmup_segments,
                                 .warmer_cpu_core = warmer_cpu_core,
                                 .retire_cpu = retire_cpu,
                                 .background_threads = background_threads,
                                 .retained_segment_limit =
                                     retained_segment_limit});
           }),
           py::arg("location"), py::arg("dest_id"), py::kw_only(),
           py::arg("populate") = SegmentPopulate::Background,
           py::arg("lock_pages") = false, py::arg("background_threads") = true,
           py::arg("warmup_segments") = 1, py::arg("warmer_cpu_core") = -1,
           py::arg("retire_cpu") = -1,
           py::arg("retained_segment_limit") =
               DEFAULT_STREAM_APPENDER_RETAINED_SEGMENTS)
      .def(
          "write",
          [](Appender<> &w, int msg_type, int64_t gen_time, py::buffer data) {
            py::buffer_info info = data.request();
            w.writeBytes(gen_time, static_cast<MsgType>(msg_type), info.ptr,
                         static_cast<std::size_t>(info.size * info.itemsize));
          },
          py::arg("msg_type"), py::arg("gen_time"), py::arg("data"),
          R"(Write one frame.  One memcpy from data into the mmap slot.

Args:
    msg_type: Application message type (int).
    gen_time: Publish timestamp (ns since epoch).
    data:     Payload — any buffer-protocol object (bytes, bytearray, numpy array).
)")
      .def("current_sequence", &Appender<>::getCurrentSequence,
           "Next sequence number to be assigned.")
      .def("current_segment_id", &Appender<>::getCurrentSegmentId,
           "Current segment ID.");
}

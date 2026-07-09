// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 RevoRacer
//
// Chronicle Queue comparative driver for the `journal_replayable` group.
//
// Implements the canonical workload in benchmarks/comparative/workload.md:
//   * 1 producer + 1 consumer, SEPARATE OS PROCESSES, over Chronicle's native
//     memory-mapped queue file (no TCP).
//   * Open-loop pacing at a fixed offered rate; the record is stamped with its
//     INTENDED (scheduled) send time, not the actual send time, so a late
//     producer shows up in the tail instead of being erased (coordinated
//     omission).
//   * Fixed-size records: 8-byte seq + 8-byte intended-send-nanos + payload pad
//     to the requested message size.
//   * HdrHistogram over 1 ns .. 1 s at 3 significant figures, emitted as the
//     base64 "V2" interchange string — byte-compatible with the HdrHistogram-C
//     `hdr_log_encode` output the revoq driver produces, so the two are directly
//     chartable.
//
// This is a single class with two sub-commands, `producer` and `consumer`,
// launched as two processes by run.sh. Both timestamp with System.nanoTime().
// On Linux/HotSpot nanoTime() is CLOCK_MONOTONIC read directly (no per-process
// offset), so it is directly comparable across processes on the same host and
// gives true sub-microsecond resolution. An epoch-calibrated clock such as
// Chronicle's SystemTimeProvider was tried first but injected a large constant
// skew (~100us) between the two processes' independent calibrations, which
// swamped the real IPC latency — see the clock note in ../aeron/README.md.
package racer.revoq.bench;

import com.sun.management.GarbageCollectionNotificationInfo;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;

import org.HdrHistogram.Histogram;

import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;
import java.io.PrintStream;
import java.lang.management.CompilationMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public final class ChronicleBench {

    static final int HEADER_BYTES = 16; // long seq + long intendedSendNanos

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("usage: ChronicleBench <producer|consumer> [--k v ...]");
            System.exit(2);
        }
        Map<String, String> a = parse(args);
        String mode = args[0];
        switch (mode) {
            case "producer" -> producer(a);
            case "consumer" -> consumer(a);
            default -> {
                System.err.println("unknown mode: " + mode);
                System.exit(2);
            }
        }
    }

    // ---- producer: paced open-loop appender -------------------------------

    static void producer(Map<String, String> a) throws Exception {
        Path queue = Path.of(req(a, "queue"));
        int bytes = Integer.parseInt(req(a, "bytes"));
        long warmup = Long.parseLong(req(a, "warmup"));
        long measured = Long.parseLong(req(a, "measured"));
        long intervalNs = Long.parseLong(a.getOrDefault("interval-ns", "0"));
        Path out = Path.of(req(a, "out"));
        boolean fsync = Boolean.parseBoolean(a.getOrDefault("fsync", "false"));

        if (bytes < HEADER_BYTES)
            throw new IllegalArgumentException("bytes must be >= " + HEADER_BYTES);

        long total = warmup + measured;

        try (SingleChronicleQueue q = SingleChronicleQueueBuilder.binary(queue).build()) {
            ExcerptAppender appender = q.acquireAppender();

            // Reusable record buffer: establish `bytes` of content once, then only
            // overwrite the 16-byte header per message (absolute writes) so the
            // producer's per-message cost stays O(1) and does not distort the
            // offered rate at large payloads.
            Bytes<ByteBuffer> buf = Bytes.elasticByteBuffer(bytes);
            for (int i = 0; i < bytes; i++) buf.writeByte((byte) 0);

            long next = System.nanoTime();
            long measuredStartNanoTime = 0;

            for (long seq = 0; seq < total; seq++) {
                long intended;
                if (intervalNs > 0) {
                    next += intervalNs;
                    while (System.nanoTime() < next) Jvm.nanoPause();
                    intended = next;
                } else {
                    intended = System.nanoTime();
                }
                if (seq == warmup) measuredStartNanoTime = System.nanoTime();

                buf.writeLong(0, seq);
                buf.writeLong(8, intended);
                buf.readPositionRemaining(0, bytes);
                appender.writeBytes(buf);
                if (fsync) appender.sync();
            }

            long windowNs = System.nanoTime() - measuredStartNanoTime;
            double achievedMps = measured * 1e9 / windowNs;

            StringBuilder sb = new StringBuilder();
            sb.append("achieved_mps=").append(String.format("%.3f", achievedMps)).append('\n');
            sb.append("producer_window_ns=").append(windowNs).append('\n');
            Files.writeString(out, sb.toString());
        }
    }

    // ---- consumer: latency-measuring tailer -------------------------------

    static void consumer(Map<String, String> a) throws Exception {
        Path queue = Path.of(req(a, "queue"));
        int bytes = Integer.parseInt(req(a, "bytes"));
        long warmup = Long.parseLong(req(a, "warmup"));
        long measured = Long.parseLong(req(a, "measured"));
        Path out = Path.of(req(a, "out"));

        GcMonitor gc = new GcMonitor();
        gc.install();

        // 1 ns .. 1 s, 3 significant figures — identical resolution to the revoq
        // (HdrHistogram-C) side so the encoded histograms line up bucket-for-bucket.
        Histogram hist = new Histogram(1L, 1_000_000_000L, 3);

        long collected = 0;
        long gcCountAtStart = gc.totalCount();
        long gcCountAtMeasured = gcCountAtStart;

        // JIT steady state is confirmed from the compilation clock, not GC. We
        // also force one GC mid-warmup so a collection completes and the heap
        // settles before the measured window (workload.md §3).
        CompilationMXBean comp = ManagementFactory.getCompilationMXBean();
        boolean compSupported = comp != null && comp.isCompilationTimeMonitoringSupported();
        long compAtStart = compSupported ? comp.getTotalCompilationTime() : -1L;
        long compAtMeasured = compAtStart;
        long forceGcAt = warmup / 2;

        try (SingleChronicleQueue q = SingleChronicleQueueBuilder.binary(queue).build()) {
            ExcerptTailer tailer = q.createTailer();
            while (collected < measured) {
                try (DocumentContext dc = tailer.readingDocument()) {
                    if (!dc.isPresent()) {
                        Jvm.nanoPause();
                        continue;
                    }
                    Bytes<?> b = dc.wire().bytes();
                    long seq = b.readLong();
                    long intended = b.readLong();
                    long now = System.nanoTime();

                    if (seq == forceGcAt) System.gc();
                    if (seq == warmup) {
                        gcCountAtMeasured = gc.totalCount();
                        compAtMeasured = compSupported ? comp.getTotalCompilationTime() : -1L;
                        gc.startMeasuredWindow();
                    }
                    if (seq >= warmup) {
                        long lat = now - intended;
                        if (lat < 0) lat = 0;
                        if (lat > 1_000_000_000L) lat = 1_000_000_000L;
                        hist.recordValue(lat);
                        collected++;
                    }
                }
            }
        }
        gc.stopMeasuredWindow();

        // Confirmed = JIT compiled during warmup (compilation clock advanced) AND
        // a GC cycle occurred before measuring (forced mid-warmup above). The GC
        // condition is workload.md §3; the JIT condition no longer piggybacks on
        // GC, which a low-garbage run may never trigger on its own.
        boolean warmupGcSeen = gcCountAtMeasured > gcCountAtStart;
        boolean jitConfirmed = compAtStart >= 0 && compAtMeasured > compAtStart;
        boolean confirmed = jitConfirmed && warmupGcSeen;

        String hdrB64 = encodeHistogram(hist);

        StringBuilder sb = new StringBuilder();
        sb.append("p50_ns=").append(hist.getValueAtPercentile(50.0)).append('\n');
        sb.append("p90_ns=").append(hist.getValueAtPercentile(90.0)).append('\n');
        sb.append("p99_ns=").append(hist.getValueAtPercentile(99.0)).append('\n');
        sb.append("p999_ns=").append(hist.getValueAtPercentile(99.9)).append('\n');
        sb.append("p9999_ns=").append(hist.getValueAtPercentile(99.99)).append('\n');
        sb.append("p99999_ns=").append(hist.getValueAtPercentile(99.999)).append('\n');
        sb.append("max_ns=").append(hist.getMaxValue()).append('\n');
        sb.append("samples=").append(hist.getTotalCount()).append('\n');
        sb.append("payload_bytes=").append(bytes).append('\n');
        sb.append("jdk_vendor=").append(System.getProperty("java.vendor", "")).append('\n');
        sb.append("jdk_version=").append(System.getProperty("java.version", "")).append('\n');
        sb.append("gc_name=").append(gc.names()).append('\n');
        sb.append("gc_pause_count=").append(gc.measuredPauseCount()).append('\n');
        sb.append("gc_pause_max_ns=").append(gc.measuredPauseMaxNs()).append('\n');
        sb.append("jit_steady_state_confirmed=").append(confirmed).append('\n');
        sb.append("chronicle_version=").append(chronicleVersion()).append('\n');
        sb.append("latency_hdr_b64=").append(hdrB64).append('\n');
        Files.writeString(out, sb.toString());
    }

    // ---- helpers ----------------------------------------------------------

    /** Base64 "HISTF..." V2 interchange string, matching HdrHistogram-C hdr_log_encode. */
    static String encodeHistogram(Histogram hist) {
        ByteBuffer bb = ByteBuffer.allocate(hist.getNeededByteBufferCapacity());
        int n = hist.encodeIntoCompressedByteBuffer(bb);
        byte[] arr = new byte[n];
        bb.rewind();
        bb.get(arr, 0, n);
        return Base64.getEncoder().encodeToString(arr);
    }

    static String chronicleVersion() {
        Package p = SingleChronicleQueue.class.getPackage();
        String v = p == null ? null : p.getImplementationVersion();
        return v == null ? "unknown" : v;
    }

    static Map<String, String> parse(String[] args) {
        Map<String, String> m = new HashMap<>();
        for (int i = 1; i < args.length; i++) {
            String s = args[i];
            if (s.startsWith("--") && i + 1 < args.length) {
                m.put(s.substring(2), args[++i]);
            }
        }
        return m;
    }

    static String req(Map<String, String> a, String k) {
        String v = a.get(k);
        if (v == null) throw new IllegalArgumentException("missing --" + k);
        return v;
    }

    /**
     * Captures GC pause count and max pause duration during the measured window
     * only, via GarbageCollectionNotificationInfo (per-collection durations),
     * so GC pauses land in the reported tail and are never warmed past.
     */
    static final class GcMonitor {
        private final AtomicBoolean measuring = new AtomicBoolean(false);
        private final AtomicLong pauseCount = new AtomicLong();
        private final AtomicLong pauseMaxNs = new AtomicLong();

        void install() {
            for (GarbageCollectorMXBean bean : ManagementFactory.getGarbageCollectorMXBeans()) {
                if (bean instanceof NotificationEmitter emitter) {
                    NotificationListener l = (notification, handback) -> {
                        if (!GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION
                                .equals(notification.getType())) return;
                        if (!measuring.get()) return;
                        CompositeData cd = (CompositeData) notification.getUserData();
                        GarbageCollectionNotificationInfo info =
                                GarbageCollectionNotificationInfo.from(cd);
                        long durNs = info.getGcInfo().getDuration() * 1_000_000L; // ms -> ns
                        pauseCount.incrementAndGet();
                        pauseMaxNs.accumulateAndGet(durNs, Math::max);
                    };
                    emitter.addNotificationListener(l, null, null);
                }
            }
        }

        void startMeasuredWindow() { measuring.set(true); }
        void stopMeasuredWindow() { measuring.set(false); }

        long totalCount() {
            long c = 0;
            for (GarbageCollectorMXBean b : ManagementFactory.getGarbageCollectorMXBeans()) {
                long cc = b.getCollectionCount();
                if (cc > 0) c += cc;
            }
            return c;
        }

        long measuredPauseCount() { return pauseCount.get(); }
        long measuredPauseMaxNs() { return pauseMaxNs.get(); }

        String names() {
            StringBuilder sb = new StringBuilder();
            for (GarbageCollectorMXBean b : ManagementFactory.getGarbageCollectorMXBeans()) {
                if (sb.length() > 0) sb.append('+');
                sb.append(b.getName());
            }
            return sb.toString();
        }
    }

    private ChronicleBench() {}
}

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 RevoRacer
package racer.revoq.bench;

import com.sun.management.GarbageCollectionNotificationInfo;
import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;

import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;
import java.io.IOException;
import java.lang.management.CompilationMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Chronicle Queue typed-dispatch driver (semantic group typed_dispatch,
 * durability page_cache). Single process: write a mixed 3-type stream with a
 * {@link MethodWriter}-style proxy (untimed), then time a {@link MethodReader}
 * draining all of it into typed handler methods — Chronicle's idiomatic typed
 * decode+dispatch path. Reports decode throughput (ns/message). See
 * ../workload.md section 11.
 */
public final class ChronicleTypedBench {

    /** The typed schema (see workload.md §11). Public so Chronicle's proxy can implement it. */
    public interface MarketData {
        void trade(long ts, int symbolId, double price);
        void bbo(long ts, int bidSize, int askSize);
        void bookReset(long ts);
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> o = parseArgs(args);
        String queue = o.get("queue");
        long warmup = Long.parseLong(o.getOrDefault("warmup", "200000"));
        long measured = Long.parseLong(o.getOrDefault("measured", "5000000"));
        Path out = Paths.get(o.get("out"));
        long total = warmup + measured;

        GcMonitor gc = new GcMonitor();
        CompilationMXBean comp = ManagementFactory.getCompilationMXBean();
        long compAtStart = (comp != null && comp.isCompilationTimeMonitoringSupported())
            ? comp.getTotalCompilationTime() : -1;
        final long[] compAtMeasured = {compAtStart};

        try (SingleChronicleQueue q = SingleChronicleQueueBuilder.binary(queue).build()) {
            // --- write phase (untimed) ---
            ExcerptAppender appender = q.acquireAppender();
            MarketData writer = appender.methodWriter(MarketData.class);
            for (long i = 0; i < total; i++) {
                long ts = i + 1;
                switch ((int) (i % 3)) {
                    case 0: writer.trade(ts, 101, 100.0 + i); break;
                    case 1: writer.bbo(ts, (int) i, (int) (i + 1)); break;
                    default: writer.bookReset(ts); break;
                }
            }

            // --- read phase: time decode+dispatch of the measured window ---
            final long[] count = {0};
            final long[] startNanos = {0};
            final long[] checksum = {0};
            MarketData impl = new MarketData() {
                @Override public void trade(long ts, int symbolId, double price) {
                    checksum[0] ^= symbolId + (long) price; tick();
                }
                @Override public void bbo(long ts, int bidSize, int askSize) {
                    checksum[0] ^= bidSize + askSize; tick();
                }
                @Override public void bookReset(long ts) {
                    checksum[0] ^= ts; tick();
                }
                private void tick() {
                    long c = count[0]++;
                    if (c == warmup) {
                        gc.startMeasuring();
                        compAtMeasured[0] = (compAtStart >= 0) ? comp.getTotalCompilationTime() : -1;
                        startNanos[0] = System.nanoTime();
                    }
                }
            };

            ExcerptTailer tailer = q.createTailer();
            MethodReader reader = tailer.methodReader(impl);
            while (count[0] < total) {
                if (!reader.readOne()) Jvm.nanoPause();
            }
            long elapsedNs = System.nanoTime() - startNanos[0];
            gc.stop();

            if (checksum[0] == 0x1234_5678_9abc_def0L) System.err.print(""); // keep checksum live

            double mps = elapsedNs > 0 ? (measured / (double) elapsedNs) * 1e9 : 0.0;
            double nsPer = measured > 0 ? elapsedNs / (double) measured : 0.0;
            boolean jitConfirmed = compAtStart >= 0 && compAtMeasured[0] > compAtStart;

            String cv = SingleChronicleQueue.class.getPackage().getImplementationVersion();
            Map<String, String> kv = new LinkedHashMap<>();
            kv.put("messages_per_s", Double.toString(mps));
            kv.put("ns_per_message", Double.toString(nsPer));
            kv.put("measured", Long.toString(measured));
            kv.put("warmup", Long.toString(warmup));
            kv.put("jdk_vendor", System.getProperty("java.vendor", "unknown"));
            kv.put("jdk_version", System.getProperty("java.version", "unknown"));
            kv.put("gc_name", gc.gcName());
            kv.put("gc_pause_count", Long.toString(gc.pauseCount()));
            kv.put("gc_pause_max_ns", Long.toString(gc.pauseMaxNs()));
            kv.put("jit_steady_state_confirmed", Boolean.toString(jitConfirmed));
            kv.put("chronicle_version", cv == null ? "unknown" : cv);
            writeKv(out, kv);
        }
    }

    static Map<String, String> parseArgs(String[] args) {
        Map<String, String> o = new LinkedHashMap<>();
        for (int i = 0; i + 1 < args.length; i++) {
            if (args[i].startsWith("--")) { o.put(args[i].substring(2), args[i + 1]); i++; }
        }
        return o;
    }

    static void writeKv(Path out, Map<String, String> kv) throws IOException {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> e : kv.entrySet()) {
            sb.append(e.getKey()).append('=').append(e.getValue()).append('\n');
        }
        Files.write(out, sb.toString().getBytes(StandardCharsets.UTF_8));
    }

    /** GC pauses counted only during the measured window (per-collection notifications). */
    static final class GcMonitor {
        private final List<GarbageCollectorMXBean> beans = new ArrayList<>();
        private volatile boolean measuring = false;
        private volatile long pauseCount = 0;
        private volatile long pauseMaxNs = 0;
        private String gcName = "unknown";

        GcMonitor() {
            for (GarbageCollectorMXBean bean : ManagementFactory.getGarbageCollectorMXBeans()) {
                beans.add(bean);
                if ("unknown".equals(gcName) && bean.getName() != null) gcName = bean.getName();
                if (bean instanceof NotificationEmitter) {
                    NotificationListener listener = (notification, handback) -> {
                        if (!GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION
                                .equals(notification.getType())) return;
                        if (!measuring) return;
                        GarbageCollectionNotificationInfo info =
                            GarbageCollectionNotificationInfo.from(
                                (CompositeData) notification.getUserData());
                        long durNs = info.getGcInfo().getDuration() * 1_000_000L;
                        pauseCount++;
                        if (durNs > pauseMaxNs) pauseMaxNs = durNs;
                    };
                    ((NotificationEmitter) bean).addNotificationListener(listener, null, null);
                }
            }
        }

        void startMeasuring() { measuring = true; }
        void stop() { measuring = false; }
        String gcName() { return gcName; }
        long pauseCount() { return pauseCount; }
        long pauseMaxNs() { return pauseMaxNs; }
    }
}

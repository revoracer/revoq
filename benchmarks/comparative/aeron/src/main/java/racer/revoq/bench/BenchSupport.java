// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 RevoRacer
package racer.revoq.bench;

import com.sun.management.GarbageCollectionNotificationInfo;
import org.HdrHistogram.Histogram;

import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;
import java.io.IOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;

/**
 * Shared helpers for the Aeron comparative drivers: HdrHistogram base64
 * encoding (byte-compatible with the revoq HdrHistogram-C output), a key=value
 * result writer, and per-collection GC pause accounting scoped to the measured
 * window so pauses land in the reported tail rather than being warmed past.
 */
final class BenchSupport {
    private BenchSupport() {}

    /** V2 compressed HdrHistogram, base64 — identical wire format to revoq's C encoder. */
    static String encodeHistogram(Histogram hist) {
        ByteBuffer bb = ByteBuffer.allocate(hist.getNeededByteBufferCapacity());
        int n = hist.encodeIntoCompressedByteBuffer(bb);
        byte[] arr = new byte[n];
        bb.rewind();
        bb.get(arr, 0, n);
        return Base64.getEncoder().encodeToString(arr);
    }

    static void writeKv(Path out, Map<String, String> kv) throws IOException {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> e : kv.entrySet()) {
            sb.append(e.getKey()).append('=').append(e.getValue()).append('\n');
        }
        Files.write(out, sb.toString().getBytes(StandardCharsets.UTF_8));
    }

    static String jdkVendor() { return System.getProperty("java.vendor", "unknown"); }
    static String jdkVersion() { return System.getProperty("java.version", "unknown"); }

    /**
     * Counts GC pauses and tracks the worst pause, but only while {@code measuring}
     * is true (the measured window). {@code jit_steady_state_confirmed} is the
     * workload's proxy: at least one GC observed before the measured window began.
     */
    static final class GcMonitor {
        private final List<GarbageCollectorMXBean> beans = new ArrayList<>();
        private volatile boolean measuring = false;
        private volatile long pauseCount = 0;
        private volatile long pauseMaxNs = 0;
        private long gcCountAtMeasured = 0;
        private String gcName = "unknown";

        GcMonitor() {
            for (GarbageCollectorMXBean bean : ManagementFactory.getGarbageCollectorMXBeans()) {
                beans.add(bean);
                if ("unknown".equals(gcName) && bean.getName() != null) {
                    gcName = bean.getName();
                }
                if (bean instanceof NotificationEmitter) {
                    NotificationListener listener = (notification, handback) -> {
                        if (!GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION
                                .equals(notification.getType())) {
                            return;
                        }
                        if (!measuring) return;
                        GarbageCollectionNotificationInfo info =
                            GarbageCollectionNotificationInfo.from(
                                (CompositeData) notification.getUserData());
                        long durNs = info.getGcInfo().getDuration() * 1_000_000L; // ms -> ns
                        pauseCount++;
                        if (durNs > pauseMaxNs) pauseMaxNs = durNs;
                    };
                    ((NotificationEmitter) bean).addNotificationListener(listener, null, null);
                }
            }
        }

        private long totalCollections() {
            long c = 0;
            for (GarbageCollectorMXBean b : beans) {
                long n = b.getCollectionCount();
                if (n > 0) c += n;
            }
            return c;
        }

        /** Called when the measured window opens. */
        void startMeasuring() {
            gcCountAtMeasured = totalCollections();
            measuring = true;
        }

        void stop() { measuring = false; }

        String gcName() { return gcName; }
        long pauseCount() { return pauseCount; }
        long pauseMaxNs() { return pauseMaxNs; }
        boolean jitSteadyStateConfirmed() { return gcCountAtMeasured > 0; }
    }
}

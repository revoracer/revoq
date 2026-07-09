// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 RevoRacer
package racer.revoq.bench;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import org.HdrHistogram.Histogram;
import org.agrona.BufferUtil;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;

import java.lang.management.CompilationMXBean;
import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The producer/consumer loops shared by the IPC and Archive drivers. The two
 * differ only in transport wiring (Archive additionally records the stream to
 * disk); the on-wire record layout and measurement are identical here so their
 * results land in the same schema and are directly comparable within a cell.
 *
 * Record layout: [0..8) little-endian long seq, [8..16) long intendedSendNanos,
 * then zero padding to the payload size. The consumer reads the intended send
 * time and measures {@code receive - intended} — coordinated-omission-free.
 */
final class AeronBenchCommon {
    static final int HEADER_BYTES = 16; // seq (8) + intended send epoch-nanos (8)

    private AeronBenchCommon() {}

    static Aeron connect(String aeronDir) {
        Aeron.Context ctx = new Aeron.Context();
        if (aeronDir != null) ctx.aeronDirectoryName(aeronDir);
        return Aeron.connect(ctx);
    }

    static Map<String, String> parseArgs(String[] args) {
        Map<String, String> o = new LinkedHashMap<>();
        for (int i = 1; i < args.length - 1; i++) {
            if (args[i].startsWith("--")) { o.put(args[i].substring(2), args[i + 1]); i++; }
        }
        return o;
    }

    /**
     * Open-loop paced appender. For paced runs (interval &gt; 0) the record
     * carries its intended (scheduled) send time, so a late producer or transport
     * back-pressure lands in the tail rather than being hidden. Writes
     * achieved_mps + producer_window_ns to {@code out}.
     */
    static void runProducer(Aeron aeron, String channel, int streamId, int bytes,
                            long warmup, long measured, long intervalNs, Path out) throws Exception {
        if (bytes < HEADER_BYTES) bytes = HEADER_BYTES;
        final long total = warmup + measured;

        try (Publication pub = aeron.addPublication(channel, streamId)) {
            UnsafeBuffer buf = new UnsafeBuffer(BufferUtil.allocateDirectAligned(bytes, 64));
            for (int i = HEADER_BYTES; i < bytes; i++) buf.putByte(i, (byte) 0);

            // Do not start the schedule until the subscriber(s) are attached, or
            // the offered rate would be distorted by connection setup.
            while (!pub.isConnected()) Thread.onSpinWait();

            long next = System.nanoTime();
            long windowStart = 0;
            for (long seq = 0; seq < total; seq++) {
                long intended;
                if (intervalNs > 0) {
                    next += intervalNs;
                    while (System.nanoTime() < next) Thread.onSpinWait();
                    intended = next;
                } else {
                    intended = System.nanoTime();
                }
                if (seq == warmup) windowStart = System.nanoTime();

                buf.putLong(0, seq);
                buf.putLong(8, intended);
                long r;
                while ((r = pub.offer(buf, 0, bytes)) < 0) {
                    if (r == Publication.CLOSED || r == Publication.MAX_POSITION_EXCEEDED) {
                        throw new IllegalStateException("publication unavailable: " + r);
                    }
                    // BACK_PRESSURED / ADMIN_ACTION / NOT_CONNECTED: retry, keeping
                    // the already-stamped intended time (coordinated omission fix).
                    Thread.onSpinWait();
                }
            }

            long windowNs = System.nanoTime() - windowStart;
            double achieved = windowNs > 0 ? (measured / (double) windowNs) * 1e9 : 0.0;
            Map<String, String> kv = new LinkedHashMap<>();
            kv.put("achieved_mps", Double.toString(achieved));
            kv.put("producer_window_ns", Long.toString(windowNs));
            BenchSupport.writeKv(out, kv);
        }
    }

    /**
     * Latency consumer (source of truth). Records {@code receive - intended} into
     * an HdrHistogram (1 ns .. 1 s, 3 sig figs) over the measured window and emits
     * percentiles + the base64 histogram + a JVM/GC honesty block to {@code out}.
     */
    static void runConsumer(Aeron aeron, String channel, int streamId, int bytes,
                            long warmup, long measured, Path out) throws Exception {
        final Histogram hist = new Histogram(1L, 1_000_000_000L, 3);
        final BenchSupport.GcMonitor gc = new BenchSupport.GcMonitor();
        final long total = warmup + measured;
        final long[] count = {0};

        // JIT steady state is confirmed from the compilation clock (compilation
        // advanced during warmup), not from GC: a zero/low-garbage Aeron run may
        // never GC on its own. We also force one GC mid-warmup so at least one
        // collection completes and the heap settles well before the measured
        // window (workload.md §3), without landing a pause in the measured tail.
        final CompilationMXBean comp = ManagementFactory.getCompilationMXBean();
        final boolean compSupported =
            comp != null && comp.isCompilationTimeMonitoringSupported();
        final long compAtStart = compSupported ? comp.getTotalCompilationTime() : -1L;
        final long[] compAtMeasured = {compAtStart};
        final long forceGcAt = warmup / 2;

        FragmentHandler handler = (buffer, offset, length, header) -> {
            long intended = buffer.getLong(offset + 8);
            long now = System.nanoTime();
            long c = count[0]++;
            if (c == forceGcAt) System.gc();
            if (c == warmup) {
                compAtMeasured[0] = compSupported ? comp.getTotalCompilationTime() : -1L;
                gc.startMeasuring();
            }
            if (c >= warmup) {
                long lat = now - intended;
                if (lat < 0) lat = 0;
                if (lat > 1_000_000_000L) lat = 1_000_000_000L;
                hist.recordValue(lat);
            }
        };

        try (Subscription sub = aeron.addSubscription(channel, streamId)) {
            IdleStrategy idle = new BusySpinIdleStrategy();
            while (count[0] < total) {
                idle.idle(sub.poll(handler, 64));
            }
        }
        gc.stop();

        String aeronVersion = Aeron.class.getPackage().getImplementationVersion();
        Map<String, String> kv = new LinkedHashMap<>();
        kv.put("p50_ns", Long.toString(hist.getValueAtPercentile(50.0)));
        kv.put("p90_ns", Long.toString(hist.getValueAtPercentile(90.0)));
        kv.put("p99_ns", Long.toString(hist.getValueAtPercentile(99.0)));
        kv.put("p999_ns", Long.toString(hist.getValueAtPercentile(99.9)));
        kv.put("p9999_ns", Long.toString(hist.getValueAtPercentile(99.99)));
        kv.put("p99999_ns", Long.toString(hist.getValueAtPercentile(99.999)));
        kv.put("max_ns", Long.toString(hist.getMaxValue()));
        kv.put("samples", Long.toString(hist.getTotalCount()));
        kv.put("payload_bytes", Integer.toString(bytes));
        kv.put("jdk_vendor", BenchSupport.jdkVendor());
        kv.put("jdk_version", BenchSupport.jdkVersion());
        kv.put("gc_name", gc.gcName());
        kv.put("gc_pause_count", Long.toString(gc.pauseCount()));
        kv.put("gc_pause_max_ns", Long.toString(gc.pauseMaxNs()));
        // Confirmed = JIT compiled during warmup (compilation clock advanced) AND
        // a GC cycle was observed before measuring (forced mid-warmup above).
        boolean jitConfirmed = compAtStart >= 0 && compAtMeasured[0] > compAtStart;
        boolean confirmed = jitConfirmed && gc.jitSteadyStateConfirmed();
        kv.put("jit_steady_state_confirmed", Boolean.toString(confirmed));
        kv.put("aeron_version", aeronVersion == null ? "unknown" : aeronVersion);
        kv.put("latency_hdr_b64", BenchSupport.encodeHistogram(hist));
        BenchSupport.writeKv(out, kv);
    }
}

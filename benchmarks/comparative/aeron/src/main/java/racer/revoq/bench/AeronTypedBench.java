// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 RevoRacer
package racer.revoq.bench;

import io.aeron.Aeron;
import org.agrona.BufferUtil;
import org.agrona.concurrent.UnsafeBuffer;

import java.lang.management.CompilationMXBean;
import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Aeron typed-dispatch driver (semantic group typed_dispatch, durability none).
 * Encodes a mixed 3-type stream into a fixed-layout buffer (the untimed "write"
 * phase), then times decoding+dispatching every record through a type switch
 * into a typed handler — the same shape as a low-latency Aeron
 * FragmentHandler/SBE decode. Reports decode throughput (ns/message).
 *
 * <p>This deliberately measures the decode+dispatch path over an in-memory
 * buffer (no live poll/transport), matching the read-back nature of the revoq
 * (TypedDispatcher over a journal) and Chronicle (MethodReader over a queue)
 * typed measurements, so the three isolate the same cost. See workload.md §11.
 */
public final class AeronTypedBench {
    static final int STRIDE = 32; // bytes per encoded record (fixed layout)
    static final byte TRADE = 0, BBO = 1, BOOK_RESET = 2;

    /** Typed handler the decoder dispatches into (a real virtual call, not inlined away). */
    interface Handler {
        void onTrade(long ts, int symbolId, double price);
        void onBbo(long ts, int bidSize, int askSize);
        void onBookReset(long ts);
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> o = parseArgs(args);
        long warmup = Long.parseLong(o.getOrDefault("warmup", "200000"));
        long measured = Long.parseLong(o.getOrDefault("measured", "5000000"));
        Path out = Paths.get(o.get("out"));
        long total = warmup + measured;
        if (total > Integer.MAX_VALUE / STRIDE) {
            throw new IllegalArgumentException("warmup+measured too large for a single buffer");
        }

        // --- encode phase (untimed) ---
        UnsafeBuffer buf = new UnsafeBuffer(
            BufferUtil.allocateDirectAligned((int) total * STRIDE, 64));
        for (long i = 0; i < total; i++) {
            int base = (int) (i * STRIDE);
            long ts = i + 1;
            switch ((int) (i % 3)) {
                case 0:
                    buf.putByte(base, TRADE);
                    buf.putLong(base + 8, ts);
                    buf.putInt(base + 16, 101);
                    buf.putDouble(base + 20, 100.0 + i);
                    break;
                case 1:
                    buf.putByte(base, BBO);
                    buf.putLong(base + 8, ts);
                    buf.putInt(base + 16, (int) i);
                    buf.putInt(base + 20, (int) (i + 1));
                    break;
                default:
                    buf.putByte(base, BOOK_RESET);
                    buf.putLong(base + 8, ts);
                    break;
            }
        }

        final long[] checksum = {0};
        Handler handler = new Handler() {
            @Override public void onTrade(long ts, int symbolId, double price) {
                checksum[0] ^= symbolId + (long) price;
            }
            @Override public void onBbo(long ts, int bidSize, int askSize) {
                checksum[0] ^= bidSize + askSize;
            }
            @Override public void onBookReset(long ts) {
                checksum[0] ^= ts;
            }
        };

        BenchSupport.GcMonitor gc = new BenchSupport.GcMonitor();
        CompilationMXBean comp = ManagementFactory.getCompilationMXBean();
        long compAtStart = (comp != null && comp.isCompilationTimeMonitoringSupported())
            ? comp.getTotalCompilationTime() : -1;
        long compAtMeasured = compAtStart;

        long startNanos = 0;
        for (long i = 0; i < total; i++) {
            if (i == warmup) {
                gc.startMeasuring();
                compAtMeasured = (compAtStart >= 0) ? comp.getTotalCompilationTime() : -1;
                startNanos = System.nanoTime();
            }
            int base = (int) (i * STRIDE);
            byte type = buf.getByte(base);
            long ts = buf.getLong(base + 8);
            switch (type) {           // type dispatch
                case TRADE:
                    handler.onTrade(ts, buf.getInt(base + 16), buf.getDouble(base + 20));
                    break;
                case BBO:
                    handler.onBbo(ts, buf.getInt(base + 16), buf.getInt(base + 20));
                    break;
                case BOOK_RESET:
                    handler.onBookReset(ts);
                    break;
                default:
                    throw new IllegalStateException("bad type " + type);
            }
        }
        long elapsedNs = System.nanoTime() - startNanos;
        gc.stop();

        if (checksum[0] == 0x1234_5678_9abc_def0L) System.err.print(""); // keep checksum live

        double mps = elapsedNs > 0 ? (measured / (double) elapsedNs) * 1e9 : 0.0;
        double nsPer = measured > 0 ? elapsedNs / (double) measured : 0.0;
        boolean jitConfirmed = compAtStart >= 0 && compAtMeasured > compAtStart;

        String av = Aeron.class.getPackage().getImplementationVersion();
        Map<String, String> kv = new LinkedHashMap<>();
        kv.put("messages_per_s", Double.toString(mps));
        kv.put("ns_per_message", Double.toString(nsPer));
        kv.put("measured", Long.toString(measured));
        kv.put("warmup", Long.toString(warmup));
        kv.put("jdk_vendor", BenchSupport.jdkVendor());
        kv.put("jdk_version", BenchSupport.jdkVersion());
        kv.put("gc_name", gc.gcName());
        kv.put("gc_pause_count", Long.toString(gc.pauseCount()));
        kv.put("gc_pause_max_ns", Long.toString(gc.pauseMaxNs()));
        kv.put("jit_steady_state_confirmed", Boolean.toString(jitConfirmed));
        kv.put("aeron_version", av == null ? "unknown" : av);
        BenchSupport.writeKv(out, kv);
    }

    static Map<String, String> parseArgs(String[] args) {
        Map<String, String> o = new LinkedHashMap<>();
        for (int i = 0; i + 1 < args.length; i++) {
            if (args[i].startsWith("--")) { o.put(args[i].substring(2), args[i + 1]); i++; }
        }
        return o;
    }
}

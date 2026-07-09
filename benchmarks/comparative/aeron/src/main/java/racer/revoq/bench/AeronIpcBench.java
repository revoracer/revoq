// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 RevoRacer
package racer.revoq.bench;

import io.aeron.Aeron;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

/**
 * Aeron IPC driver → semantic group {@code volatile_ipc}, durability
 * {@code none}. Producer and consumer are separate OS processes sharing a
 * MediaDriver's aeron dir over the {@code aeron:ipc} channel (no TCP). This is a
 * transient shared-memory ring buffer: charts must state that revoq, even in its
 * non-durable mode, still maintains a replayable journal — strictly more work.
 */
public final class AeronIpcBench {
    static final String CHANNEL = "aeron:ipc";
    static final int STREAM_ID = 1001;

    public static void main(String[] args) throws Exception {
        if (args.length == 0) { usage(); System.exit(2); return; }
        Map<String, String> o = AeronBenchCommon.parseArgs(args);
        String aeronDir = o.get("aeron-dir");
        int bytes = Integer.parseInt(o.get("bytes"));
        long warmup = Long.parseLong(o.get("warmup"));
        long measured = Long.parseLong(o.get("measured"));
        Path out = Paths.get(o.get("out"));

        try (Aeron aeron = AeronBenchCommon.connect(aeronDir)) {
            switch (args[0]) {
                case "producer": {
                    long intervalNs = Long.parseLong(o.getOrDefault("interval-ns", "0"));
                    AeronBenchCommon.runProducer(aeron, CHANNEL, STREAM_ID, bytes,
                        warmup, measured, intervalNs, out);
                    break;
                }
                case "consumer":
                    AeronBenchCommon.runConsumer(aeron, CHANNEL, STREAM_ID, bytes,
                        warmup, measured, out);
                    break;
                default:
                    usage(); System.exit(2);
            }
        }
    }

    static void usage() {
        System.err.println("usage: AeronIpcBench <producer|consumer> --aeron-dir D "
            + "--bytes N --warmup N --measured N [--interval-ns N] --out F");
    }
}

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 RevoRacer
package racer.revoq.bench;

import io.aeron.Aeron;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

/**
 * Aeron Archive driver → semantic group {@code journal_replayable}, durability
 * {@code archive}. The producer starts a LOCAL recording of the stream (the
 * durability guarantee) before publishing; the consumer subscribes to the live
 * stream and measures latency.
 *
 * <p>Semantics stated honestly (see README): the archive persists/replays the
 * stream, but the consumer here reads the <em>live</em> stream while recording
 * is active — it does not replay from disk. This is not identical to
 * Chronicle/revoq, where the consumer reads the persisted log directly. The
 * recording's back-pressure onto the producer is still felt (open-loop retry),
 * so the cost of durability is not hidden.
 */
public final class AeronArchiveBench {
    static final String CHANNEL = "aeron:ipc";
    static final int STREAM_ID = 1002;

    public static void main(String[] args) throws Exception {
        if (args.length == 0) { usage(); System.exit(2); return; }
        Map<String, String> o = AeronBenchCommon.parseArgs(args);
        String aeronDir = o.get("aeron-dir");
        int bytes = Integer.parseInt(o.get("bytes"));
        long warmup = Long.parseLong(o.get("warmup"));
        long measured = Long.parseLong(o.get("measured"));
        Path out = Paths.get(o.get("out"));
        String control = o.getOrDefault("archive-control", "aeron:udp?endpoint=localhost:8010");
        String response = o.getOrDefault("archive-response", "aeron:udp?endpoint=localhost:0");

        switch (args[0]) {
            case "consumer": {
                // Live subscriber; durability comes from the producer's recording.
                try (Aeron aeron = AeronBenchCommon.connect(aeronDir)) {
                    AeronBenchCommon.runConsumer(aeron, CHANNEL, STREAM_ID, bytes,
                        warmup, measured, out);
                }
                break;
            }
            case "producer": {
                long intervalNs = Long.parseLong(o.getOrDefault("interval-ns", "0"));
                Aeron aeron = AeronBenchCommon.connect(aeronDir);
                AeronArchive.Context actx = new AeronArchive.Context()
                    .aeron(aeron)
                    .ownsAeronClient(true)
                    .controlRequestChannel(control)
                    .controlResponseChannel(response);
                try (AeronArchive archive = AeronArchive.connect(actx)) {
                    archive.startRecording(CHANNEL, STREAM_ID, SourceLocation.LOCAL);
                    AeronBenchCommon.runProducer(aeron, CHANNEL, STREAM_ID, bytes,
                        warmup, measured, intervalNs, out);
                }
                break;
            }
            default:
                usage(); System.exit(2);
        }
    }

    static void usage() {
        System.err.println("usage: AeronArchiveBench <producer|consumer> --aeron-dir D "
            + "--bytes N --warmup N --measured N [--interval-ns N] "
            + "[--archive-control CH] [--archive-response CH] --out F");
    }
}

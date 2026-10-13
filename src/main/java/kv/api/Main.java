package kv.api;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import kv.cluster.Coordinator;
import kv.engine.StorageEngine;

public final class Main {

    private static final int DEFAULT_PORT = 8080;
    private static final String DEFAULT_DATA_DIR = "data";
    private static final long DEFAULT_FLUSH_THRESHOLD_BYTES = 4 * 1024 * 1024;
    private static final long DEFAULT_COMPACTION_INTERVAL_MS = 30_000;
    private static final int DEFAULT_COMPACTION_MIN_SSTABLES = 4;

    public static void main(String[] args) throws IOException {
        Path dataDir = Path.of(argValue(args, "--data-dir", DEFAULT_DATA_DIR));
        int port = Integer.parseInt(argValue(args, "--port", String.valueOf(DEFAULT_PORT)));
        long flushThreshold = Long.parseLong(argValue(args, "--flush-threshold-bytes",
                String.valueOf(DEFAULT_FLUSH_THRESHOLD_BYTES)));
        long compactionIntervalMs = Long.parseLong(argValue(args, "--compaction-interval-ms",
                String.valueOf(DEFAULT_COMPACTION_INTERVAL_MS)));
        int compactionMinSsTables = Integer.parseInt(argValue(args, "--compaction-min-sstables",
                String.valueOf(DEFAULT_COMPACTION_MIN_SSTABLES)));
        String nodeId = argValue(args, "--node-id", "node-1");
        String peers = argValue(args, "--peers", "");

        StorageEngine engine = new StorageEngine(dataDir, flushThreshold);
        if (compactionIntervalMs > 0) {
            engine.startBackgroundCompaction(compactionIntervalMs, compactionMinSsTables);
        }

        Coordinator coordinator = peers.isEmpty()
                ? Coordinator.singleNode(engine)
                : new Coordinator(nodeId, parsePeers(peers), engine);
        new HttpServer(coordinator).start(port);
    }

    /** Parses "--peers n1=host:9091,n2=host:9092,n3=host:9093" (all members, self included). */
    private static Map<String, String> parsePeers(String peers) {
        Map<String, String> members = new HashMap<>();
        for (String member : peers.split(",")) {
            String[] parts = member.trim().split("=", 2);
            if (parts.length != 2 || parts[0].isBlank() || parts[1].isBlank()) {
                throw new IllegalArgumentException("bad --peers entry: '" + member
                        + "' (expected nodeId=host:port)");
            }
            members.put(parts[0], parts[1]);
        }
        return members;
    }

    private static String argValue(String[] args, String flag, String defaultValue) {
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equals(flag)) {
                return args[i + 1];
            }
        }
        return defaultValue;
    }
}

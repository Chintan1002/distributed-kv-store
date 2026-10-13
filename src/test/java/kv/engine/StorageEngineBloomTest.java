package kv.engine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Bloom filters: GETs for missing keys skip SSTable reads, proven by counters. */
class StorageEngineBloomTest {

    private static final long TINY_THRESHOLD = 64;

    @TempDir
    Path dataDir;

    private static void fillAcrossSeveralSsTables(StorageEngine engine) throws IOException {
        for (int i = 0; i < 30; i++) {
            engine.put("key-" + i, "value-" + i);
        }
    }

    @Test
    void missingKeyGetsAreAnsweredWithoutDiskScans() throws IOException {
        try (StorageEngine engine = new StorageEngine(dataDir, TINY_THRESHOLD)) {
            fillAcrossSeveralSsTables(engine);
            assertTrue(engine.ssTableCount() >= 2, "need several SSTables for the test to mean anything");

            for (int i = 0; i < 100; i++) {
                assertTrue(engine.get("missing-" + i).isEmpty());
            }

            StorageEngine.StatsSnapshot stats = engine.stats();
            assertTrue(stats.bloomSkips() > 0, "bloom filters never consulted");
            // 100 misses × N tables ≈ several hundred probes; at a 1% fp rate
            // virtually all must be answered by the filter, not the disk.
            assertTrue(stats.bloomHitRate() > 0.9,
                    "bloom hit rate too low: " + stats.bloomHitRate());
            assertTrue(stats.ssTableScans() <= 20,
                    "too many disk scans for missing keys: " + stats.ssTableScans());
        }
    }

    @Test
    void presentKeysAreStillAlwaysFound() throws IOException {
        try (StorageEngine engine = new StorageEngine(dataDir, TINY_THRESHOLD)) {
            fillAcrossSeveralSsTables(engine);
            for (int i = 0; i < 30; i++) { // a false negative would break this
                assertEquals(Optional.of("value-" + i), engine.get("key-" + i));
            }
        }
    }

    @Test
    void bloomSidecarsAreWrittenAndReloadedOnRestart() throws IOException {
        try (StorageEngine engine = new StorageEngine(dataDir, TINY_THRESHOLD)) {
            fillAcrossSeveralSsTables(engine);
        }

        try (Stream<Path> files = Files.list(dataDir)) {
            assertTrue(files.anyMatch(p -> p.getFileName().toString().endsWith(".bloom")),
                    "expected .bloom sidecar files on disk");
        }

        try (StorageEngine restarted = new StorageEngine(dataDir, TINY_THRESHOLD)) {
            for (int i = 0; i < 50; i++) {
                assertTrue(restarted.get("missing-" + i).isEmpty());
            }
            assertTrue(restarted.stats().bloomSkips() > 0,
                    "filters not active after restart — sidecars not loaded?");
            for (int i = 0; i < 30; i++) {
                assertEquals(Optional.of("value-" + i), restarted.get("key-" + i));
            }
        }
    }

    @Test
    void missingSidecarIsToleratedByScanningTheFile() throws IOException {
        try (StorageEngine engine = new StorageEngine(dataDir, TINY_THRESHOLD)) {
            fillAcrossSeveralSsTables(engine);
        }
        // Simulate a crash between data-file and sidecar write.
        try (Stream<Path> files = Files.list(dataDir)) {
            for (Path p : files.filter(p -> p.getFileName().toString().endsWith(".bloom")).toList()) {
                Files.delete(p);
            }
        }

        try (StorageEngine restarted = new StorageEngine(dataDir, TINY_THRESHOLD)) {
            for (int i = 0; i < 30; i++) { // correctness must not depend on the filter
                assertEquals(Optional.of("value-" + i), restarted.get("key-" + i));
            }
            assertTrue(restarted.get("missing").isEmpty());
        }
    }
}

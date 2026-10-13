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

/** MemTable flush behavior: flush to SSTables, bounded memory, correct read-merge. */
class StorageEngineFlushTest {

    /** Tiny threshold so a handful of writes forces a flush. */
    private static final long TINY_THRESHOLD = 64;

    @TempDir
    Path dataDir;

    private long ssTableFilesOnDisk() throws IOException {
        try (Stream<Path> files = Files.list(dataDir)) {
            return files.filter(p -> p.getFileName().toString().startsWith("sstable-")).count();
        }
    }

    @Test
    void crossingThresholdCreatesSsTableAndBoundsMemory() throws IOException {
        try (StorageEngine engine = new StorageEngine(dataDir, TINY_THRESHOLD)) {
            for (int i = 0; i < 10; i++) {
                engine.put("key-" + i, "value-" + i); // ~12 bytes each
            }

            assertTrue(engine.ssTableCount() >= 1, "expected at least one flush");
            assertTrue(ssTableFilesOnDisk() >= 1, "expected sstable file on disk");
            assertTrue(engine.memTableBytes() < TINY_THRESHOLD, "memory must stay bounded");

            for (int i = 0; i < 10; i++) { // flushed and unflushed keys both readable
                assertEquals(Optional.of("value-" + i), engine.get("key-" + i));
            }
        }
    }

    @Test
    void walIsResetAfterFlush() throws IOException {
        try (StorageEngine engine = new StorageEngine(dataDir, TINY_THRESHOLD)) {
            // Exactly cross the threshold so the last write triggers a flush.
            engine.put("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
            assertEquals(1, engine.ssTableCount());
            assertEquals(0, Files.size(dataDir.resolve("wal.log")),
                    "WAL must be empty right after a flush");
        }
    }

    @Test
    void flushedDataSurvivesRestart() throws IOException {
        try (StorageEngine engine = new StorageEngine(dataDir, TINY_THRESHOLD)) {
            for (int i = 0; i < 10; i++) {
                engine.put("key-" + i, "value-" + i);
            }
            assertTrue(engine.ssTableCount() >= 1);
        }

        try (StorageEngine restarted = new StorageEngine(dataDir, TINY_THRESHOLD)) {
            for (int i = 0; i < 10; i++) {
                assertEquals(Optional.of("value-" + i), restarted.get("key-" + i));
            }
        }
    }

    @Test
    void newestValueWinsOverFlushedOlderVersions() throws IOException {
        try (StorageEngine engine = new StorageEngine(dataDir, TINY_THRESHOLD)) {
            engine.put("city", "Mumbai");
            forceFlush(engine); // "Mumbai" now lives in sstable-0
            assertEquals(1, engine.ssTableCount());

            engine.put("city", "Pune");
            forceFlush(engine); // "Pune" in the newer sstable-1
            engine.put("city", "Delhi"); // newest, in the MemTable

            assertEquals(Optional.of("Delhi"), engine.get("city"));
        }

        try (StorageEngine restarted = new StorageEngine(dataDir, TINY_THRESHOLD)) {
            assertEquals(Optional.of("Delhi"), restarted.get("city"));
        }
    }

    @Test
    void tombstoneMasksFlushedValueEvenAfterRestart() throws IOException {
        try (StorageEngine engine = new StorageEngine(dataDir, TINY_THRESHOLD)) {
            engine.put("doomed", "still-here");
            forceFlush(engine); // value is in an SSTable now
            engine.delete("doomed");
            assertTrue(engine.get("doomed").isEmpty(), "tombstone must mask the SSTable value");

            forceFlush(engine); // tombstone itself flushed to a newer SSTable
            assertTrue(engine.get("doomed").isEmpty());
        }

        try (StorageEngine restarted = new StorageEngine(dataDir, TINY_THRESHOLD)) {
            assertTrue(restarted.get("doomed").isEmpty(),
                    "tombstone in newer SSTable must still mask the older value");
        }
    }

    /** Pads the MemTable past the threshold so the engine flushes. */
    private static void forceFlush(StorageEngine engine) throws IOException {
        engine.put("padding-key-to-force-flush", "x".repeat((int) TINY_THRESHOLD));
    }
}

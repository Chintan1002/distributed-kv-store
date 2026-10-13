package kv.engine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Compaction drops old versions and tombstones; disk shrinks. */
class StorageEngineCompactionTest {

    private static final long TINY_THRESHOLD = 64;

    @TempDir
    Path dataDir;

    private long ssTableBytesOnDisk() throws IOException {
        try (Stream<Path> files = Files.list(dataDir)) {
            return files.filter(p -> p.getFileName().toString().endsWith(".db"))
                    .mapToLong(p -> {
                        try {
                            return Files.size(p);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .sum();
        }
    }

    private long ssTableFilesOnDisk() throws IOException {
        try (Stream<Path> files = Files.list(dataDir)) {
            return files.filter(p -> p.getFileName().toString().endsWith(".db")).count();
        }
    }

    @Test
    void overwritesAreDroppedAndDiskShrinks() throws IOException {
        try (StorageEngine engine = new StorageEngine(dataDir, TINY_THRESHOLD)) {
            // Every put exceeds the threshold → one SSTable per overwrite.
            for (int i = 0; i < 20; i++) {
                engine.put("hot-key", "version-" + i + "-" + "x".repeat(70));
            }
            assertTrue(engine.ssTableCount() >= 10, "expected many SSTables before compaction");
            long bytesBefore = ssTableBytesOnDisk();

            engine.compact();

            assertEquals(1, engine.ssTableCount(), "full compaction must leave one table");
            assertEquals(1, ssTableFilesOnDisk(), "old SSTable files must be deleted");
            long bytesAfter = ssTableBytesOnDisk();
            assertTrue(bytesAfter < bytesBefore / 5,
                    "disk did not shrink: " + bytesBefore + " -> " + bytesAfter);
            assertTrue(engine.get("hot-key").orElseThrow().startsWith("version-19"),
                    "newest version must survive compaction");
        }
    }

    @Test
    void compactedTableContainsExactlyOneVersionPerKey() throws IOException {
        try (StorageEngine engine = new StorageEngine(dataDir, TINY_THRESHOLD)) {
            for (int i = 0; i < 10; i++) {
                engine.put("key", "version-" + i + "-" + "x".repeat(70));
            }
            engine.compact();
        }

        // Reopen and count raw entries in the single surviving table.
        try (Stream<Path> files = Files.list(dataDir)) {
            Path only = files.filter(p -> p.getFileName().toString().endsWith(".db"))
                    .findFirst().orElseThrow();
            AtomicInteger entries = new AtomicInteger();
            SSTable.open(only).forEachEntry((k, e) -> entries.incrementAndGet());
            assertEquals(1, entries.get(), "old versions must not survive compaction");
        }
    }

    @Test
    void tombstonesAndTheirShadowedValuesAreGone() throws IOException {
        try (StorageEngine engine = new StorageEngine(dataDir, TINY_THRESHOLD)) {
            engine.put("doomed", "value-" + "x".repeat(70)); // flushed to its own table
            engine.put("keeper", "stays-" + "x".repeat(70)); // flushed too
            engine.delete("doomed");
            engine.put("pad", "y".repeat(70)); // pushes the tombstone into a flush

            engine.compact();

            assertTrue(engine.get("doomed").isEmpty());
            assertTrue(engine.get("keeper").isPresent());

            // The tombstone itself must be gone from the compacted file.
            AtomicInteger tombstones = new AtomicInteger();
            try (Stream<Path> files = Files.list(dataDir)) {
                for (Path p : files.filter(f -> f.getFileName().toString().endsWith(".db")).toList()) {
                    SSTable.open(p).forEachEntry((k, e) -> {
                        if (e.isTombstone()) {
                            tombstones.incrementAndGet();
                        }
                    });
                }
            }
            assertEquals(0, tombstones.get(), "full compaction must drop tombstones");
        }

        try (StorageEngine restarted = new StorageEngine(dataDir, TINY_THRESHOLD)) {
            assertTrue(restarted.get("doomed").isEmpty(), "delete must survive compaction + restart");
            assertTrue(restarted.get("keeper").isPresent());
        }
    }

    @Test
    void compactionPreservesAllLiveKeysAcrossRestart() throws IOException {
        try (StorageEngine engine = new StorageEngine(dataDir, TINY_THRESHOLD)) {
            for (int i = 0; i < 30; i++) {
                engine.put("key-" + i, "value-" + i);
            }
            engine.compact();
            assertTrue(engine.stats().compactions() >= 1);
            for (int i = 0; i < 30; i++) {
                assertEquals(Optional.of("value-" + i), engine.get("key-" + i));
            }
        }

        try (StorageEngine restarted = new StorageEngine(dataDir, TINY_THRESHOLD)) {
            for (int i = 0; i < 30; i++) {
                assertEquals(Optional.of("value-" + i), restarted.get("key-" + i));
            }
        }
    }

    @Test
    void backgroundCompactionRunsOnItsOwn() throws IOException, InterruptedException {
        try (StorageEngine engine = new StorageEngine(dataDir, TINY_THRESHOLD)) {
            engine.startBackgroundCompaction(50, 2);
            for (int i = 0; i < 30; i++) {
                engine.put("key-" + i, "value-" + i);
            }
            assertTrue(engine.ssTableCount() >= 2);

            long deadline = System.currentTimeMillis() + 5_000;
            while (engine.ssTableCount() > 1 && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
            }
            assertEquals(1, engine.ssTableCount(), "background compactor never ran");

            for (int i = 0; i < 30; i++) { // reads correct throughout
                assertEquals(Optional.of("value-" + i), engine.get("key-" + i));
            }
        } // close() must also stop the compactor thread without hanging
    }
}

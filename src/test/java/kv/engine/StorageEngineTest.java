package kv.engine;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StorageEngineTest {

    @TempDir
    Path dataDir;

    /** The core durability test: write → restart → read it back. */
    @Test
    void writeSurvivesRestart() throws IOException {
        try (StorageEngine engine = new StorageEngine(dataDir)) {
            engine.put("city", "Mumbai");
        }

        try (StorageEngine restarted = new StorageEngine(dataDir)) {
            assertEquals(Optional.of("Mumbai"), restarted.get("city"));
        }
    }

    @Test
    void overwriteSurvivesRestartWithLatestValue() throws IOException {
        try (StorageEngine engine = new StorageEngine(dataDir)) {
            engine.put("city", "Mumbai");
            engine.put("city", "Pune");
        }

        try (StorageEngine restarted = new StorageEngine(dataDir)) {
            assertEquals(Optional.of("Pune"), restarted.get("city"));
        }
    }

    @Test
    void deleteSurvivesRestart() throws IOException {
        try (StorageEngine engine = new StorageEngine(dataDir)) {
            engine.put("doomed", "bye");
            engine.delete("doomed");
        }

        try (StorageEngine restarted = new StorageEngine(dataDir)) {
            assertTrue(restarted.get("doomed").isEmpty());
        }
    }

    @Test
    void survivesManyRestarts() throws IOException {
        for (int i = 0; i < 5; i++) {
            try (StorageEngine engine = new StorageEngine(dataDir)) {
                if (i > 0) {
                    assertEquals(Optional.of("v" + (i - 1)), engine.get("k"));
                }
                engine.put("k", "v" + i);
            }
        }
    }

    @Test
    void basicPutGetDeleteWithinOneProcess() throws IOException {
        try (StorageEngine engine = new StorageEngine(dataDir)) {
            engine.put("k1", "v1");
            assertEquals(Optional.of("v1"), engine.get("k1"));

            engine.put("k1", "v2");
            assertEquals(Optional.of("v2"), engine.get("k1"));

            engine.delete("k1");
            assertTrue(engine.get("k1").isEmpty());

            assertTrue(engine.get("never-written").isEmpty());
        }
    }
}

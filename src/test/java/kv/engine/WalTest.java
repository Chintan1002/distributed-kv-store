package kv.engine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WalTest {

    @TempDir
    Path dir;

    /** Collects replayed records as "PUT key=value" / "DELETE key" strings. */
    private static final class RecordingHandler implements Wal.ReplayHandler {
        final List<String> records = new ArrayList<>();

        @Override
        public void onPut(String key, String value) {
            records.add("PUT " + key + "=" + value);
        }

        @Override
        public void onDelete(String key) {
            records.add("DELETE " + key);
        }
    }

    private Path walFile() {
        return dir.resolve("wal.log");
    }

    @Test
    void replayReturnsAppendedRecordsInOrder() throws IOException {
        try (Wal wal = new Wal(walFile())) {
            wal.appendPut("a", "1");
            wal.appendPut("b", "2");
            wal.appendDelete("a");
            wal.appendPut("b", "3");
        }

        RecordingHandler handler = new RecordingHandler();
        Wal.replay(walFile(), handler);
        assertEquals(List.of("PUT a=1", "PUT b=2", "DELETE a", "PUT b=3"), handler.records);
    }

    @Test
    void replayOfMissingFileIsEmpty() throws IOException {
        RecordingHandler handler = new RecordingHandler();
        Wal.replay(walFile(), handler);
        assertTrue(handler.records.isEmpty());
    }

    @Test
    void replayStopsCleanlyAtTornTailRecord() throws IOException {
        try (Wal wal = new Wal(walFile())) {
            wal.appendPut("safe", "durable");
        }
        // Simulate a crash mid-append: a partial record at the tail
        // (op byte + half a length field, then EOF).
        Files.write(walFile(), new byte[] {1, 0, 0}, StandardOpenOption.APPEND);

        RecordingHandler handler = new RecordingHandler();
        Wal.replay(walFile(), handler);
        assertEquals(List.of("PUT safe=durable"), handler.records);
    }

    @Test
    void handlesEmptyValuesAndNonAsciiKeys() throws IOException {
        try (Wal wal = new Wal(walFile())) {
            wal.appendPut("empty", "");
            wal.appendPut("città-🔑", "ναι");
        }

        RecordingHandler handler = new RecordingHandler();
        Wal.replay(walFile(), handler);
        assertEquals(List.of("PUT empty=", "PUT città-🔑=ναι"), handler.records);
    }
}

package kv.engine;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;
import java.util.TreeMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SSTableTest {

    @TempDir
    Path dir;

    private SSTable table;

    @BeforeEach
    void writeTable() throws IOException {
        TreeMap<String, Entry> entries = new TreeMap<>();
        entries.put("apple", Entry.of("1"));
        entries.put("mango", Entry.of("2"));
        entries.put("removed", Entry.tombstone());
        entries.put("zebra", Entry.of("3"));
        table = SSTable.writeFrom(dir.resolve("sstable-0.db"), entries.entrySet());
    }

    @Test
    void findsStoredValues() throws IOException {
        assertEquals(Optional.of(Entry.of("1")), table.get("apple"));
        assertEquals(Optional.of(Entry.of("2")), table.get("mango"));
        assertEquals(Optional.of(Entry.of("3")), table.get("zebra")); // last entry
    }

    @Test
    void tombstoneIsReturnedAsPresentEntry() throws IOException {
        Entry entry = table.get("removed").orElseThrow();
        assertTrue(entry.isTombstone());
    }

    @Test
    void keyBetweenStoredKeysIsAbsent() throws IOException {
        // "banana" sorts between "apple" and "mango": exercises the
        // early-exit path in the sorted scan.
        assertTrue(table.get("banana").isEmpty());
    }

    @Test
    void keyPastTheEndIsAbsent() throws IOException {
        assertTrue(table.get("zzz").isEmpty());
    }

    @Test
    void reopeningTheFileReadsTheSameData() throws IOException {
        SSTable reopened = SSTable.open(table.file());
        assertEquals(Optional.of(Entry.of("2")), reopened.get("mango"));
    }
}

package kv.engine;

import java.util.List;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MemTableTest {

    @Test
    void iteratesInAscendingKeyOrder() {
        MemTable memTable = new MemTable();
        memTable.put("cherry", "3");
        memTable.put("apple", "1");
        memTable.put("banana", "2");

        List<String> keys = memTable.entries().stream().map(e -> e.getKey()).toList();
        assertEquals(List.of("apple", "banana", "cherry"), keys);
    }

    @Test
    void deleteInsertsTombstoneRatherThanRemoving() {
        MemTable memTable = new MemTable();
        memTable.put("k", "v");
        memTable.delete("k");

        Entry entry = memTable.get("k").orElseThrow();
        assertTrue(entry.isTombstone());
        assertTrue(entry.asOptional().isEmpty());
    }

    @Test
    void missingKeyIsAbsentNotTombstone() {
        MemTable memTable = new MemTable();
        assertTrue(memTable.get("never").isEmpty());
    }

    @Test
    void byteAccountingGrowsAndClearResets() {
        MemTable memTable = new MemTable();
        assertEquals(0, memTable.approximateBytes());

        memTable.put("key", "value"); // 3 + 5
        assertEquals(8, memTable.approximateBytes());

        memTable.clear();
        assertEquals(0, memTable.approximateBytes());
        assertTrue(memTable.isEmpty());
    }
}

package kv.engine;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * In-memory table of recent writes: a ConcurrentSkipListMap because it is
 * both thread-safe (concurrent writers, no global lock) and sorted, so a
 * flush can stream entries out in key order to produce a sorted SSTable.
 */
public final class MemTable {

    private final ConcurrentSkipListMap<String, Entry> map = new ConcurrentSkipListMap<>();
    private final AtomicLong approximateBytes = new AtomicLong();

    public void put(String key, String value) {
        map.put(key, Entry.of(value));
        // Approximate on purpose: overwrites double-count, which only makes
        // flushes slightly more eager, never lets memory grow unbounded.
        approximateBytes.addAndGet(key.length() + value.length());
    }

    /** A delete is an insert of a tombstone, not a removal. */
    public void delete(String key) {
        map.put(key, Entry.tombstone());
        approximateBytes.addAndGet(key.length());
    }

    /** Empty means "not in the MemTable" — older SSTables must still be consulted. */
    public Optional<Entry> get(String key) {
        return Optional.ofNullable(map.get(key));
    }

    /** Entries in ascending key order, ready to stream into an SSTable. */
    public Set<Map.Entry<String, Entry>> entries() {
        return map.entrySet();
    }

    public long approximateBytes() {
        return approximateBytes.get();
    }

    public boolean isEmpty() {
        return map.isEmpty();
    }

    public void clear() {
        map.clear();
        approximateBytes.set(0);
    }
}

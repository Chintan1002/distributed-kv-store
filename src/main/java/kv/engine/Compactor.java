package kv.engine;

import java.io.IOException;
import java.util.List;
import java.util.TreeMap;

/**
 * Merge logic for compaction: collapse many SSTables into the
 * minimal equivalent set of entries — the newest version of each key wins,
 * and tombstones are dropped together with everything they shadow.
 *
 * <p>Dropping tombstones is safe ONLY in a full compaction, i.e. when the
 * input includes the oldest table. If a table older than the merged set
 * survived, removing a tombstone would "resurrect" the value it was masking.
 */
final class Compactor {

    private Compactor() {
    }

    /**
     * Merges tables given newest first. Iterating newest→oldest with
     * putIfAbsent means the first (newest) version of a key sticks and older
     * versions are discarded on arrival.
     */
    static TreeMap<String, Entry> mergeNewestWins(List<SSTable> newestFirst) throws IOException {
        TreeMap<String, Entry> merged = new TreeMap<>();
        for (SSTable table : newestFirst) {
            table.forEachEntry(merged::putIfAbsent);
        }
        // Full compaction: nothing older remains, so tombstones have done
        // their masking job and can vanish along with the versions they hid.
        merged.values().removeIf(Entry::isTombstone);
        return merged;
    }
}

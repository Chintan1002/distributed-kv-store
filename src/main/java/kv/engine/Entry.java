package kv.engine;

import java.util.Optional;

/**
 * A stored value or a tombstone. Deletes can't erase from immutable SSTables,
 * so they write a tombstone marker that masks any older value for the same
 * key in older files. Compaction eventually drops
 * tombstones and the data they shadow together.
 */
public record Entry(String value, boolean isTombstone) {

    public static Entry of(String value) {
        return new Entry(value, false);
    }

    public static Entry tombstone() {
        return new Entry(null, true);
    }

    /** The client-visible result: a tombstone reads as "not found". */
    public Optional<String> asOptional() {
        return isTombstone ? Optional.empty() : Optional.of(value);
    }
}

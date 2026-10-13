package kv.engine;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.Optional;

/**
 * Sorted String Table: an immutable on-disk file of key-ordered entries,
 * written once (fsynced) and never modified — only compaction may
 * replace it with a merged successor. Immutability is what makes concurrent
 * reads and merges safe.
 *
 * <p>On-disk entry format (same manual framing style as the WAL):
 * <pre>
 *   entry := keyLen(int32) keyBytes op(1 byte) [valueLen(int32) valueBytes]
 *   op    := 1 = PUT (has value), 2 = TOMBSTONE (no value)
 * </pre>
 *
 * <p>Reads are a sequential scan with early exit (keys are sorted, so we
 * stop as soon as we pass the target), and the Bloom filter lets most
 * lookups skip the file entirely.
 */
public final class SSTable {

    private static final byte OP_PUT = 1;
    private static final byte OP_TOMBSTONE = 2;

    /** Target false-positive rate for the per-table Bloom filter. */
    private static final double BLOOM_FP_RATE = 0.01;

    private final Path file;
    /** Null when the sidecar is missing (e.g. crash between data and filter write): always scan. */
    private final BloomFilter bloom;

    private SSTable(Path file, BloomFilter bloom) {
        this.file = file;
        this.bloom = bloom;
    }

    /** Opens an existing SSTable file (e.g. found on disk at startup). */
    public static SSTable open(Path file) throws IOException {
        Path bloomFile = bloomFileFor(file);
        BloomFilter bloom = Files.exists(bloomFile) ? BloomFilter.readFrom(bloomFile) : null;
        return new SSTable(file, bloom);
    }

    /**
     * Writes a new SSTable (and its Bloom-filter sidecar) from entries that
     * MUST be in ascending key order (a MemTable snapshot). Both files are
     * fsynced before this returns, so a flush may only discard the
     * MemTable/WAL afterwards.
     */
    public static SSTable writeFrom(Path file, java.util.Set<Map.Entry<String, Entry>> sortedEntries)
            throws IOException {
        BloomFilter bloom = BloomFilter.create(sortedEntries.size(), BLOOM_FP_RATE);
        try (FileChannel channel = FileChannel.open(file,
                StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            for (Map.Entry<String, Entry> e : sortedEntries) {
                // Tombstones go in the filter too: hiding one would make a GET
                // skip this file and resurrect an older value from a older table.
                bloom.add(e.getKey());
                ByteBuffer record = encode(e.getKey(), e.getValue());
                while (record.hasRemaining()) {
                    channel.write(record);
                }
            }
            channel.force(true);
        }
        bloom.writeTo(bloomFileFor(file));
        return new SSTable(file, bloom);
    }

    static Path bloomFileFor(Path dataFile) {
        return dataFile.resolveSibling(dataFile.getFileName() + ".bloom");
    }

    /**
     * Bloom skip-check: false means the key is definitely not in this table
     * (no disk I/O needed). No filter loaded means "unknown" — must scan.
     */
    public boolean mightContain(String key) {
        return bloom == null || bloom.mightContain(key);
    }

    private static ByteBuffer encode(String key, Entry entry) {
        byte[] k = key.getBytes(StandardCharsets.UTF_8);
        if (entry.isTombstone()) {
            return ByteBuffer.allocate(4 + k.length + 1)
                    .putInt(k.length).put(k).put(OP_TOMBSTONE).flip();
        }
        byte[] v = entry.value().getBytes(StandardCharsets.UTF_8);
        return ByteBuffer.allocate(4 + k.length + 1 + 4 + v.length)
                .putInt(k.length).put(k).put(OP_PUT).putInt(v.length).put(v).flip();
    }

    /**
     * Looks up a key. Empty means "not in this table" — the caller must keep
     * checking older tables. A present tombstone means "deleted; stop looking".
     */
    public Optional<Entry> get(String key) throws IOException {
        try (DataInputStream in = new DataInputStream(
                new BufferedInputStream(Files.newInputStream(file)))) {
            while (true) {
                int keyLen;
                try {
                    keyLen = in.readInt();
                } catch (EOFException end) {
                    return Optional.empty();
                }
                String currentKey = new String(in.readNBytes(keyLen), StandardCharsets.UTF_8);
                byte op = in.readByte();
                int cmp = currentKey.compareTo(key);
                if (cmp > 0) {
                    return Optional.empty(); // sorted file: we passed the target
                }
                if (cmp == 0) {
                    if (op == OP_TOMBSTONE) {
                        return Optional.of(Entry.tombstone());
                    }
                    int valueLen = in.readInt();
                    return Optional.of(Entry.of(
                            new String(in.readNBytes(valueLen), StandardCharsets.UTF_8)));
                }
                if (op == OP_PUT) {
                    in.skipNBytes(in.readInt()); // not our key: skip the value
                }
            }
        }
    }

    public Path file() {
        return file;
    }

    /** Sequential-read callback used by compaction and diagnostics. */
    interface EntryConsumer {
        void accept(String key, Entry entry);
    }

    /** Streams every entry in key order (the file's physical order). */
    void forEachEntry(EntryConsumer consumer) throws IOException {
        try (DataInputStream in = new DataInputStream(
                new BufferedInputStream(Files.newInputStream(file)))) {
            while (true) {
                int keyLen;
                try {
                    keyLen = in.readInt();
                } catch (EOFException end) {
                    return;
                }
                String key = new String(in.readNBytes(keyLen), StandardCharsets.UTF_8);
                if (in.readByte() == OP_TOMBSTONE) {
                    consumer.accept(key, Entry.tombstone());
                } else {
                    int valueLen = in.readInt();
                    consumer.accept(key, Entry.of(
                            new String(in.readNBytes(valueLen), StandardCharsets.UTF_8)));
                }
            }
        }
    }

    /**
     * Removes this table's files after compaction has replaced it. The one
     * sanctioned "mutation" of an SSTable: deletion of the whole file once
     * no reader list references it.
     */
    void deleteFiles() throws IOException {
        Files.deleteIfExists(file);
        Files.deleteIfExists(bloomFileFor(file));
    }
}

package kv.engine;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

/**
 * Single-node LSM storage engine facade — the only class the network layer
 * talks to (this package stays free of HTTP/network imports so it can be
 * unit-tested in isolation).
 *
 * <p>Write path: WAL append (fsync) → MemTable. When the
 * MemTable crosses {@code flushThresholdBytes} it is flushed to a new
 * immutable SSTable and the WAL is reset, so memory stays bounded.
 *
 * <p>Read path: MemTable first (newest data), then SSTables newest → oldest;
 * the first entry found wins, and a tombstone reads as "not found".
 *
 * <p>Concurrency: puts/deletes/gets run concurrently (concurrent MemTable,
 * synchronized WAL appends). Only the multi-step flush takes the exclusive
 * write lock; gets stay lock-free because flush
 * ordering keeps every key readable at every instant — the SSTable is
 * registered before the MemTable is cleared.
 */
public final class StorageEngine implements Closeable {

    static final String WAL_FILE = "wal.log";
    static final String SSTABLE_PREFIX = "sstable-";
    static final String SSTABLE_SUFFIX = ".db";
    private static final long DEFAULT_FLUSH_THRESHOLD_BYTES = 4 * 1024 * 1024;

    private final Path dataDir;
    private final long flushThresholdBytes;
    private final MemTable memTable = new MemTable();
    /** Newest first. Copy-on-write: flushes are rare, reads iterate constantly. */
    private final List<SSTable> ssTables = new CopyOnWriteArrayList<>();
    private final ReentrantReadWriteLock flushLock = new ReentrantReadWriteLock();
    private final Wal wal;
    private final AtomicLong nextSsTableSeq = new AtomicLong();
    private volatile boolean closed;
    private Thread compactionThread; // guarded by synchronized(this)

    // Read-path counters: prove that Bloom filters skip disk reads.
    private final LongAdder bloomSkips = new LongAdder();
    private final LongAdder ssTableScans = new LongAdder();
    private final LongAdder ssTableScanMisses = new LongAdder();
    private final LongAdder compactions = new LongAdder();

    private static final System.Logger LOG = System.getLogger(StorageEngine.class.getName());

    public StorageEngine(Path dataDir) throws IOException {
        this(dataDir, DEFAULT_FLUSH_THRESHOLD_BYTES);
    }

    public StorageEngine(Path dataDir, long flushThresholdBytes) throws IOException {
        this.dataDir = dataDir;
        this.flushThresholdBytes = flushThresholdBytes;
        Files.createDirectories(dataDir);
        loadSsTables();
        Path walFile = dataDir.resolve(WAL_FILE);
        Wal.replay(walFile, new Wal.ReplayHandler() {
            @Override
            public void onPut(String key, String value) {
                memTable.put(key, value);
            }

            @Override
            public void onDelete(String key) {
                memTable.delete(key);
            }
        });
        this.wal = new Wal(walFile);
        maybeFlush(); // the replayed WAL may already exceed the threshold
    }

    /** Finds sstable-<seq>.db files from previous runs, newest (highest seq) first. */
    private void loadSsTables() throws IOException {
        List<Path> dataFiles;
        try (Stream<Path> files = Files.list(dataDir)) {
            dataFiles = files.filter(p -> {
                        String name = p.getFileName().toString();
                        return name.startsWith(SSTABLE_PREFIX) && name.endsWith(SSTABLE_SUFFIX);
                    })
                    .sorted(Comparator.comparingLong(StorageEngine::seqOf).reversed())
                    .toList();
        }
        for (Path dataFile : dataFiles) {
            ssTables.add(SSTable.open(dataFile)); // loads the Bloom sidecar too
            nextSsTableSeq.accumulateAndGet(seqOf(dataFile) + 1, Math::max);
        }
    }

    private static long seqOf(Path ssTableFile) {
        String name = ssTableFile.getFileName().toString();
        return Long.parseLong(name.substring(
                SSTABLE_PREFIX.length(), name.length() - SSTABLE_SUFFIX.length()));
    }

    /** Durable before visible: WAL append (fsync) first, then memory. */
    public void put(String key, String value) throws IOException {
        flushLock.readLock().lock();
        try {
            wal.appendPut(key, value);
            memTable.put(key, value);
        } finally {
            flushLock.readLock().unlock();
        }
        maybeFlush();
    }

    public Optional<String> get(String key) throws IOException {
        for (int attempt = 0; ; attempt++) {
            try {
                return doGet(key);
            } catch (NoSuchFileException raced) {
                // Compaction deleted an SSTable between our list snapshot and
                // the file open. Its data lives on in the compacted table, so
                // retrying against the fresh list is always correct.
                if (attempt >= 3) {
                    throw raced;
                }
            }
        }
    }

    private Optional<String> doGet(String key) throws IOException {
        Optional<Entry> inMemory = memTable.get(key);
        if (inMemory.isPresent()) {
            return inMemory.get().asOptional();
        }
        for (SSTable table : ssTables) { // newest → oldest
            if (!table.mightContain(key)) {
                bloomSkips.increment(); // definitely not in this file: no disk I/O
                continue;
            }
            ssTableScans.increment();
            Optional<Entry> entry = table.get(key);
            if (entry.isPresent()) {
                return entry.get().asOptional();
            }
            ssTableScanMisses.increment(); // bloom false positive: scanned for nothing
        }
        return Optional.empty();
    }

    /** Deletes write a tombstone — it must mask older SSTable versions. */
    public void delete(String key) throws IOException {
        flushLock.readLock().lock();
        try {
            wal.appendDelete(key);
            memTable.delete(key);
        } finally {
            flushLock.readLock().unlock();
        }
        maybeFlush();
    }

    private void maybeFlush() throws IOException {
        if (memTable.approximateBytes() < flushThresholdBytes) {
            return;
        }
        flushLock.writeLock().lock();
        try {
            if (memTable.approximateBytes() < flushThresholdBytes) {
                return; // another writer flushed while we waited
            }
            flushMemTable();
        } finally {
            flushLock.writeLock().unlock();
        }
    }

    /**
     * Crash-safe ordering: (1) SSTable written and fsynced, (2) registered
     * for reads, (3) MemTable cleared, (4) WAL reset. A crash between any of
     * these steps loses nothing — at worst the WAL replays entries that are
     * also in the SSTable, and replay is idempotent.
     */
    private void flushMemTable() throws IOException {
        if (memTable.isEmpty()) {
            return;
        }
        Path file = dataDir.resolve(SSTABLE_PREFIX + nextSsTableSeq.getAndIncrement() + SSTABLE_SUFFIX);
        SSTable table = SSTable.writeFrom(file, memTable.entries());
        ssTables.add(0, table);
        memTable.clear();
        wal.reset();
    }

    /**
     * Full compaction: merge every current SSTable into one, newest version
     * of each key winning, tombstones dropped. The expensive
     * merge runs with NO locks — SSTables are immutable, so reading them
     * while writes and flushes continue is safe. Only the list swap takes
     * the write lock, and it adds the merged table before removing the old
     * ones so a concurrent reader never sees a gap.
     */
    public synchronized void compact() throws IOException {
        List<SSTable> toCompact = List.copyOf(ssTables);
        if (toCompact.size() < 2) {
            return;
        }
        TreeMap<String, Entry> merged = Compactor.mergeNewestWins(toCompact);

        SSTable compacted = null;
        if (!merged.isEmpty()) {
            Path file = dataDir.resolve(SSTABLE_PREFIX + nextSsTableSeq.getAndIncrement() + SSTABLE_SUFFIX);
            compacted = SSTable.writeFrom(file, merged.entrySet());
        }

        flushLock.writeLock().lock();
        try {
            if (compacted != null) {
                // Appended at the END of the newest-first list: anything
                // flushed while we merged is newer than the compacted data.
                ssTables.add(compacted);
            }
            ssTables.removeAll(toCompact);
        } finally {
            flushLock.writeLock().unlock();
        }

        for (SSTable old : toCompact) {
            old.deleteFiles(); // in-flight readers that raced this retry in get()
        }
        compactions.increment();
    }

    /**
     * Starts the background compactor on a virtual thread: every
     * {@code intervalMillis} it compacts if at least {@code minSsTables}
     * tables have accumulated. Stopped by {@link #close()}.
     */
    public synchronized void startBackgroundCompaction(long intervalMillis, int minSsTables) {
        if (compactionThread != null) {
            return;
        }
        compactionThread = Thread.ofVirtual().name("compactor").start(() -> {
            while (!closed) {
                try {
                    Thread.sleep(intervalMillis);
                    if (ssTables.size() >= minSsTables) {
                        compact();
                    }
                } catch (InterruptedException stop) {
                    return;
                } catch (IOException e) {
                    // Never let a failed merge kill the loop; data is intact
                    // (old tables are only deleted after a successful swap).
                    LOG.log(System.Logger.Level.ERROR, "background compaction failed", e);
                }
            }
        });
    }

    // Introspection for tests and the /stats endpoint.

    public int ssTableCount() {
        return ssTables.size();
    }

    public long memTableBytes() {
        return memTable.approximateBytes();
    }

    /**
     * Point-in-time engine stats. {@code bloomHitRate} is the fraction of
     * per-SSTable probes the Bloom filters answered without disk I/O;
     * {@code ssTableScanMisses} counts scans that found nothing — i.e. Bloom
     * false positives (expected ~1%, the configured rate).
     */
    public record StatsSnapshot(long memTableBytes, int ssTableCount, long bloomSkips,
                                long ssTableScans, long ssTableScanMisses, long compactions) {
        public double bloomHitRate() {
            long probes = bloomSkips + ssTableScans;
            return probes == 0 ? 0.0 : (double) bloomSkips / probes;
        }
    }

    public StatsSnapshot stats() {
        return new StatsSnapshot(memTable.approximateBytes(), ssTables.size(),
                bloomSkips.sum(), ssTableScans.sum(), ssTableScanMisses.sum(), compactions.sum());
    }

    @Override
    public synchronized void close() throws IOException {
        closed = true;
        if (compactionThread != null) {
            compactionThread.interrupt();
        }
        wal.close();
    }
}

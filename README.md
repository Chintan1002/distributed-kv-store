# Distributed Key-Value Store (Java 21)

A from-scratch distributed key-value database in the style of Amazon Dynamo and Apache Cassandra. It has two layers:

1. **A single-node storage engine** built as an LSM-tree: a write-ahead log, a sorted MemTable, immutable SSTable files, Bloom filters, and background compaction.
2. **A distribution layer** that shards keys across nodes with a consistent-hashing ring that uses virtual nodes.

The engine is written from first principles. It uses no embedded database library (no RocksDB, LevelDB, or H2). The on-disk formats are hand-designed binary layouts.

---

## Features

- **HTTP key-value API** (PUT / GET / DELETE) served on virtual threads
- **Write-ahead log** with fsync and crash recovery
- **LSM-tree storage:** a sorted MemTable that flushes to immutable SSTable files
- **Bloom filter per SSTable**, with a `/stats` endpoint reporting the hit rate
- **Background compaction** that drops overwritten versions and tombstones
- **Consistent-hashing ring with virtual nodes**, running as a 3-node cluster

**Size:** about 1,240 lines of production Java and 1,070 lines of tests. **51 JUnit 5 tests** across 11 test classes, all passing.

**Roadmap:** replication with tunable quorum consistency, read-repair and Merkle-tree anti-entropy, heartbeat membership, metrics, and a Docker Compose cluster.

---

## Tech stack

- **Java 21.** Virtual threads (Project Loom) handle HTTP requests, inter-node forwarding, and background compaction.
- **Javalin** for HTTP, with a virtual-thread-per-request executor.
- **`java.nio` `FileChannel`** with `force(true)` (a real fsync) for durability.
- **Hand-rolled binary formats** built on `ByteBuffer`. Java `Serializable` is not used anywhere.
- **SHA-256** (`MessageDigest`) for Bloom-filter hashing and ring placement.
- **JUnit 5** and **Gradle**.

---

## Architecture

### Layer 1: storage engine (LSM-tree, one per node)

```
 WRITE:  PUT ─▶ append to WAL (fsync) ─▶ insert into MemTable (ConcurrentSkipListMap)
                                              │ when MemTable exceeds a size threshold
                                              ▼
                               flush to an immutable, sorted SSTable file (+ Bloom filter)
                                              │ when too many SSTables accumulate
                                              ▼
                               background compactor merges them into one

 READ:   GET ─▶ MemTable ─▶ SSTable (newest) ─▶ … ─▶ SSTable (oldest)
                             ▲ each SSTable's Bloom filter is checked first;
                               the file is skipped if the key is definitely not there
```

| Component | What it does |
|---|---|
| `Wal` | Append-only log. Every write is fsynced before the client gets a 200. On startup the log is replayed to rebuild memory. A partially written final record (from a crash mid-write) is detected and ignored. |
| `MemTable` | `ConcurrentSkipListMap`: thread-safe and sorted, so a flush streams keys out in order. Deletes are stored as **tombstones** so they can mask older values on disk. |
| `SSTable` | Immutable sorted file in a custom binary format. It is written once, fsynced, and never edited. |
| `BloomFilter` | Hand-rolled `long[]` bit-array. Sized from the target false-positive rate (1%) using the standard formulas. It uses double hashing, so each key is hashed only once with SHA-256. It is saved to disk as a sidecar file. |
| `Compactor` | Merges all SSTables, keeping the newest version of each key and dropping tombstones together with the values they hide. |
| `StorageEngine` | The facade the network layer calls. It has **no HTTP or network imports**, so it can be unit-tested on its own. |

### Layer 2: distribution

| Component | What it does |
|---|---|
| `ConsistentHashRing` | `TreeMap<Long, VirtualNode>`. A key's owner is found with `ceilingEntry(hash(key))`, wrapping around to `firstEntry()`. Each node gets 128 virtual nodes to spread load evenly. It also computes each node's exact share of the keyspace from the arc lengths on the ring. |
| `Coordinator` | Any node can receive any request. If it owns the key, it serves it from its local engine. Otherwise it forwards the request over HTTP to the owner, using blocking calls on virtual threads. |

### REST API

```
PUT    /kv/{key}     body = value             → 200
GET    /kv/{key}                              → 200 with the value, or 404
DELETE /kv/{key}                              → 200
GET    /stats        MemTable size, SSTable count, Bloom-filter hit rate, compaction count
GET    /ring         ring ownership: each node's share of the keyspace
```

---

## Engineering highlights

These are the design decisions that go beyond a basic implementation.

**1. Crash safety at every step.**
- Writes follow the rule "log first, apply second." The WAL is fsynced before the in-memory update and before the HTTP 200.
- The flush order is: write the SSTable and fsync it, then register it for reads, then clear the MemTable, then reset the WAL. A crash between any two steps loses nothing, because WAL replay is idempotent.
- WAL replay tolerates a torn final record, which is what a crash in the middle of an append leaves behind.
- Verified at the process level: writes, then `kill -9` on the JVM, then a fresh process reads every acknowledged write back.

**2. Compaction that never blocks traffic.**
- The expensive merge runs with **no locks**. This is safe because SSTables are immutable.
- Only the swap of the SSTable list takes a write lock, and it is brief. The merged table is added *before* the old ones are removed, so a reader never sees a gap.
- A read that races with old-file deletion catches `NoSuchFileException` and retries against the updated list.
- Compaction runs on a **scheduled virtual thread** and survives I/O errors without crashing.

**3. Tombstone correctness.**
- Deleted keys are added to the Bloom filter too. If the filter hid a tombstone, a GET would skip that file and bring back an older, deleted value.
- Tombstones are only dropped during a *full* compaction, which includes the oldest table. Dropping them during a partial merge could also bring deleted data back.

**4. Concurrency model.** Reads, writes, and deletes run concurrently using `ConcurrentSkipListMap`, `CopyOnWriteArrayList`, and `LongAdder` counters. A `ReentrantReadWriteLock` is used only around the multi-step flush.

---

## Measured results

These numbers come from live demos on running JVMs, not only from unit tests.

| What was measured | Result |
|---|---|
| Bloom filter: 200 GETs for missing keys across 3 SSTables | **98.3% of SSTable probes skipped without any disk read** (590 skipped, 10 false-positive scans, close to the configured 1% rate) |
| Compaction: one key overwritten 40 times | **41 SSTables merged into 1; disk use dropped from 336 KB to 16 KB (about 95%)**. The newest value survived and the deleted key stayed deleted. |
| Crash recovery: `kill -9` then restart | All acknowledged writes and deletes recovered from the WAL and SSTables |
| Ring balance: 3 nodes with 128 virtual nodes each | Keyspace shares of 32.9% / 31.3% / 35.8% |
| Adding a 4th node (10,000 sample keys) | About 25% of keys moved (the expected 1/N), and **every moved key went to the new node**. No keys shuffled between existing nodes. |
| 3-node cluster run as separate JVMs | 90 writes sent to one node were spread across all three. Every key could be read through any node. |

---

## Testing

There are 51 JUnit 5 tests, one test class per component:

- **WAL:** replay order, a missing file, a torn final record (simulated mid-write crash), and Unicode and empty values.
- **Restart durability:** the canonical test writes, closes the engine, reopens it, and reads the value back. Overwrites and deletes are also checked across restarts.
- **SSTable / MemTable:** sort order, tombstones, and early-exit lookups.
- **Bloom filter:** no false negatives ever, a false-positive rate near the target, and a lossless save-and-reload. At the engine level, the tests check that GETs for missing keys avoid disk scans and that the engine still works if a filter file is missing.
- **Compaction:** disk shrinks more than 5x, exactly one version per key remains, zero tombstones remain on disk, and deletes survive compaction plus a restart. The background compactor is also observed running on its own.
- **Ring:** ownership is deterministic, shares are even, adding a node moves about 1/N of keys and only to the new node, and removing a node reassigns only its keys.
- **Cluster:** a 3-node cluster runs in-process. The tests check that a key physically exists only on its owner, and cover reads and deletes through any node.

```bash
./gradlew test
```

---

## Running it

```bash
./gradlew run --args="--port 8080 --data-dir ./data"
```

Example 3-node cluster, one terminal per node:

```bash
./gradlew run --args="--port 9091 --node-id n1 --peers n1=localhost:9091,n2=localhost:9092,n3=localhost:9093 --data-dir ./data/n1"
```

Repeat with `n2` on port 9092 and `n3` on port 9093.

Optional flags: `--flush-threshold-bytes` (default 4 MiB), `--compaction-interval-ms` (default 30000), `--compaction-min-sstables` (default 4).

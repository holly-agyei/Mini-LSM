# Mini LSM

A minimal but real log-structured merge-tree key-value store in C++20 — the design behind
RocksDB, LevelDB and Cassandra, boiled down to something you can read in an hour and still
benchmark honestly against a production database.

Writes are buffered in an in-memory **memtable**, made durable up front by a **write-ahead
log**, flushed to sorted files (**SSTables**) once they fill up, and periodically merged by
**size-tiered compaction** to keep reads fast. Everything is standard-library only.

> Brand: *proven, not promised* — every performance claim in this README is a measured number,
> reproduced by `make bench` on a laptop.

## Design

| Component      | What it does                                                                 |
|----------------|------------------------------------------------------------------------------|
| WAL            | Append-only log; every write is fsync-flushed before the memtable is touched. CRC32 per record; a torn tail is skipped on recovery. |
| Memtable       | Sorted `std::map`; tombstones for deletes. Flushed to an SSTable at a size threshold. |
| SSTable        | Sorted entries + a sparse index (one key every N) for binary-search lookups.  |
| Compaction     | k-way merge of a full level into the next; newest write wins; tombstones dropped at the deepest level. |
| Read path      | Memtable, then SSTables newest-to-oldest with key-range pruning; first hit wins. |
| Recovery       | MANIFEST restores the level layout; the WAL replays the un-flushed tail.       |

## Build & run

```sh
make test           # correctness suite (CRUD, scan, flush, compaction, WAL recovery, 1M scale)
make bench          # performance numbers + the paste-ready results block
make bench-rocksdb  # same numbers plus a head-to-head vs RocksDB (needs `brew install rocksdb`)
make count          # source line count against the 1,500-line budget
```

Compiler: `g++ -std=c++20 -O2 -pthread` (Apple clang works too).

## Configuration

`mlsm::Options` — `memtable_size_bytes` (4 MB), `level_max_tables` (4), `index_interval` (16),
`data_dir`.

## Results

Measured on an Apple laptop (macOS, clang `-O2`), single-threaded, `make bench-rocksdb`.
Workload: 1,000,000 keys, 100-byte values, 256 KB memtable, `level_max_tables = 4`.

| Metric                     | Mini LSM            | Notes                                    |
|----------------------------|---------------------|------------------------------------------|
| Sequential write           | 75 K ops/s (7.8 MB/s) | 1M keys in sorted order                 |
| Random write               | 75 K ops/s (7.9 MB/s) | 1M keys shuffled                        |
| Point read                 | 19 K ops/s (53 µs)  | 500K random existing keys                |
| Range scan                 | 1.4 M rows/s        | full range in 1K-key windows             |
| Read amplification         | 7.6×                | SSTables read per point lookup, 3 levels |
| Write amplification        | 5.3×                | bytes written to disk ÷ user bytes       |
| Disk footprint             | 114 MB, 8 SSTables  | 3 levels                                 |
| Source lines               | 816 code (1,053 raw)| across 4 files                           |

### Honest comparison vs RocksDB (defaults, same workload)

| Operation | Mini LSM   | RocksDB    | Ratio         |
|-----------|------------|------------|---------------|
| Write     | 75 K ops/s | 167 K ops/s| 2.2× slower   |
| Read      | 19 K ops/s | 270 K ops/s| 14.2× slower  |

### Reading the numbers

- **Writes land within ~2× of RocksDB.** Buffered WAL + memtable + batched flushes keep the write
  path cheap; the 5.3× write amplification is the compaction cost of keeping reads sorted.
- **Reads are amplification-bound.** With no bloom filter, a point lookup probes every SSTable whose
  key range covers the key — 7.6 files on average here. That is the single biggest gap to RocksDB,
  whose per-table bloom filters skip almost all of those reads. A bloom filter (one bit-array per
  SSTable, checked before the block read) is the standard fix and the obvious next step.
- **Scans are fast** because the sparse index seeks straight to the overlapping blocks rather than
  scanning whole files.

Smaller memtables create more levels and therefore *more* read amplification and slower reads; larger
memtables do the reverse. The 256 KB setting above is chosen to exercise multiple levels; a 2 MB
memtable roughly doubles read throughput at 2 levels. Every number here is reproduced by `make bench`.

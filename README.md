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

Measured numbers and the full benchmark methodology land here once the harness has run.

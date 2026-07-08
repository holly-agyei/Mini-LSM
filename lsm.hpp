// Mini LSM — a small log-structured merge-tree key-value store.
//
// Layout of the engine:
//   put/del  -> append to the write-ahead log, then update the in-memory memtable
//   memtable -> once it grows past a threshold it is flushed to a sorted file (SSTable)
//   SSTables -> live in levels; a full level is merged down into the next (compaction)
//   get/scan -> newest source wins: memtable first, then SSTables newest-to-oldest
//
// The whole engine is std-library only. See lsm.cpp for the implementation.
#pragma once

#include <cstdint>
#include <fstream>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace mlsm {

// IEEE CRC32 over a byte buffer. Each WAL record carries one so we can spot a torn write.
uint32_t crc32(const char* data, size_t len);

// Tunable knobs. Defaults match the project brief.
struct Options {
    size_t      memtable_size_bytes = 4u << 20;  // flush once the memtable holds this many key+value bytes
    int         level_max_tables    = 4;         // a level is compacted once it exceeds this many tables
    int         index_interval      = 16;        // sparse index keeps one key every N entries
    std::string data_dir            = "mlsm_data";
};

// A stored value, or a tombstone. An empty optional means the key was deleted.
using Slot = std::optional<std::string>;

// One sparse-index record: the first key of an on-disk block and that block's byte offset.
struct IndexEntry {
    std::string key;
    uint64_t    offset;
};

// A single immutable sorted run on disk. The entries stay on disk; only the sparse
// index and key range are held in memory.
struct SSTable {
    uint64_t                seq = 0;   // creation id; also the number in the filename and the recency rank
    std::string             path;      // full path to the .sst file
    std::vector<IndexEntry> index;     // sparse index, loaded when the table is opened
    std::string             min_key;   // smallest key in the file (for scan/point-read pruning)
    std::string             max_key;   // largest key in the file
    uint64_t                index_offset = 0;  // byte offset where the index block begins
    uint64_t                file_size    = 0;  // size on disk, for footprint accounting
    mutable std::ifstream   in_;               // read handle kept open so lookups avoid re-opening the file

    // Load the sparse index and key range from an existing file (also opens the read handle).
    void open();
    // Point lookup. Returns true if the key exists in this table, filling `out` (tombstone => empty Slot).
    bool get(const std::string& key, Slot& out) const;
    // Visit every entry in key order. Used by scan and by compaction merges.
    void for_each(const std::function<void(const std::string&, const Slot&)>& fn) const;
};

class LSM {
public:
    explicit LSM(Options opts);
    ~LSM();

    // ---- data operations ----
    void put(const std::string& key, const std::string& value);
    void del(const std::string& key);
    std::optional<std::string> get(const std::string& key);
    // Half-open range [start, end): sorted, tombstones skipped, newest value per key.
    std::vector<std::pair<std::string, std::string>> scan(const std::string& start, const std::string& end);

    void flush();  // force the active memtable out to a new SSTable
    void close();  // flush and release file handles


    // ---- benchmark instrumentation ----
    uint64_t sstables_consulted_total() const { return consulted_; }
    uint64_t point_reads_total() const { return point_reads_; }
    uint64_t bytes_written_to_disk() const { return bytes_written_; }  // WAL + SSTables + compaction output
    uint64_t disk_footprint_bytes() const;                            // live bytes currently on disk
    size_t   sstable_count() const;
    int      level_count() const;
    int      tables_at_level(int level) const {                       // table count at one level
        return level < static_cast<int>(levels_.size()) ? static_cast<int>(levels_[level].size()) : 0;
    }

private:
    // Persist a sorted, de-duplicated run of entries to a new SSTable file at `level`.
    std::shared_ptr<SSTable> write_sstable(const std::vector<std::pair<std::string, Slot>>& entries, int level);
    // Merge every table at `level` into one new table at `level+1`, then cascade downward.
    void compact(int level);
    // Rewrite the MANIFEST so the current level layout survives a restart.
    void save_manifest();
    // Rebuild levels from the MANIFEST and replay the WAL into a fresh memtable.
    void recover();
    // Flush the active memtable to a new SSTable. Assumes the caller already holds mu_.
    void flush_locked();
    // Append one record to the WAL and make it durable.
    void wal_append(const std::string& key, const Slot& value);

    Options opts_;
    std::mutex mu_;

    std::map<std::string, Slot> mem_;        // active memtable
    size_t mem_bytes_ = 0;                    // approximate key+value bytes buffered in mem_

    std::vector<std::vector<std::shared_ptr<SSTable>>> levels_;  // levels_[L] = tables at level L, newest last
    uint64_t next_seq_ = 1;                   // next SSTable id to hand out

    std::string   wal_path_;
    std::ofstream wal_;  // append-only write-ahead log stream

    // instrumentation counters
    uint64_t consulted_     = 0;  // SSTables read during point lookups
    uint64_t point_reads_   = 0;  // number of get() calls that reached the SSTables
    uint64_t bytes_written_ = 0;  // cumulative bytes written to disk over the store's life
};

}  // namespace mlsm

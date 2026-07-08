// Mini LSM engine implementation. See lsm.hpp for the overview.
#include "lsm.hpp"

#include <algorithm>
#include <array>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <queue>
#include <system_error>

namespace fs = std::filesystem;

namespace mlsm {

// ------------------------------------------------------------------ helpers

namespace {

constexpr uint32_t kTombstone = 0xFFFFFFFFu;  // value-length sentinel meaning "deleted"

// Append a little-endian integer to a byte buffer.
void put_u32(std::string& buf, uint32_t v) { buf.append(reinterpret_cast<const char*>(&v), 4); }
void put_u64(std::string& buf, uint64_t v) { buf.append(reinterpret_cast<const char*>(&v), 8); }

// Read a fixed-width little-endian integer out of a buffer at `pos`, advancing it.
uint32_t get_u32(const char* p) { uint32_t v; std::memcpy(&v, p, 4); return v; }
uint64_t get_u64(const char* p) { uint64_t v; std::memcpy(&v, p, 8); return v; }

}  // namespace

// IEEE CRC32, table built once on first use.
uint32_t crc32(const char* data, size_t len) {
    static const std::array<uint32_t, 256> table = [] {
        std::array<uint32_t, 256> t{};
        for (uint32_t i = 0; i < 256; ++i) {
            uint32_t c = i;
            for (int k = 0; k < 8; ++k) c = (c & 1) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
            t[i] = c;
        }
        return t;
    }();
    uint32_t crc = 0xFFFFFFFFu;
    for (size_t i = 0; i < len; ++i) crc = table[(crc ^ static_cast<uint8_t>(data[i])) & 0xFF] ^ (crc >> 8);
    return crc ^ 0xFFFFFFFFu;
}

// ------------------------------------------------------------------ lifecycle

LSM::LSM(Options opts) : opts_(std::move(opts)) {
    fs::create_directories(opts_.data_dir);
    wal_path_ = opts_.data_dir + "/wal.log";
    levels_.emplace_back();  // always have a level 0
    recover();               // rebuild SSTable levels and replay the WAL tail
    // Reopen the WAL for appending after recovery has read it.
    wal_.open(wal_path_, std::ios::binary | std::ios::app);
}

LSM::~LSM() {
    // Close file handles only. We deliberately do NOT flush the memtable here so an
    // abandoned store (a simulated crash) leaves its writes to be recovered from the WAL.
    if (wal_.is_open()) wal_.close();
}

void LSM::close() {
    std::lock_guard<std::mutex> lock(mu_);
    if (!mem_.empty()) flush_locked();
    if (wal_.is_open()) wal_.close();
}

// ------------------------------------------------------------------ writes

// Append one record to the WAL and flush it to the OS so it survives a crash.
void LSM::wal_append(const std::string& key, const Slot& value) {
    std::string payload;
    put_u32(payload, static_cast<uint32_t>(key.size()));
    payload += key;
    if (value) {
        put_u32(payload, static_cast<uint32_t>(value->size()));
        payload += *value;
    } else {
        put_u32(payload, kTombstone);
    }
    std::string record;
    put_u32(record, crc32(payload.data(), payload.size()));  // checksum precedes the payload
    record += payload;
    wal_.write(record.data(), static_cast<std::streamsize>(record.size()));
    wal_.flush();
    bytes_written_ += record.size();
}

void LSM::put(const std::string& key, const std::string& value) {
    std::lock_guard<std::mutex> lock(mu_);
    wal_append(key, value);
    mem_bytes_ += key.size() + value.size();
    mem_[key] = value;
    if (mem_bytes_ >= opts_.memtable_size_bytes) flush_locked();
}

void LSM::del(const std::string& key) {
    std::lock_guard<std::mutex> lock(mu_);
    wal_append(key, std::nullopt);
    mem_bytes_ += key.size();
    mem_[key] = std::nullopt;  // tombstone
    if (mem_bytes_ >= opts_.memtable_size_bytes) flush_locked();
}

// ------------------------------------------------------------------ flush

void LSM::flush() {
    std::lock_guard<std::mutex> lock(mu_);
    flush_locked();
}

void LSM::flush_locked() {
    if (mem_.empty()) return;
    // Move the full memtable aside and write it out. New writes will land in the fresh mem_.
    std::map<std::string, Slot> frozen;
    frozen.swap(mem_);
    mem_bytes_ = 0;

    auto table = write_sstable(frozen, 0);
    levels_[0].push_back(table);

    // The data is now durable in the SSTable, so reset the WAL to reclaim its space.
    wal_.close();
    std::ofstream(wal_path_, std::ios::binary | std::ios::trunc).close();
    wal_.open(wal_path_, std::ios::binary | std::ios::app);

    save_manifest();

    // Compact any level that is now over budget (cascades downward inside compact()).
    for (int L = 0; L < static_cast<int>(levels_.size()); ++L) {
        if (static_cast<int>(levels_[L].size()) > opts_.level_max_tables) { compact(L); break; }
    }
}

// ------------------------------------------------------------------ SSTable I/O

// Encode one entry: [klen:4][key][vlen:4][value]; a tombstone stores vlen == kTombstone and no value.
static void encode_entry(std::string& buf, const std::string& key, const Slot& value) {
    put_u32(buf, static_cast<uint32_t>(key.size()));
    buf += key;
    if (value) { put_u32(buf, static_cast<uint32_t>(value->size())); buf += *value; }
    else       { put_u32(buf, kTombstone); }
}

// Write a sorted table of entries to a new SSTable file and return its in-memory metadata.
// Returns nullptr if `table` is empty (e.g. a compaction that dropped everything).
std::shared_ptr<SSTable> LSM::write_sstable(const std::map<std::string, Slot>& table, int /*level*/) {
    if (table.empty()) return nullptr;
    auto sst = std::make_shared<SSTable>();
    sst->seq = next_seq_++;
    char name[32];
    std::snprintf(name, sizeof(name), "sst_%06llu.sst", static_cast<unsigned long long>(sst->seq));
    sst->path = opts_.data_dir + "/" + name;

    std::ofstream out(sst->path, std::ios::binary | std::ios::trunc);
    std::vector<IndexEntry> index;
    uint64_t offset = 0, last_offset = 0;
    std::string last_key;
    int i = 0;
    for (const auto& [k, v] : table) {
        if (i % opts_.index_interval == 0) index.push_back({k, offset});  // sample the block's first key
        std::string rec;
        encode_entry(rec, k, v);
        out.write(rec.data(), static_cast<std::streamsize>(rec.size()));
        last_key = k;
        last_offset = offset;
        offset += rec.size();
        ++i;
    }
    // Always index the final key so max_key is exact and the last block has a tight bound.
    if (index.empty() || index.back().key != last_key) index.push_back({last_key, last_offset});

    uint64_t index_offset = offset;
    std::string idx;
    for (const auto& e : index) { put_u32(idx, static_cast<uint32_t>(e.key.size())); idx += e.key; put_u64(idx, e.offset); }
    out.write(idx.data(), static_cast<std::streamsize>(idx.size()));
    std::string footer;
    put_u64(footer, index_offset);  // 8-byte trailer points back at the index block
    out.write(footer.data(), 8);
    out.close();

    sst->index        = std::move(index);
    sst->index_offset = index_offset;
    sst->min_key      = sst->index.front().key;
    sst->max_key      = sst->index.back().key;
    sst->file_size    = fs::file_size(sst->path);
    bytes_written_ += sst->file_size;
    return sst;
}

// Load the sparse index and key range from an existing SSTable file.
void SSTable::open() {
    std::ifstream in(path, std::ios::binary);
    in.seekg(0, std::ios::end);
    file_size = static_cast<uint64_t>(in.tellg());
    char foot[8];
    in.seekg(static_cast<std::streamoff>(file_size - 8));
    in.read(foot, 8);
    index_offset = get_u64(foot);

    uint64_t idx_bytes = file_size - 8 - index_offset;
    std::string blob(idx_bytes, '\0');
    in.seekg(static_cast<std::streamoff>(index_offset));
    in.read(blob.data(), static_cast<std::streamsize>(idx_bytes));
    index.clear();
    size_t pos = 0;
    while (pos < idx_bytes) {
        uint32_t klen = get_u32(&blob[pos]); pos += 4;
        std::string key(blob, pos, klen);   pos += klen;
        uint64_t off = get_u64(&blob[pos]);  pos += 8;
        index.push_back({std::move(key), off});
    }
    min_key = index.front().key;
    max_key = index.back().key;
}

// Point lookup. Binary-search the sparse index for the containing block, then scan it.
bool SSTable::get(const std::string& key, Slot& out) const {
    if (key < min_key || key > max_key) return false;
    // Find the first index entry whose key is greater than the target; the block before it holds `key`.
    auto it = std::upper_bound(index.begin(), index.end(), key,
                               [](const std::string& k, const IndexEntry& e) { return k < e.key; });
    size_t bi = (it == index.begin()) ? 0 : static_cast<size_t>(std::distance(index.begin(), it) - 1);
    uint64_t start = index[bi].offset;
    uint64_t stop  = (bi + 1 < index.size()) ? index[bi + 1].offset : index_offset;

    std::ifstream in(path, std::ios::binary);
    in.seekg(static_cast<std::streamoff>(start));
    std::string block(stop - start, '\0');
    in.read(block.data(), static_cast<std::streamsize>(stop - start));
    size_t pos = 0;
    while (pos < block.size()) {
        uint32_t klen = get_u32(&block[pos]); pos += 4;
        std::string k(block, pos, klen);      pos += klen;
        uint32_t vlen = get_u32(&block[pos]); pos += 4;
        if (vlen == kTombstone) {
            if (k == key) { out = std::nullopt; return true; }
        } else {
            std::string v(block, pos, vlen); pos += vlen;
            if (k == key) { out = std::move(v); return true; }
        }
        if (k > key) return false;  // entries are sorted, so we have passed where the key would be
    }
    return false;
}

// Visit every entry in key order (used by scan and compaction).
void SSTable::for_each(const std::function<void(const std::string&, const Slot&)>& fn) const {
    std::ifstream in(path, std::ios::binary);
    std::string data(index_offset, '\0');
    in.read(data.data(), static_cast<std::streamsize>(index_offset));
    size_t pos = 0;
    while (pos < index_offset) {
        uint32_t klen = get_u32(&data[pos]); pos += 4;
        std::string k(data, pos, klen);      pos += klen;
        uint32_t vlen = get_u32(&data[pos]); pos += 4;
        if (vlen == kTombstone) {
            fn(k, std::nullopt);
        } else {
            std::string v(data, pos, vlen); pos += vlen;
            fn(k, v);
        }
    }
}

// ------------------------------------------------------------------ compaction

// Merge every table at `level` into one new run at `level+1`, then cascade if that level fills up.
void LSM::compact(int level) {
    if (levels_[level].empty()) return;
    int target = level + 1;
    if (target >= static_cast<int>(levels_.size())) levels_.resize(target + 1);

    // A tombstone can only be discarded once no older data survives at or below the target level;
    // otherwise dropping it would let a shadowed value reappear.
    bool older_below = false;
    for (int L = target; L < static_cast<int>(levels_.size()); ++L)
        if (!levels_[L].empty()) older_below = true;
    const bool drop_tombstones = !older_below;

    // Collect the source tables oldest-first so that newer entries overwrite older ones on merge.
    std::vector<std::shared_ptr<SSTable>> sources = levels_[level];
    std::sort(sources.begin(), sources.end(),
              [](const auto& a, const auto& b) { return a->seq < b->seq; });

    std::map<std::string, Slot> merged;
    for (const auto& t : sources)
        t->for_each([&](const std::string& k, const Slot& v) { merged[k] = v; });
    if (drop_tombstones)
        for (auto it = merged.begin(); it != merged.end();)
            it = it->second ? std::next(it) : merged.erase(it);

    auto out = write_sstable(merged, target);  // nullptr if the merge dropped everything

    // Swap the layout in memory first, then persist, then delete the old files — in that order the
    // MANIFEST never points at a file we have already removed.
    std::vector<std::string> old_paths;
    for (const auto& t : levels_[level]) old_paths.push_back(t->path);
    levels_[level].clear();
    if (out) levels_[target].push_back(out);
    save_manifest();
    for (const auto& p : old_paths) fs::remove(p);

    if (static_cast<int>(levels_[target].size()) > opts_.level_max_tables) compact(target);
}

// ------------------------------------------------------------------ manifest & recovery

// Persist the level layout so a restart can rebuild it. Written atomically via a temp file + rename.
void LSM::save_manifest() {
    std::string tmp = opts_.data_dir + "/MANIFEST.tmp";
    {
        std::ofstream out(tmp, std::ios::trunc);
        out << "next_seq " << next_seq_ << "\n";
        for (int L = 0; L < static_cast<int>(levels_.size()); ++L)
            for (const auto& t : levels_[L]) out << "table " << t->seq << " " << L << "\n";
    }
    fs::rename(tmp, opts_.data_dir + "/MANIFEST");
}

// Rebuild the levels from the MANIFEST, then replay the WAL tail into a fresh memtable.
void LSM::recover() {
    std::string manifest = opts_.data_dir + "/MANIFEST";
    if (fs::exists(manifest)) {
        std::ifstream in(manifest);
        std::string tok;
        while (in >> tok) {
            if (tok == "next_seq") {
                in >> next_seq_;
            } else if (tok == "table") {
                uint64_t seq; int level;
                in >> seq >> level;
                auto sst = std::make_shared<SSTable>();
                sst->seq = seq;
                char name[32];
                std::snprintf(name, sizeof(name), "sst_%06llu.sst", static_cast<unsigned long long>(seq));
                sst->path = opts_.data_dir + "/" + name;
                sst->open();
                if (level >= static_cast<int>(levels_.size())) levels_.resize(level + 1);
                levels_[level].push_back(sst);
            }
        }
        // Keep each level ordered oldest-to-newest so reads can scan newest-first from the back.
        for (auto& lvl : levels_)
            std::sort(lvl.begin(), lvl.end(), [](const auto& a, const auto& b) { return a->seq < b->seq; });
    }

    // Replay the WAL, stopping at the first record that is short or fails its checksum (a torn write).
    std::ifstream wal(wal_path_, std::ios::binary);
    if (!wal) return;
    std::string buf((std::istreambuf_iterator<char>(wal)), std::istreambuf_iterator<char>());
    size_t pos = 0;
    while (pos + 8 <= buf.size()) {
        uint32_t crc  = get_u32(&buf[pos]);
        uint32_t klen = get_u32(&buf[pos + 4]);
        if (pos + 8 + klen + 4 > buf.size()) break;  // truncated key or missing value length
        uint32_t vlen = get_u32(&buf[pos + 8 + klen]);
        size_t payload_len = 4 + klen + 4;
        if (vlen != kTombstone) {
            if (pos + 8 + klen + 4 + vlen > buf.size()) break;  // truncated value
            payload_len += vlen;
        }
        if (crc32(&buf[pos + 4], payload_len) != crc) break;  // corrupt record — stop here

        std::string key(buf, pos + 8, klen);
        if (vlen == kTombstone) { mem_[key] = std::nullopt; mem_bytes_ += key.size(); }
        else { std::string val(buf, pos + 12 + klen, vlen); mem_bytes_ += key.size() + val.size(); mem_[key] = std::move(val); }
        pos += 4 + payload_len;
    }
}

}  // namespace mlsm

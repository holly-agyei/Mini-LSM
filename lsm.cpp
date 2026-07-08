// Mini LSM engine implementation. See lsm.hpp for the overview.
#include "lsm.hpp"

#include <algorithm>
#include <array>
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

}  // namespace mlsm

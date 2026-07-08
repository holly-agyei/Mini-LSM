// Performance harness for the Mini LSM engine. Every number printed here is measured on the spot;
// nothing is hard-coded. Run with `make bench` (or `make bench-rocksdb` for the RocksDB comparison).
#include "lsm.hpp"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <random>
#include <string>
#include <vector>

#ifdef HAVE_ROCKSDB
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#endif

namespace fs = std::filesystem;
using namespace mlsm;
using std::string;
using Clock = std::chrono::steady_clock;

static double secs(Clock::time_point a, Clock::time_point b) {
    return std::chrono::duration<double>(b - a).count();
}

// Fixed workload shared by every phase: 1M keys, 100-byte values.
static const int    N          = 1'000'000;
static const int    READS      = 500'000;
static const size_t VALUE_SIZE = 100;

// Zero-padded so lexicographic order matches numeric order.
static string key_for(int i) { char b[16]; std::snprintf(b, sizeof(b), "k%09d", i); return b; }

// Raw and code (non-blank, non-comment) line counts for one source file.
static std::pair<int, int> count_lines(const string& path) {
    std::ifstream in(path);
    string line;
    int raw = 0, code = 0;
    while (std::getline(in, line)) {
        ++raw;
        size_t s = line.find_first_not_of(" \t");
        if (s == string::npos) continue;                       // blank
        if (line.compare(s, 2, "//") == 0) continue;           // comment-only line
        ++code;
    }
    return {raw, code};
}

int main() {
    // Pre-build the keys and one shared value up front so the timed loops only measure the engine.
    std::vector<string> keys(N);
    for (int i = 0; i < N; ++i) keys[i] = key_for(i);
    const string value(VALUE_SIZE, 'v');
    const double user_mb = double(N) * (keys[0].size() + VALUE_SIZE) / (1024.0 * 1024.0);

    std::vector<int> shuffled(N);
    for (int i = 0; i < N; ++i) shuffled[i] = i;
    std::mt19937 rng(42);
    std::shuffle(shuffled.begin(), shuffled.end(), rng);

    Options base;
    base.memtable_size_bytes = 256u << 10;  // 256 KB — 1M keys then build ~4 size-tiered levels
    base.level_max_tables    = 4;
    base.index_interval      = 16;

    // ---- 1. sequential write throughput ----
    double seq_ops = 0;
    {
        Options o = base; o.data_dir = "bench_data_seq";
        fs::remove_all(o.data_dir);
        LSM db(o);
        auto t0 = Clock::now();
        for (int i = 0; i < N; ++i) db.put(keys[i], value);
        db.flush();
        double t = secs(t0, Clock::now());
        seq_ops = N / t;
        std::printf("1. sequential write : %.0f K ops/s  (%.1f MB/s)  %.2fs\n", seq_ops / 1e3, user_mb / t, t);
        fs::remove_all(o.data_dir);
    }

    // ---- 2. random write throughput (this store is kept for the read phases) ----
    Options o = base; o.data_dir = "bench_data";
    fs::remove_all(o.data_dir);
    LSM db(o);
    double rand_ops = 0;
    {
        auto t0 = Clock::now();
        for (int i = 0; i < N; ++i) db.put(keys[shuffled[i]], value);
        db.flush();
        double t = secs(t0, Clock::now());
        rand_ops = N / t;
        std::printf("2. random write     : %.0f K ops/s  (%.1f MB/s)  %.2fs\n", rand_ops / 1e3, user_mb / t, t);
    }

    // ---- 3. point read throughput + read amplification ----
    double read_ops = 0, read_amp = 0;
    {
        uint64_t consulted0 = db.sstables_consulted_total();
        uint64_t reads0     = db.point_reads_total();
        auto t0 = Clock::now();
        size_t hits = 0;
        for (int i = 0; i < READS; ++i) if (db.get(keys[shuffled[i]])) ++hits;
        double t = secs(t0, Clock::now());
        read_ops = READS / t;
        uint64_t consulted = db.sstables_consulted_total() - consulted0;
        uint64_t reads     = db.point_reads_total() - reads0;
        read_amp = reads ? double(consulted) / double(reads) : 0;
        std::printf("3. point read       : %.0f K ops/s  (avg %.2f us)  %zu/%d hit\n",
                    read_ops / 1e3, t * 1e6 / READS, hits, READS);
    }

    // ---- 4. scan throughput (full range in 1K-key windows) ----
    double scan_rows_per_s = 0;
    {
        auto t0 = Clock::now();
        uint64_t rows = 0;
        for (int w = 0; w < N; w += 1000) rows += db.scan(key_for(w), key_for(w + 1000)).size();
        double t = secs(t0, Clock::now());
        scan_rows_per_s = rows / t;
        std::printf("4. scan             : %.0f K rows/s  (%.2fs, %llu rows)\n",
                    scan_rows_per_s / 1e3, t, (unsigned long long)rows);
    }

    // ---- 5-7. amplification, footprint, levels ----
    double write_amp = double(db.bytes_written_to_disk()) / (double(N) * (keys[0].size() + VALUE_SIZE));
    double disk_mb   = db.disk_footprint_bytes() / (1024.0 * 1024.0);
    int    levels    = db.level_count();
    std::printf("5. read  amplification: %.2fx across %d levels\n", read_amp, levels);
    std::printf("6. write amplification: %.2fx\n", write_amp);
    std::printf("7. disk footprint     : %.1f MB across %zu SSTables, %d levels\n",
                disk_mb, db.sstable_count(), levels);

    // ---- 8. source line count ----
    int raw_total = 0, code_total = 0;
    for (const string& f : {"lsm.hpp", "lsm.cpp", "test.cpp", "bench.cpp"}) {
        auto [raw, code] = count_lines(f);
        raw_total += raw; code_total += code;
    }
    std::printf("8. source lines       : %d code (%d raw) across 4 files\n", code_total, raw_total);

    // ---- 9. RocksDB comparison (only compiled with -DHAVE_ROCKSDB) ----
    string rocks_line = "RocksDB comparison: skipped (not available)";
#ifdef HAVE_ROCKSDB
    {
        fs::remove_all("bench_data_rocks");
        rocksdb::DB* rdb = nullptr;
        rocksdb::Options ro; ro.create_if_missing = true;
        rocksdb::DB::Open(ro, "bench_data_rocks", &rdb);
        rocksdb::WriteOptions wo;
        auto t0 = Clock::now();
        for (int i = 0; i < N; ++i) rdb->Put(wo, keys[i], value);
        double wt = secs(t0, Clock::now());
        double rocks_write = N / wt;
        rocksdb::ReadOptions rdo;
        auto t1 = Clock::now();
        string got;
        for (int i = 0; i < READS; ++i) rdb->Get(rdo, keys[shuffled[i]], &got);
        double rt = secs(t1, Clock::now());
        double rocks_read = READS / rt;
        delete rdb;
        fs::remove_all("bench_data_rocks");
        char buf[160];
        std::snprintf(buf, sizeof(buf),
                      "RocksDB comparison: write %.1fx slower, read %.1fx slower vs RocksDB defaults",
                      rocks_write / seq_ops, rocks_read / read_ops);
        rocks_line = buf;
        std::printf("9. RocksDB           : rocksdb write %.0f K ops/s, read %.0f K ops/s\n",
                    rocks_write / 1e3, rocks_read / 1e3);
    }
#endif

    // ---- paste-ready summary ----
    std::printf("\n=== RESULTS (paste into resume) ===\n");
    std::printf("Sequential write throughput:   %.0f K ops/sec (%.1f MB/sec)\n", seq_ops / 1e3, user_mb / (N / seq_ops));
    std::printf("Random write throughput:       %.0f K ops/sec (%.1f MB/sec)\n", rand_ops / 1e3, user_mb / (N / rand_ops));
    std::printf("Point read throughput:         %.0f K ops/sec (avg %.2f us/read)\n", read_ops / 1e3, 1e6 / read_ops);
    std::printf("Scan throughput:               %.0f K rows/sec\n", scan_rows_per_s / 1e3);
    std::printf("Read amplification:            %.2fx (across %d levels)\n", read_amp, levels);
    std::printf("Write amplification:           %.2fx\n", write_amp);
    std::printf("Disk footprint:                %.1f MB across %zu SSTable files\n", disk_mb, db.sstable_count());
    std::printf("Total source lines:            %d code (%d raw)\n", code_total, raw_total);
    std::printf("%s\n", rocks_line.c_str());
    std::printf("===================================\n");

    fs::remove_all("bench_data");
    return 0;
}

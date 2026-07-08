// Correctness suite for the Mini LSM engine. Each group prints its own PASS/FAIL lines and a
// running tally is printed at the end. Every group runs against a fresh data directory.
#include "lsm.hpp"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <numeric>
#include <optional>
#include <random>
#include <string>
#include <vector>

namespace fs = std::filesystem;
using namespace mlsm;
using std::optional;
using std::string;

static int g_total = 0, g_passed = 0;

// Record one assertion. Prints only failures so a clean run stays readable.
static void check(bool ok, const string& name) {
    ++g_total;
    if (ok) { ++g_passed; }
    else    { std::printf("  FAIL: %s\n", name.c_str()); }
}

// A fresh, empty data directory for one test group.
static Options fresh(const string& name, size_t mem = 1u << 30, int max_tables = 4, int interval = 16) {
    string dir = "test_data_" + name;
    fs::remove_all(dir);
    Options o;
    o.data_dir = dir;
    o.memtable_size_bytes = mem;
    o.level_max_tables = max_tables;
    o.index_interval = interval;
    return o;
}

static bool has(const optional<string>& v, const string& want) { return v.has_value() && *v == want; }

// ---------------------------------------------------------------- 1. basic CRUD
static void test_crud() {
    std::printf("[1] basic CRUD\n");
    LSM db(fresh("crud"));
    db.put("a", "1");
    check(has(db.get("a"), "1"), "put then get");
    db.put("a", "2");
    check(has(db.get("a"), "2"), "overwrite returns latest");
    db.put("b", "hello");
    db.put("c", "world");
    check(has(db.get("b"), "hello"), "second key");
    check(has(db.get("c"), "world"), "third key");
    db.del("a");
    check(!db.get("a").has_value(), "delete then get is empty");
    check(!db.get("zzz").has_value(), "missing key is empty");
    db.put("a", "3");
    check(has(db.get("a"), "3"), "put after delete revives key");
    db.put("", "empty-key");
    check(has(db.get(""), "empty-key"), "empty key round-trips");
    db.put("k", "");
    check(has(db.get("k"), ""), "empty value round-trips");
    check(has(db.get("b"), "hello"), "untouched key unchanged");
}

// ---------------------------------------------------------------- 2. scan
static void test_scan() {
    std::printf("[2] scan\n");
    LSM db(fresh("scan", 4096, 2, 4));  // small memtable so data spans several SSTables
    for (int i = 0; i < 200; ++i) {
        char k[16]; std::snprintf(k, sizeof(k), "k%04d", i);
        db.put(k, "v" + std::to_string(i));
    }
    db.del("k0100");
    auto rows = db.scan("k0000", "k0300");
    check(std::is_sorted(rows.begin(), rows.end()), "scan output is sorted");
    check(rows.size() == 199, "scan excludes the one tombstone");  // 200 keys minus 1 deleted
    bool tomb_absent = std::none_of(rows.begin(), rows.end(),
                                    [](auto& p) { return p.first == "k0100"; });
    check(tomb_absent, "tombstoned key not in scan");
    check(has(db.get("k0050"), "v50"), "value correct after overlapping flushes");
    auto empty = db.scan("z0", "z9");
    check(empty.empty(), "empty range returns empty");
    auto window = db.scan("k0010", "k0020");
    check(window.size() == 10 && window.front().first == "k0010", "half-open window bounds");
}

// ---------------------------------------------------------------- 3. flush & persistence
static void test_persistence() {
    std::printf("[3] flush & persistence\n");
    Options o = fresh("persist", 4096, 4, 8);  // small memtable -> real flushes
    {
        LSM db(o);
        for (int i = 0; i < 500; ++i) db.put("p" + std::to_string(i), "val" + std::to_string(i));
        check(db.sstable_count() > 0, "writes triggered at least one flush");
        db.close();
    }
    {
        LSM db(o);  // reopen
        check(has(db.get("p0"), "val0"), "first key survives reopen");
        check(has(db.get("p499"), "val499"), "last key survives reopen");
        check(has(db.get("p250"), "val250"), "middle key survives reopen");
        int found = 0;
        for (int i = 0; i < 500; ++i) if (has(db.get("p" + std::to_string(i)), "val" + std::to_string(i))) ++found;
        check(found == 500, "all 500 keys readable across SSTables after reopen");
    }
}

// ---------------------------------------------------------------- 4. compaction
static void test_compaction() {
    std::printf("[4] compaction\n");
    Options o = fresh("compact", 1u << 30, 2, 8);  // huge memtable: only explicit flush() creates tables
    LSM db(o);
    for (int i = 0; i < 10; ++i) db.put("g" + std::to_string(i), "a"); db.flush();       // L0 = 1
    for (int i = 10; i < 20; ++i) db.put("g" + std::to_string(i), "b"); db.del("g5"); db.flush();  // L0 = 2
    for (int i = 20; i < 30; ++i) db.put("g" + std::to_string(i), "c"); db.flush();      // L0 = 3 -> compacts

    check(db.tables_at_level(0) == 0, "level 0 emptied by compaction");
    check(db.tables_at_level(1) == 1, "compaction produced one table at level 1");
    check(db.sstable_count() < 3, "file count dropped below the number of flushes");
    int found = 0;
    for (int i = 0; i < 30; ++i) if (i != 5 && db.get("g" + std::to_string(i)).has_value()) ++found;
    check(found == 29, "all live keys readable after compaction");
    check(!db.get("g5").has_value(), "deleted key stays gone after compaction");
    check(db.level_count() >= 1, "levels reported after compaction");
}

// ---------------------------------------------------------------- 5. WAL recovery
static void test_wal_recovery() {
    std::printf("[5] WAL recovery\n");
    Options o = fresh("wal", 1u << 30, 4, 16);  // huge memtable: nothing flushes, everything stays in the WAL
    {
        LSM db(o);
        for (int i = 0; i < 50; ++i) db.put("w" + std::to_string(i), "x" + std::to_string(i));
        // No close(): the destructor leaves the memtable un-flushed, simulating a crash.
    }
    {
        LSM db(o);  // reopen replays the WAL
        int found = 0;
        for (int i = 0; i < 50; ++i) if (has(db.get("w" + std::to_string(i)), "x" + std::to_string(i))) ++found;
        check(found == 50, "WAL replay restores every un-flushed write");
    }

    // Corrupt the WAL tail and confirm recovery is graceful (early records intact, no crash).
    string wal = o.data_dir + "/wal.log";
    auto sz = fs::file_size(wal);
    fs::resize_file(wal, sz - 3);  // shear off the final record (a torn write)
    {
        LSM db(o);
        check(has(db.get("w0"), "x0"), "first record survives a torn tail");
        check(has(db.get("w40"), "x40"), "early records survive a torn tail");
        int found = 0;
        for (int i = 0; i < 50; ++i) if (db.get("w" + std::to_string(i)).has_value()) ++found;
        check(found >= 49, "at most the torn last record is lost");
        db.put("after", "ok");
        check(has(db.get("after"), "ok"), "store still writable after recovery");
    }
    // Flip a byte in the middle: everything from that record on is discarded, but no crash.
    {
        Options o2 = fresh("wal2", 1u << 30, 4, 16);
        { LSM db(o2); for (int i = 0; i < 20; ++i) db.put("c" + std::to_string(i), "y"); }
        std::fstream f(o2.data_dir + "/wal.log", std::ios::in | std::ios::out | std::ios::binary);
        f.seekp(30); char bad = '\xFF'; f.write(&bad, 1); f.close();
        LSM db(o2);
        check(db.get("c0").has_value(), "records before the corruption survive");  // no crash reaching here
    }
}

// ---------------------------------------------------------------- 6. overwrite & tombstone ordering
static void test_ordering() {
    std::printf("[6] overwrite & tombstone ordering\n");
    Options o = fresh("order", 1u << 30, 4, 8);
    LSM db(o);
    db.put("A", "1");
    db.flush();                       // A=1 now lives in an SSTable
    db.put("A", "2");                 // newer value only in the memtable
    check(has(db.get("A"), "2"), "memtable shadows older SSTable value");
    db.flush();                       // both versions on disk; newest must win
    check(has(db.get("A"), "2"), "newest SSTable wins after flush");
    db.del("A");
    db.flush();
    check(!db.get("A").has_value(), "delete shadows all older versions");
    db.put("B", "old"); db.flush();
    db.put("B", "new"); db.del("B"); db.flush();
    check(!db.get("B").has_value(), "tombstone wins over interleaved writes");
    db.close();
    { LSM db2(o); check(!db2.get("A").has_value(), "delete stays after reopen"); }
}

// ---------------------------------------------------------------- 7. scale
static void test_scale() {
    std::printf("[7] scale (1M keys)\n");
    Options o = fresh("scale");
    o.memtable_size_bytes = 4u << 20;  // real 4 MB memtable
    const int N = 1'000'000;

    auto make_val = [](int i) { string v = std::to_string(i); v.resize(100, '#'); return v; };
    std::vector<int> order(N);
    std::iota(order.begin(), order.end(), 0);
    std::mt19937 rng(1234);
    std::shuffle(order.begin(), order.end(), rng);

    LSM db(o);
    auto t0 = std::chrono::steady_clock::now();
    for (int i : order) { char k[16]; std::snprintf(k, sizeof(k), "k%09d", i); db.put(k, make_val(i)); }
    db.flush();
    auto t1 = std::chrono::steady_clock::now();

    int matched = 0;
    for (int i = 0; i < N; ++i) {
        char k[16]; std::snprintf(k, sizeof(k), "k%09d", i);
        if (has(db.get(k), make_val(i))) ++matched;
    }
    auto t2 = std::chrono::steady_clock::now();

    double wr = std::chrono::duration<double>(t1 - t0).count();
    double rd = std::chrono::duration<double>(t2 - t1).count();
    check(matched == N, "all 1,000,000 keys read back exactly");
    std::printf("  write: %.2fs (%.0f K ops/s)   read: %.2fs (%.0f K ops/s)   tables=%zu levels=%d\n",
                wr, N / wr / 1000.0, rd, N / rd / 1000.0, db.sstable_count(), db.level_count());
}

int main() {
    test_crud();
    test_scan();
    test_persistence();
    test_compaction();
    test_wal_recovery();
    test_ordering();
    test_scale();

    // Tidy up the per-group data directories.
    for (auto& e : fs::directory_iterator("."))
        if (e.is_directory() && e.path().filename().string().rfind("test_data_", 0) == 0)
            fs::remove_all(e.path());

    std::printf("\n==== %d/%d checks passed, %d failed ====\n", g_passed, g_total, g_total - g_passed);
    return g_passed == g_total ? 0 : 1;
}

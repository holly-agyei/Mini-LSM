CXX      ?= g++
CXXFLAGS ?= -std=c++20 -O2 -pthread -Wall -Wextra
SRC       = lsm.cpp

.PHONY: all test bench bench-rocksdb count clean

all: test bench

# Correctness suite. Runs every group and prints a pass/fail summary.
test: test.cpp $(SRC) lsm.hpp
	$(CXX) $(CXXFLAGS) test.cpp $(SRC) -o test
	./test

# Performance harness. Prints the measured throughput / amplification / footprint numbers.
bench: bench.cpp $(SRC) lsm.hpp
	$(CXX) $(CXXFLAGS) bench.cpp $(SRC) -o bench
	./bench

# Same benchmark plus a head-to-head against RocksDB (installed via `brew install rocksdb`).
bench-rocksdb: bench.cpp $(SRC) lsm.hpp
	$(CXX) $(CXXFLAGS) -DHAVE_ROCKSDB \
		-I$(shell brew --prefix rocksdb)/include \
		bench.cpp $(SRC) \
		-L$(shell brew --prefix rocksdb)/lib -lrocksdb -o bench
	./bench

# Report the raw source line count against the 1,500-line budget.
count:
	wc -l lsm.hpp $(SRC) test.cpp bench.cpp

clean:
	rm -f test bench
	rm -rf mlsm_data bench_data test_data_* /tmp/mlsm_*

CXX ?= g++
CXXFLAGS ?= -std=c++17 -O3 -Wall -Wextra -Wpedantic -fopenmp

.PHONY: all clean
all: bin/etl

bin/etl: cpp/etl.cpp
	@mkdir -p bin
	$(CXX) $(CXXFLAGS) $< -o $@

clean:
	rm -f bin/etl

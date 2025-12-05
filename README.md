# DODA — Data Ordered Dense Array
A small, deterministic timeseries data store for embedded/IoT firmware.

## Overview
- In-memory, fixed-size arrays; no heap or filesystem.
- Optimized for integer timestamped samples and simple range queries.
- Deterministic operations with bounded memory and timing.

## Key features
- Timeseries first: append samples with INT timestamps; range queries (>=, >, <).
- Primary-key hash on first INT column for O(1) equality lookups.
- Optional per-column sorted index for efficient range scans.
- Safe deletes with slot reuse via a free list.
- Compile-time feature gates to reduce footprint (disable text/float/double/pointers/stdio).

## Build and run
- Prerequisites: CMake 3.15+, Clang/AppleClang (macOS)
- Clean: rm -rf build
- Host/tests (timeseries):
  - cmake -S . -B build -DDRIVERSQL_FIRMWARE=OFF -DDRIVERSQL_TIMESERIES=ON
  - cmake --build build
  - ./build/doda
- Firmware-only library:
  - cmake -S . -B build -DDRIVERSQL_FIRMWARE=ON
  - cmake --build build

## Usage (host/tests)
- Define schema with INT time column; initialize and append; query ranges.
// ...existing code examples...

## Configuration (feature gates)
- DRIVERSQL_NO_STDIO, DRIVERSQL_NO_POINTER_COLUMN
- DRIVERSQL_NO_TEXT, DRIVERSQL_NO_FLOAT, DRIVERSQL_NO_DOUBLE
- DRIVERSQL_MAX_ROWS, DRIVERSQL_MAX_COLUMNS, DRIVERSQL_MAX_TEXT_LEN, DRIVERSQL_HASH_SIZE
- DRIVERSQL_TIMESERIES (enable timeseries helpers)

## Limits and timing
- Capacity: MAX_ROWS; Columns: MAX_COLUMNS.
- Insert: O(1) avg; PK eq: O(1); ranges: O(N) or O(log N + R) with index.
- Deleted slots reused via free_list; DS_ERR_FULL when no free slots.

## Concurrency and ISR safety
- Single-writer, non-reentrant; no internal locks.
- Do not mutate in ISRs; reads only when writers excluded.

## Production checklist
- Schema validation vs feature gates; strict status codes.
- Replace stdio with logging hooks for firmware.
- Static analysis (MISRA/CERT), unit/property tests, CI.

## Memory requirements
- Core table overhead (independent of columns):
  - deleted_bits: (MAX_ROWS + 63)/64 × 8 bytes
  - free_list: MAX_ROWS × 2 bytes
  - pk_hash: HASH_SIZE × 2 bytes (HASH_SIZE must be power of two)
  - Other fields (name, counters): ~64–128 bytes
- Per-column storage (multiply by number of columns of each type):
  - INT: MAX_ROWS × 4 bytes
  - BOOL: MAX_ROWS × 1 byte
  - FLOAT: MAX_ROWS × 4 bytes (omit with -DDRIVERSQL_NO_FLOAT)
  - DOUBLE: MAX_ROWS × 8 bytes (omit with -DDRIVERSQL_NO_DOUBLE)
  - TEXT: MAX_ROWS × MAX_TEXT_LEN bytes (omit with -DDRIVERSQL_NO_TEXT)
  - POINTER: MAX_ROWS × pointer_size (omit with -DDRIVERSQL_NO_POINTER_COLUMN)
- Quick estimates (defaults: MAX_ROWS=256, HASH_SIZE=512, MAX_TEXT_LEN=64):
  - Core overhead ≈ deleted_bits(32B) + free_list(512B) + pk_hash(1024B) + misc ≈ 1.7KB
  - 3-column INT/INT/INT: 3 × (256 × 4B) = 3KB → total ≈ 4.7KB
  - INT/TEXT(64)/INT: INT(1KB) + TEXT(16KB) + INT(1KB) = 18KB → total ≈ ~19.7KB
  - INT/BOOL/INT: 1KB + 256B + 1KB ≈ 2.25KB → total ≈ ~4KB
- Tuning tips:
  - Reduce MAX_ROWS and MAX_TEXT_LEN to fit RAM budget.
  - Disable unused types via feature gates to remove their storage entirely.
  - For timeseries, prefer INT metrics (scaled units) to minimize footprint.

## License
MIT License. See LICENSE.

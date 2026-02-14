# DODA — Data Ordered Dense Array
A small, deterministic timeseries data store for embedded/IoT firmware.

## The “Why” (the logic)
**DODA** stands for **Data Ordered Dense Array**.

- **The name**: data is kept in *ordered* (typically time-ordered) sequences and stored in *dense* contiguous arrays. This maximizes cache locality, keeps memory use predictable, and makes performance easy to reason about on MCUs.
- **The problem**: popular TSDB/database options (e.g., Influx-like stacks, SQLite) are often too heavy for a bare-metal RTOS or a tiny MCU: they bring filesystem assumptions, dynamic allocation, larger code size, and less predictable latencies.
- **The solution**: DODA sits in the middle ground—structured storage (schema, indexing, predicates) without the overhead of a full database server.

## Key features
- Timeseries first: append samples with INT timestamps; range queries (>=, >, <).
- Primary-key hash on first INT column for O(1) equality lookups.
- Optional per-column sorted index for efficient range scans.
- Safe deletes with slot reuse via a free list.
- Compile-time feature gates to reduce footprint (disable text/float/double/pointers/stdio).

## Technical features (firmware-oriented)
- **Resource constrained**: fixed-size RAM tables; bounded runtime; no mandatory heap usage.
- **Flash-memory aware**: persistence is implemented as sequential streaming I/O (so platform backends *can* minimize write amplification). CRC32 is supported to detect corruption/partial writes.
- **SQL-like interface**: SQL-*style* querying via C APIs (e.g., `select_where_*`) and optional driver-facing helpers.
- **Time-series core**: timestamp-first layout with optional sorted indexes. Complexity depends on whether an index is built/used: full scan `O(n)`; index-assisted queries can be `O(log n + R)` (binary search + scan of result range).

## SQL query architecture (design notes)
DODA’s SQL-like querying is designed to remain safe and small on embedded systems:
- **Predicate filtering**: predicates are evaluated directly against dense column arrays; deleted rows are skipped via the deleted bitset.
- **Index acceleration**: when an `Index` is built for a column, equality/range operations can be served by binary search + contiguous scan over matching rows.

> Note: this repository currently exposes the query surface via C APIs (not a text SQL parser). If/when a text `SELECT ... WHERE ...` parser is added, it should follow embedded constraints (single pass, bounded buffers, no heap).

## Proposed roadmap (future features)
High-value additions that fit embedded constraints:
- **Data compression**: delta-of-delta timestamps and Gorilla/XOR encoding for numeric series.
- **Retention policies**: automatic pruning / downsampling of old samples.
- **Power-failure resilience**: atomic commits (double-buffer) or a lightweight WAL.
- **Edge-to-cloud sync hooks**: batch export to MQTT/HTTP (application-provided callbacks).

## Community, contributions, and license
- **License**: MIT (permissive for hobby and commercial use).
- **Contributions welcome**: especially from engineers with RTOS/MCU experience (flash wear-leveling, power-fail safe writes, ISR-safe patterns, and profiling/benchmarks).

## Build and run
- Prerequisites: CMake 3.15+, Clang/AppleClang (macOS)
- Clean: rm -rf build

- Host/tests (timeseries + persistence, default):
  - cmake -S . -B build \
      -DDRIVERSQL_FIRMWARE=OFF \
      -DDRIVERSQL_TIMESERIES=ON \
      -DDODA_PERSIST=ON
  - cmake --build build
  - ./build/doda

- Host/tests (disable persistence):
  - cmake -S . -B build \
      -DDRIVERSQL_FIRMWARE=OFF \
      -DDRIVERSQL_TIMESERIES=ON \
      -DDODA_PERSIST=OFF

- Host/tests (disable CRC for persistence format):
  - cmake -S . -B build \
      -DDRIVERSQL_FIRMWARE=OFF \
      -DDRIVERSQL_TIMESERIES=ON \
      -DDODA_PERSIST=ON \
      -DDODA_PERSIST_NO_CRC=ON

- Firmware-only library (no host test binary):
  - cmake -S . -B build \
      -DDRIVERSQL_FIRMWARE=ON
  - cmake --build build

- Optional: compile the flash/EEPROM backend template (usually OFF; provided to copy into your project):
  - cmake -S . -B build \
      -DDRIVERSQL_FIRMWARE=OFF \
      -DDODA_PERSIST=ON \
      -DDODA_BUILD_FLASH_STUB=ON

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

## Persistence (optional)
DODA is in-memory by default. Persistence is provided by a **separate, portable module** that serializes tables to a platform-defined storage backend.

Files:
- `doda_persist.h/.c`: portable save/load and binary format
- `doda_storage_flash_stub.h/.c`: MCU flash/EEPROM **template** backend (HAL hooks required)

### How it works
- The core engine never touches files/flash.
- You provide a `DodaStorage` with callbacks:
  - `write_all(ctx, data, size)`
  - `read_all(ctx, data, size)`
  - optional `erase(ctx)`
- `doda_persist_save_table()` writes:
  1) header (magic/version/build limits)
  2) schema (column names + types)
  3) row index list (non-deleted rows)
  4) row payload (non-deleted rows only)
- `doda_persist_load_table()` validates header/schema and rebuilds the table in RAM.

### Integrity (CRC32)
- CRC32 is enabled by default and detects corruption/power-fail partial writes.
- Disable CRC at build time with: `-DDODA_PERSIST_NO_CRC`

### Notes / constraints
- Deleted rows are not stored (load compacts rows).
- Pointer columns are not persisted.
- Load validates build limits (e.g., `MAX_ROWS`, `HASH_SIZE`) match the persisted file.

## Aggregations (helpers)
Basic aggregations over **non-deleted** rows for INT columns:
- `agg_min_int(t, "col", &out)`
- `agg_max_int(t, "col", &out)`
- `agg_avg_int(t, "col", &out)`
- `agg_count(t)`

## CMake options
- `DRIVERSQL_FIRMWARE=ON|OFF`: build firmware-only (no host test binary)
- `DRIVERSQL_TIMESERIES=ON|OFF`: enable timeseries helpers
- `DODA_PERSIST=ON|OFF`: build persistence module (`doda_persist.*`)
- `DODA_BUILD_FLASH_STUB=ON|OFF`: compile the flash/EEPROM template backend (OFF by default)

## License
MIT License. See LICENSE.

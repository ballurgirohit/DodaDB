#pragma once
#include "doda_engine.h"

#ifdef __cplusplus
extern "C" {
#endif

// Persistence is intentionally kept out of the core engine. This header defines a
// platform-agnostic storage interface and portable (de)serialization routines.
//
// Typical usage:
//  - Provide a DodaStorage implementation for your platform (file, flash, nvms).
//  - Call doda_persist_save_table(...) / doda_persist_load_table(...).

// Enable CRC32 integrity check by default. Define DODA_PERSIST_NO_CRC to disable.
#ifndef DODA_PERSIST_NO_CRC
#define DODA_PERSIST_HAS_CRC 1
#else
#define DODA_PERSIST_HAS_CRC 0
#endif

// Storage interface (implemented by the application/platform)
typedef struct DodaStorage {
    void *ctx;

    // Return true on success
    bool (*write_all)(void *ctx, const void *data, size_t size);

    // Return true on success
    bool (*read_all)(void *ctx, void *data, size_t size);

    // Optional: erase/clear medium before write (may be NULL)
    bool (*erase)(void *ctx);
} DodaStorage;

// Persisted format version
#define DODA_PERSIST_VERSION 1u

// Error codes for persistence
typedef enum {
    DODA_PERSIST_OK = 0,
    DODA_PERSIST_ERR_INVALID,
    DODA_PERSIST_ERR_IO,
    DODA_PERSIST_ERR_UNSUPPORTED,
    DODA_PERSIST_ERR_CORRUPT
} DodaPersistStatus;

// Save/load a Table in a portable binary format.
// Notes:
//  - Only non-deleted rows are stored.
//  - Pointer columns are never persisted.
//  - TEXT/FLOAT/DOUBLE are persisted only if enabled in the build.
//  - Table schema (column names/types) is stored in the header and validated on load.
DodaPersistStatus doda_persist_save_table(const DodaTable *t, const DodaStorage *st);
DodaPersistStatus doda_persist_load_table(DodaTable *out, const DodaStorage *st);

// Size estimation for persistence payload (worst-case, includes header)
// Useful for preallocating flash pages/buffers.
size_t doda_persist_estimate_max_bytes(const DodaTable *t);

#ifdef __cplusplus
}
#endif

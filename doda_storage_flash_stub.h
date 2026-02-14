#pragma once
#include "doda_persist.h"

// Platform backend template for MCU Flash/EEPROM.
//
// This is a *stub/template* intentionally: you must wire it to your HAL/driver.
// The persistence module (`doda_persist.c`) stays platform-agnostic and calls
// the function pointers in `DodaStorage`.
//
// Typical strategy on MCU flash:
//  - Choose a fixed region/partition.
//  - Implement erase() for sector/page erase.
//  - Implement write_all() as sequential writes at an internal cursor.
//  - Implement read_all() as sequential reads at an internal cursor.
//  - Consider wear-leveling and power-fail safety (double-buffer + CRC).

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    // Base address of your flash region (or an opaque handle)
    uintptr_t base_addr;

    // Total length available for persistence
    size_t region_size;

    // Current cursor for sequential read/write
    size_t cursor;

    // Hook: erase region (sector/page erase). Return true on success.
    bool (*flash_erase_region)(uintptr_t base_addr, size_t region_size);

    // Hook: program bytes to flash (must satisfy alignment/word-write rules).
    bool (*flash_program)(uintptr_t addr, const void *data, size_t size);

    // Hook: read bytes from flash.
    bool (*flash_read)(uintptr_t addr, void *data, size_t size);

    // Optional: user context passed to hooks (HAL handle)
    void *user;
} DodaFlashStorageCtx;

// Initialize a DodaStorage instance using the flash ctx.
// You provide flash_erase_region/flash_program/flash_read hooks.
void doda_flash_storage_init(DodaStorage *out, DodaFlashStorageCtx *ctx);

#ifdef __cplusplus
}
#endif

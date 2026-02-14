#include "doda_storage_flash_stub.h"
#include <string.h>

static bool flash_erase(void *vctx) {
    DodaFlashStorageCtx *ctx = (DodaFlashStorageCtx *)vctx;
    ctx->cursor = 0;
    if (!ctx->flash_erase_region) return false;
    return ctx->flash_erase_region(ctx->base_addr, ctx->region_size);
}

static bool flash_write_all(void *vctx, const void *data, size_t size) {
    DodaFlashStorageCtx *ctx = (DodaFlashStorageCtx *)vctx;
    if (!ctx->flash_program) return false;
    if (ctx->cursor + size > ctx->region_size) return false;

    uintptr_t addr = ctx->base_addr + (uintptr_t)ctx->cursor;
    bool ok = ctx->flash_program(addr, data, size);
    if (!ok) return false;
    ctx->cursor += size;
    return true;
}

static bool flash_read_all(void *vctx, void *data, size_t size) {
    DodaFlashStorageCtx *ctx = (DodaFlashStorageCtx *)vctx;
    if (!ctx->flash_read) return false;
    if (ctx->cursor + size > ctx->region_size) return false;

    uintptr_t addr = ctx->base_addr + (uintptr_t)ctx->cursor;
    bool ok = ctx->flash_read(addr, data, size);
    if (!ok) return false;
    ctx->cursor += size;
    return true;
}

void doda_flash_storage_init(DodaStorage *out, DodaFlashStorageCtx *ctx) {
    if (!out || !ctx) return;
    // Reset cursor so first operation starts at offset 0
    ctx->cursor = 0;
    out->ctx = ctx;
    out->write_all = flash_write_all;
    out->read_all = flash_read_all;
    out->erase = flash_erase;
}

/*
 * Copyright (c) 2025 Rohit Ballurgi
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software... [rest of standard MIT short-text]
 * ...
 * MIT License (see LICENSE file for full text)
 */
#include "doda_engine.h"
#ifdef DRIVERSQL_TIMESERIES
#include "doda_api.h"
#endif
#include <stdio.h>
#include "doda_persist.h"
#include "doda_storage_flash_stub.h"
#include <string.h>

static void print_cb(const DodaTable *tab, size_t row, void *user) {
    (void)user; doda_print_row(tab, row);
}

static void test_basic(void) {
    const char *cols[] = {"id", "name", "age"};
    DodaColumnType types[] = {COL_INT, COL_TEXT, COL_INT};
    DodaTable t; doda_init_table(&t, "people", 3, cols, types);
    doda_insert_row_int_text_int(&t, 1, "Alice", 30);
    doda_insert_row_int_text_int(&t, 2, "Bob", 22);
    doda_insert_row_int_text_int(&t, 3, "Cara", 22);
    printf("All rows before delete:\n");
    for (size_t r = 0; r < t.count; ++r) if (!doda_is_deleted(&t, r)) doda_print_row(&t, r);
    int target_age = 22; doda_select_where_eq(&t, "age", &target_age, print_cb, NULL);
    size_t deleted = 0; doda_delete_where_eq(&t, "name", "Bob", &deleted);
    printf("Deleted: %zu\n", deleted);
}

#ifdef DRIVERSQL_TIMESERIES
static void test_timeseries(void) {
    const char *cols[] = {"id", "time", "value"};
    DodaColumnType types[] = {COL_INT, COL_INT, COL_INT};
    DodaTable t; doda_init_table(&t, "metrics", 3, cols, types);
    DodaTSDB ts; doda_tsdb_init(&ts, &t, "time");
    doda_tsdb_append_int3(&ts, 1, 1000, 42);
    doda_tsdb_append_int3(&ts, 2, 1500, 43);
    doda_tsdb_append_int3(&ts, 3, 2000, 44);
    printf("Timeseries: time >= 1500\n");
    int t0 = 1500; doda_tsdb_select_time_ge(&ts, t0, (doda_row_callback)print_cb, NULL);
}
#endif

// Aggregations test
static void test_aggregations(void) {
    const char *cols[] = {"id", "time", "value"};
    DodaColumnType types[] = {COL_INT, COL_INT, COL_INT};
    DodaTable t; doda_init_table(&t, "agg_metrics", 3, cols, types);

    // Insert samples
    DodaTSDB ts; doda_tsdb_init(&ts, &t, "time");
    doda_tsdb_append_int3(&ts, 1, 1000, 40);
    doda_tsdb_append_int3(&ts, 2, 1500, 50);
    doda_tsdb_append_int3(&ts, 3, 2000, 35);
    doda_tsdb_append_int3(&ts, 4, 2500, 60);

    // Run aggregations on 'value'
    int minv=0, maxv=0; double avg=0.0; size_t cnt=0;
    if (agg_min_int((const Table *)&t, "value", &minv)) printf("min(value)=%d\n", minv);
    if (agg_max_int((const Table *)&t, "value", &maxv)) printf("max(value)=%d\n", maxv);
    if (agg_avg_int((const Table *)&t, "value", &avg)) printf("avg(value)=%.2f\n", avg);
    cnt = agg_count((const Table *)&t); printf("count(rows)=%zu\n", cnt);
}

#ifndef DRIVERSQL_FIRMWARE
// Simple file-backed storage backend (host example only)
typedef struct {
    const char *path;
    bool write_mode;
} FileStorageCtx;

static bool file_erase(void *ctx) {
    FileStorageCtx *c = (FileStorageCtx *)ctx;
    FILE *f = fopen(c->path, "wb");
    if (!f) return false;
    fclose(f);
    return true;
}

static bool file_write_all(void *ctx, const void *data, size_t size) {
    FileStorageCtx *c = (FileStorageCtx *)ctx;
    FILE *f = fopen(c->path, "ab");
    if (!f) return false;
    size_t n = fwrite(data, 1, size, f);
    fclose(f);
    return n == size;
}

static bool file_read_all(void *ctx, void *data, size_t size) {
    FileStorageCtx *c = (FileStorageCtx *)ctx;
    static FILE *f = NULL;
    if (!f) {
        f = fopen(c->path, "rb");
        if (!f) return false;
    }
    size_t n = fread(data, 1, size, f);
    if (n != size) { fclose(f); f = NULL; return false; }
    return true;
}

static void file_read_reset(void) {
    // crude reset for this test
}

static void test_persistence_roundtrip(void) {
    const char *cols[] = {"id", "time", "value"};
    DodaColumnType types[] = {COL_INT, COL_INT, COL_INT};

    DodaTable t; doda_init_table(&t, "persisted", 3, cols, types);
#ifdef DRIVERSQL_TIMESERIES
    DodaTSDB ts; doda_tsdb_init(&ts, &t, "time");
    doda_tsdb_append_int3(&ts, 1, 1000, 40);
    doda_tsdb_append_int3(&ts, 2, 1500, 50);
    doda_tsdb_append_int3(&ts, 3, 2000, 35);
#else
    const void *vals[3]; int id=1,time=1000,val=40; vals[0]=&id; vals[1]=&time; vals[2]=&val; doda_insert_row(&t, vals);
#endif

    const char *path = "./doda_test.bin";
    FileStorageCtx wctx = { path, true };
    DodaStorage stw = { &wctx, file_write_all, NULL, file_erase };
    DodaPersistStatus ps = doda_persist_save_table(&t, &stw);
    printf("persist save status=%d\n", (int)ps);

    // Load
    FileStorageCtx rctx = { path, false };
    DodaStorage str = { &rctx, NULL, file_read_all, NULL };
    // reset simple reader by relying on static FILE* opening on first read
    DodaTable loaded;
    DodaPersistStatus pl = doda_persist_load_table(&loaded, &str);
    printf("persist load status=%d\n", (int)pl);

    printf("loaded rows=%zu\n", loaded.count);
    for (size_t r = 0; r < loaded.count; ++r) if (!doda_is_deleted(&loaded, r)) doda_print_row(&loaded, r);
}

static void test_persistence_load_only(void) {
    const char *path = "./doda_test.bin";

    FileStorageCtx rctx = { path, false };
    DodaStorage str = { &rctx, NULL, file_read_all, NULL };

    DodaTable loaded;
    DodaPersistStatus pl = doda_persist_load_table(&loaded, &str);
    printf("persist load-only status=%d\n", (int)pl);

    if (pl != DODA_PERSIST_OK) return;

    printf("loaded rows=%zu\n", loaded.count);
    for (size_t r = 0; r < loaded.count; ++r)
        if (!doda_is_deleted(&loaded, r))
            doda_print_row(&loaded, r);
}

// Fake flash region in RAM for testing the flash stub adapter
#define FAKE_FLASH_SIZE (64u * 1024u)
static uint8_t g_fake_flash[FAKE_FLASH_SIZE];

static bool fake_flash_erase(uintptr_t base, size_t region_size) {
    (void)base;
    if (region_size > FAKE_FLASH_SIZE) return false;
    memset(g_fake_flash, 0xFF, region_size);
    return true;
}

static bool fake_flash_program(uintptr_t addr, const void *data, size_t size) {
    // addr is treated as offset into g_fake_flash for this host test
    if ((size_t)addr + size > FAKE_FLASH_SIZE) return false;
    memcpy(&g_fake_flash[(size_t)addr], data, size);
    return true;
}

static bool fake_flash_read(uintptr_t addr, void *data, size_t size) {
    if ((size_t)addr + size > FAKE_FLASH_SIZE) return false;
    memcpy(data, &g_fake_flash[(size_t)addr], size);
    return true;
}

static void test_flash_stub_with_persistence(void) {
    const char *cols[] = {"id", "time", "value"};
    DodaColumnType types[] = {COL_INT, COL_INT, COL_INT};

    DodaTable t;
    doda_init_table(&t, "flash", 3, cols, types);
#ifdef DRIVERSQL_TIMESERIES
    DodaTSDB ts;
    doda_tsdb_init(&ts, &t, "time");
    doda_tsdb_append_int3(&ts, 1, 1000, 11);
    doda_tsdb_append_int3(&ts, 2, 2000, 22);
#else
    const void *vals[3];
    int id=1, tm=1000, v=11;
    vals[0]=&id; vals[1]=&tm; vals[2]=&v;
    doda_insert_row(&t, vals);
#endif

    // Wire fake flash hooks into the flash-stub adapter
    DodaFlashStorageCtx fctx = {
        .base_addr = 0,
        .region_size = FAKE_FLASH_SIZE,
        .cursor = 0,
        .flash_erase_region = fake_flash_erase,
        .flash_program = fake_flash_program,
        .flash_read = fake_flash_read,
        .user = NULL,
    };

    DodaStorage st;
    doda_flash_storage_init(&st, &fctx);

    // Save to "flash"
    DodaPersistStatus ps = doda_persist_save_table(&t, &st);
    printf("flash-stub save status=%d\n", (int)ps);

    // Reset cursor and load back from same region
    fctx.cursor = 0;
    DodaTable loaded;
    DodaPersistStatus pl = doda_persist_load_table(&loaded, &st);
    printf("flash-stub load status=%d\n", (int)pl);

    if (pl != DODA_PERSIST_OK) return;
    printf("flash-stub loaded rows=%zu\n", loaded.count);
    for (size_t r = 0; r < loaded.count; ++r)
        if (!doda_is_deleted(&loaded, r))
            doda_print_row(&loaded, r);
}
#endif

int main(void) {
    // test_basic();
#ifdef DRIVERSQL_TIMESERIES
    // test_timeseries();
#endif
    // test_aggregations();
#ifndef DRIVERSQL_FIRMWARE
    // test_persistence_roundtrip();
    test_persistence_load_only();
    test_flash_stub_with_persistence();
#endif
    return 0;
}

#include "test_framework.h"
#include "doda_engine.h"
#include "doda_persist.h"

#include <string.h>

typedef struct {
    uint8_t *buf;
    size_t cap;
    size_t pos;
    bool allow_erase;
} MemStore;

static bool mem_erase(void *ctx) {
    MemStore *m = (MemStore *)ctx;
    if (!m->allow_erase) return false;
    memset(m->buf, 0xFF, m->cap);
    m->pos = 0;
    return true;
}

static bool mem_write_all(void *ctx, const void *data, size_t size) {
    MemStore *m = (MemStore *)ctx;
    if (m->pos + size > m->cap) return false;
    memcpy(m->buf + m->pos, data, size);
    m->pos += size;
    return true;
}

static bool mem_read_all(void *ctx, void *data, size_t size) {
    MemStore *m = (MemStore *)ctx;
    if (m->pos + size > m->cap) return false;
    memcpy(data, m->buf + m->pos, size);
    m->pos += size;
    return true;
}

static void mem_reset(MemStore *m) { m->pos = 0; }

// forward declarations
static void cb_find_id_eq_needle(const DodaTable *tab, size_t row, void *user);

DODA_TEST(test_persist_roundtrip_memstore) {
    const char *cols[] = {"id", "time", "value"};
    DodaColumnType types[] = {COL_INT, COL_INT, COL_INT};

    DodaTable t;
    doda_init_table(&t, "p", 3, cols, types);

    for (int i = 0; i < 10; ++i) {
        int id = i + 1;
        int tm = 1000 + i * 10;
        int val = i * 2;
        const void *vals[] = {&id, &tm, &val};
        DODA_ASSERT_EQ_INT(DS_OK, doda_insert_row(&t, vals));
    }

    uint8_t buf[8192];
    MemStore ms = { buf, sizeof(buf), 0, true };
    DodaStorage stw = { &ms, mem_write_all, NULL, mem_erase };

    DodaPersistStatus ps = doda_persist_save_table(&t, &stw);
    DODA_ASSERT_EQ_INT(DODA_PERSIST_OK, ps);

    mem_reset(&ms);
    DodaStorage str = { &ms, NULL, mem_read_all, NULL };
    DodaTable loaded;
    DodaPersistStatus pl = doda_persist_load_table(&loaded, &str);
    DODA_ASSERT_EQ_INT(DODA_PERSIST_OK, pl);

    DODA_ASSERT_EQ_INT(t.column_count, loaded.column_count);
    DODA_ASSERT_EQ_INT(t.count, loaded.count);

    // Spot check a value
    int needle = 5;
    size_t cnt = 0;
    doda_select_where_eq(&loaded, "id", &needle, cb_find_id_eq_needle, &cnt);
    DODA_ASSERT_EQ_INT(1, cnt);
}

// helper callback for spot-checking loaded tables
static void cb_find_id_eq_needle(const DodaTable *tab, size_t row, void *user) {
    // user points to a size_t counter; needle is fixed to 5 for this test table structure
    // (keep callback signature compatible with doda_select_where_eq)
    size_t *cnt = (size_t *)user;
    if (!cnt) return;
    if (tab->columns[0].data.int_data[row] == 5) (*cnt)++;
}

#if DODA_PERSIST_HAS_CRC
DODA_TEST(test_persist_crc_detects_corruption) {
    const char *cols[] = {"id", "value"};
    DodaColumnType types[] = {COL_INT, COL_INT};

    DodaTable t;
    doda_init_table(&t, "p", 2, cols, types);

    for (int i = 0; i < 5; ++i) {
        int id=i+1, v=i;
        const void *vals[] = {&id, &v};
        DODA_ASSERT_EQ_INT(DS_OK, doda_insert_row(&t, vals));
    }

    uint8_t buf[4096];
    MemStore ms = { buf, sizeof(buf), 0, true };
    DodaStorage stw = { &ms, mem_write_all, NULL, mem_erase };
    DODA_ASSERT_EQ_INT(DODA_PERSIST_OK, doda_persist_save_table(&t, &stw));

    // Flip a byte in the payload region (after full header). The on-disk header is
    // 30 bytes when CRC is enabled: 4 magic + 9*2 u16 fields + 4 payload_bytes + 4 crc32.
    size_t flip = 30u + 8u;
    if (flip < sizeof(buf)) buf[flip] ^= 0x5A;

    mem_reset(&ms);
    DodaStorage str = { &ms, NULL, mem_read_all, NULL };
    DodaTable loaded;
    DodaPersistStatus pl = doda_persist_load_table(&loaded, &str);
    DODA_ASSERT_EQ_INT(DODA_PERSIST_ERR_CORRUPT, pl);
}
#endif

static void wr_u16_le(uint8_t *p, uint16_t v) { p[0] = (uint8_t)v; p[1] = (uint8_t)(v >> 8); }
static void wr_u32_le(uint8_t *p, uint32_t v) { p[0] = (uint8_t)v; p[1] = (uint8_t)(v >> 8); p[2] = (uint8_t)(v >> 16); p[3] = (uint8_t)(v >> 24); }

#ifndef DRIVERSQL_NO_POINTER_COLUMN
DODA_TEST(test_persist_rejects_pointer_column) {
    const char *cols[] = {"id", "ptr"};
    DodaColumnType types[] = {COL_INT, COL_POINTER};
    DodaTable t;
    doda_init_table(&t, "ptr", 2, cols, types);

    int id = 1;
    void *p = (void *)0x1234;
    const void *vals[] = { &id, &p };
    (void)doda_insert_row(&t, vals);

    uint8_t buf[1024];
    MemStore ms = { buf, sizeof(buf), 0, true };
    DodaStorage stw = { &ms, mem_write_all, NULL, mem_erase };

    DodaPersistStatus ps = doda_persist_save_table(&t, &stw);
    DODA_ASSERT_EQ_INT(DODA_PERSIST_ERR_UNSUPPORTED, ps);
}
#endif

DODA_TEST(test_persist_load_rejects_bad_magic) {
    // Craft a minimal header with wrong magic.
#if DODA_PERSIST_HAS_CRC
    const size_t header_bytes = 30u;
#else
    const size_t header_bytes = 26u;
#endif
    uint8_t buf[64];
    memset(buf, 0, sizeof(buf));

    // wrong magic 'XXXX'
    wr_u32_le(&buf[0], 0x58585858u);
    wr_u16_le(&buf[4], (uint16_t)DODA_PERSIST_VERSION);
    wr_u16_le(&buf[6], (uint16_t)header_bytes);
    wr_u16_le(&buf[8], 1);
    wr_u16_le(&buf[10], 0);
    wr_u16_le(&buf[12], (uint16_t)MAX_ROWS);
    wr_u16_le(&buf[14], (uint16_t)MAX_COLUMNS);
    wr_u16_le(&buf[16], (uint16_t)MAX_NAME_LEN);
#ifndef DRIVERSQL_NO_TEXT
    wr_u16_le(&buf[18], (uint16_t)MAX_TEXT_LEN);
#else
    wr_u16_le(&buf[18], 0);
#endif
    wr_u16_le(&buf[20], (uint16_t)HASH_SIZE);
    wr_u32_le(&buf[22], 0u);
#if DODA_PERSIST_HAS_CRC
    wr_u32_le(&buf[26], 0u);
#endif

    MemStore ms = { buf, sizeof(buf), 0, false };
    mem_reset(&ms);
    DodaStorage st = { &ms, NULL, mem_read_all, NULL };
    DodaTable out;
    DodaPersistStatus pl = doda_persist_load_table(&out, &st);
    DODA_ASSERT_EQ_INT(DODA_PERSIST_ERR_CORRUPT, pl);
}

DODA_TEST(test_persist_load_rejects_unsupported_version) {
#if DODA_PERSIST_HAS_CRC
    const size_t header_bytes = 30u;
#else
    const size_t header_bytes = 26u;
#endif
    uint8_t buf[64];
    memset(buf, 0, sizeof(buf));

    // magic 'DODA'
    wr_u32_le(&buf[0], 0x41444F44u);
    // unsupported version
    wr_u16_le(&buf[4], (uint16_t)(DODA_PERSIST_VERSION + 1u));
    wr_u16_le(&buf[6], (uint16_t)header_bytes);
    wr_u16_le(&buf[8], 1);
    wr_u16_le(&buf[10], 0);
    wr_u16_le(&buf[12], (uint16_t)MAX_ROWS);
    wr_u16_le(&buf[14], (uint16_t)MAX_COLUMNS);
    wr_u16_le(&buf[16], (uint16_t)MAX_NAME_LEN);
#ifndef DRIVERSQL_NO_TEXT
    wr_u16_le(&buf[18], (uint16_t)MAX_TEXT_LEN);
#else
    wr_u16_le(&buf[18], 0);
#endif
    wr_u16_le(&buf[20], (uint16_t)HASH_SIZE);
    wr_u32_le(&buf[22], 0u);
#if DODA_PERSIST_HAS_CRC
    wr_u32_le(&buf[26], 0u);
#endif

    MemStore ms = { buf, sizeof(buf), 0, false };
    mem_reset(&ms);
    DodaStorage st = { &ms, NULL, mem_read_all, NULL };
    DodaTable out;
    DodaPersistStatus pl = doda_persist_load_table(&out, &st);
    DODA_ASSERT_EQ_INT(DODA_PERSIST_ERR_UNSUPPORTED, pl);
}

void doda_register_persist_tests(void) {
    DODA_REGISTER(test_persist_roundtrip_memstore);
#if DODA_PERSIST_HAS_CRC
    DODA_REGISTER(test_persist_crc_detects_corruption);
#endif
#ifndef DRIVERSQL_NO_POINTER_COLUMN
    DODA_REGISTER(test_persist_rejects_pointer_column);
#endif
    DODA_REGISTER(test_persist_load_rejects_bad_magic);
    DODA_REGISTER(test_persist_load_rejects_unsupported_version);
}

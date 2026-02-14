#include "test_framework.h"
#include "doda_engine.h"

#include <string.h>
#include <stdint.h>

static void cb_count(const DodaTable *t, size_t row, void *user) {
    (void)t; (void)row;
    size_t *cnt = (size_t *)user;
    (*cnt)++;
}

static uint32_t xorshift32(uint32_t *state) {
    uint32_t x = *state;
    x ^= x << 13;
    x ^= x >> 17;
    x ^= x << 5;
    *state = x;
    return x;
}

static void cb_collect_row_ids(const DodaTable *t, size_t row, void *user) {
    (void)t;
    uint16_t *out = (uint16_t *)user;
    // Convention: out[0] is count, then values.
    uint16_t n = out[0];
    if (n + 1u < MAX_ROWS) {
        out[0] = (uint16_t)(n + 1u);
        out[1u + n] = (uint16_t)row;
    }
}

DODA_TEST(test_insert_and_select_eq_int) {
    const char *cols[] = {"id", "age"};
    DodaColumnType types[] = {COL_INT, COL_INT};
    DodaTable t;
    doda_init_table(&t, "people", 2, cols, types);

    int id1=1, age1=10;
    const void *v1[] = {&id1, &age1};
    DODA_ASSERT_EQ_INT(DodaStatus_OK, doda_insert_row(&t, v1));

    int id2=2, age2=20;
    const void *v2[] = {&id2, &age2};
    DODA_ASSERT_EQ_INT(DodaStatus_OK, doda_insert_row(&t, v2));

    size_t cnt = 0;
    int target_age = 20;
    doda_select_where_eq(&t, "age", &target_age, cb_count, &cnt);
    DODA_ASSERT_EQ_INT(1, cnt);
}

DODA_TEST(test_delete_where_eq_and_reuse_slot) {
    const char *cols[] = {"id", "age"};
    DodaColumnType types[] = {COL_INT, COL_INT};
    DodaTable t;
    doda_init_table(&t, "people", 2, cols, types);

    for (int i = 0; i < 10; ++i) {
        int id=i, age=100+i;
        const void *vals[] = {&id, &age};
        DODA_ASSERT_EQ_INT(DodaStatus_OK, doda_insert_row(&t, vals));
    }

    size_t deleted = 0;
    int age = 105;
    DODA_ASSERT_EQ_INT(DodaStatus_OK, doda_delete_where_eq(&t, "age", &age, &deleted));
    DODA_ASSERT_EQ_INT(1, deleted);

    // Insert again; should succeed (slot reuse)
    int id=99, age99=999;
    const void *vals[] = {&id, &age99};
    DODA_ASSERT_EQ_INT(DodaStatus_OK, doda_insert_row(&t, vals));

    size_t cnt = 0;
    doda_select_where_eq(&t, "id", &id, cb_count, &cnt);
    DODA_ASSERT_EQ_INT(1, cnt);
}

DODA_TEST(test_full_table_returns_full) {
    const char *cols[] = {"id"};
    DodaColumnType types[] = {COL_INT};
    DodaTable t;
    doda_init_table(&t, "one", 1, cols, types);

    // Fill to MAX_ROWS
    for (int i = 0; i < (int)MAX_ROWS; ++i) {
        int id=i;
        const void *vals[] = {&id};
        DODA_ASSERT_EQ_INT(DodaStatus_OK, doda_insert_row(&t, vals));
    }

    int extra = 1234;
    const void *vals[] = {&extra};
    DODA_ASSERT_EQ_INT(DodaStatus_ERR_FULL, doda_insert_row(&t, vals));
}

DODA_TEST(test_primary_key_hash_lookup_eq) {
    const char *cols[] = {"id", "v"};
    DodaColumnType types[] = {COL_INT, COL_INT};
    DodaTable t;
    doda_init_table(&t, "pk", 2, cols, types);

    for (int i = 0; i < 100; ++i) {
        int id = 1000 + i;
        int v = i;
        const void *vals[] = {&id, &v};
        DODA_ASSERT_EQ_INT(DodaStatus_OK, doda_insert_row(&t, vals));
    }

    // Equality select on first int column should work
    size_t cnt = 0;
    int needle = 1025;
    doda_select_where_eq(&t, "id", &needle, cb_count, &cnt);
    DODA_ASSERT_EQ_INT(1, cnt);

    // Non-existent
    cnt = 0;
    int missing = 9999;
    doda_select_where_eq(&t, "id", &missing, cb_count, &cnt);
    DODA_ASSERT_EQ_INT(0, cnt);
}

DODA_TEST(test_invalid_column_name_select_no_crash) {
    const char *cols[] = {"id"};
    DodaColumnType types[] = {COL_INT};
    DodaTable t;
    doda_init_table(&t, "t", 1, cols, types);

    size_t cnt = 0;
    int v = 1;
    // Expect: no callback; function should handle missing column.
    doda_select_where_eq(&t, "does_not_exist", &v, cb_count, &cnt);
    DODA_ASSERT_EQ_INT(0, cnt);
}

DODA_TEST(test_fuzz_insert_delete_consistency) {
    const char *cols[] = {"id", "v"};
    DodaColumnType types[] = {COL_INT, COL_INT};
    DodaTable t;
    doda_init_table(&t, "fuzz", 2, cols, types);

    // Model presence by id in a small universe
    enum { UNIV = 128 };
    bool present[UNIV];
    int value[UNIV];
    memset(present, 0, sizeof(present));
    memset(value, 0, sizeof(value));

    uint32_t rng = 0xC0FFEEu;

    for (int step = 0; step < 2000; ++step) {
        uint32_t r = xorshift32(&rng);
        int id = (int)(r % UNIV);

        if ((r & 3u) != 0u) {
            // Insert (id,v)
            int v = (int)(xorshift32(&rng) & 0x7FFF);
            const void *vals[] = { &id, &v };
            DodaStatus st = doda_insert_row(&t, vals);
            if (st == DodaStatus_OK) {
                present[id] = true;
                value[id] = v;
            }
        } else {
            // Delete by id
            size_t deleted = 0;
            (void)doda_delete_where_eq(&t, "id", &id, &deleted);
            if (deleted) present[id] = false;
        }

        // Occasionally validate: scanning select result matches model
        if ((step % 200) == 0) {
            for (int probe = 0; probe < 8; ++probe) {
                int pid = (int)(xorshift32(&rng) % UNIV);
                uint16_t rows[MAX_ROWS + 1u];
                memset(rows, 0, sizeof(rows));
                doda_select_where_eq(&t, "id", &pid, cb_collect_row_ids, rows);
                if (present[pid]) {
                    DODA_ASSERT_MSG(rows[0] >= 1, "expected at least one row for present id");
                } else {
                    DODA_ASSERT_EQ_INT(0, rows[0]);
                }
            }
        }
    }
}

DODA_TEST(test_index_eq_matches_full_scan) {
    const char *cols[] = {"id", "v"};
    DodaColumnType types[] = {COL_INT, COL_INT};
    DodaTable t;
    doda_init_table(&t, "idx", 2, cols, types);

    // Insert many rows with repeated ids
    uint32_t rng = 0x12345678u;
    for (int i = 0; i < 200; ++i) {
        int id = (int)(xorshift32(&rng) % 64);
        int v = i;
        const void *vals[] = { &id, &v };
        DODA_ASSERT_EQ_INT(DodaStatus_OK, doda_insert_row(&t, vals));
    }

    DodaIndex idx;
    DODA_ASSERT(doda_index_build(&t, &idx, "id"));

    // Compare counts for a set of needles
    for (int needle = 0; needle < 64; needle += 7) {
        size_t scan_cnt = 0;
        size_t idx_cnt = 0;

        doda_select_where_eq(&t, "id", &needle, cb_count, &scan_cnt);
        DodaIndexStatus s = doda_index_select_eq(&t, &idx, &needle, cb_count, &idx_cnt);
        DODA_ASSERT_EQ_INT(DodaIndexStatus_OK, s);
        DODA_ASSERT_EQ_INT(scan_cnt, idx_cnt);
    }

    doda_index_drop(&idx);
}

DODA_TEST(test_index_range_gte_matches_full_scan) {
    const char *cols[] = {"id", "v"};
    DodaColumnType types[] = {COL_INT, COL_INT};
    DodaTable t;
    doda_init_table(&t, "idxr", 2, cols, types);

    for (int i = 0; i < 200; ++i) {
        int id = i % 50;
        int v = i;
        const void *vals[] = { &id, &v };
        DODA_ASSERT_EQ_INT(DodaStatus_OK, doda_insert_row(&t, vals));
    }

    DodaIndex idx;
    DODA_ASSERT(doda_index_build(&t, &idx, "id"));

    int needle = 25;
    size_t scan_cnt = 0;
    size_t idx_cnt = 0;

    doda_select_where_op(&t, "id", DodaOp_GTE, &needle, cb_count, &scan_cnt);
    DodaIndexStatus s = doda_index_select_op(&t, &idx, DodaOp_GTE, &needle, cb_count, &idx_cnt);
    DODA_ASSERT_EQ_INT(DodaIndexStatus_OK, s);
    DODA_ASSERT_EQ_INT(scan_cnt, idx_cnt);

    doda_index_drop(&idx);
}

void doda_register_core_tests(void) {
    DODA_REGISTER(test_insert_and_select_eq_int);
    DODA_REGISTER(test_delete_where_eq_and_reuse_slot);
    DODA_REGISTER(test_full_table_returns_full);
    DODA_REGISTER(test_primary_key_hash_lookup_eq);
    DODA_REGISTER(test_invalid_column_name_select_no_crash);
    DODA_REGISTER(test_fuzz_insert_delete_consistency);
    DODA_REGISTER(test_index_eq_matches_full_scan);
    DODA_REGISTER(test_index_range_gte_matches_full_scan);
}

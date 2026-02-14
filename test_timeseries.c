#include "test_framework.h"

#ifdef DRIVERSQL_TIMESERIES
#include "doda_api.h"
#include "doda_engine.h"

static void cb_count(const DodaTable *t, size_t row, void *user) {
    (void)t; (void)row;
    size_t *cnt = (size_t *)user;
    (*cnt)++;
}

DODA_TEST(test_ts_append_and_select_ge) {
    const char *cols[] = {"id", "time", "value"};
    DodaColumnType types[] = {COL_INT, COL_INT, COL_INT};
    DodaTable t;
    doda_init_table(&t, "metrics", 3, cols, types);

    DodaTSDB ts;
    doda_tsdb_init(&ts, &t, "time");

    doda_tsdb_append_int3(&ts, 1, 1000, 10);
    doda_tsdb_append_int3(&ts, 2, 1500, 20);
    doda_tsdb_append_int3(&ts, 3, 2000, 30);

    size_t cnt = 0;
    int t0 = 1500;
    doda_tsdb_select_time_ge(&ts, t0, (doda_row_callback)cb_count, &cnt);
    DODA_ASSERT_EQ_INT(2, cnt);
}

void doda_register_timeseries_tests(void) {
    DODA_REGISTER(test_ts_append_and_select_ge);
}

#else
// If timeseries is disabled, provide an empty registration function.
void doda_register_timeseries_tests(void) {}
#endif

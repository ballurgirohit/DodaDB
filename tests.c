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

int main(void) {
    // test_basic();
#ifdef DRIVERSQL_TIMESERIES
    // test_timeseries();
#endif
    test_aggregations();
    return 0;
}

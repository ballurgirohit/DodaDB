/*
 * Copyright (c) 2025 Rohit Ballurgi
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software... [rest of standard MIT short-text]
 * ...
 * MIT License (see LICENSE file for full text)
 */

#pragma once
#include "doda_engine.h"

#ifdef DRIVERSQL_TIMESERIES

// Timeseries convenience API built on core without changing core logic
// Assumes a schema with primary key 'id' (int) and a timestamp column 'time' (int)

typedef struct {
    DodaTable *table;
    const char *time_col; // e.g., "time"
} DodaTSDB;

void doda_tsdb_init(DodaTSDB *ts, DodaTable *t, const char *time_col);

// Append sample with monotonic time (optional check). Returns DodaStatus.
DodaStatus doda_tsdb_append_int3(DodaTSDB *ts, int id, int time, int value);

// Range query on time using core select_where_op; user callback handles rows.
DodaStatus doda_tsdb_select_time_ge(const DodaTSDB *ts, int t0, doda_row_callback cb, void *user);
DodaStatus doda_tsdb_select_time_gt(const DodaTSDB *ts, int t0, doda_row_callback cb, void *user);
DodaStatus doda_tsdb_select_time_lt(const DodaTSDB *ts, int t1, doda_row_callback cb, void *user);

// Build index on time column for efficient ranges
bool doda_tsdb_build_time_index(DodaTSDB *ts, DodaIndex *idx);

// Delete samples older than cutoff time
DodaStatus doda_tsdb_delete_older_than(DodaTSDB *ts, int cutoff_time, size_t *deleted_out);

// Aggregations over non-deleted rows for numeric columns
bool agg_min_int(const Table *t, const char *col_name, int *out);
bool agg_max_int(const Table *t, const char *col_name, int *out);
bool agg_avg_int(const Table *t, const char *col_name, double *out);
size_t agg_count(const Table *t);

#endif // DRIVERSQL_TIMESERIES

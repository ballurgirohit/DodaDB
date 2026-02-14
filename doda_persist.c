#include "doda_persist.h"
#include <string.h>

// Portable little-endian encoding helpers
static void wr_u32(uint8_t *p, uint32_t v) { p[0]=(uint8_t)v; p[1]=(uint8_t)(v>>8); p[2]=(uint8_t)(v>>16); p[3]=(uint8_t)(v>>24); }
static void wr_u16(uint8_t *p, uint16_t v) { p[0]=(uint8_t)v; p[1]=(uint8_t)(v>>8); }
static uint32_t rd_u32(const uint8_t *p) { return (uint32_t)p[0] | ((uint32_t)p[1]<<8) | ((uint32_t)p[2]<<16) | ((uint32_t)p[3]<<24); }
static uint16_t rd_u16(const uint8_t *p) { return (uint16_t)p[0] | ((uint16_t)p[1]<<8); }

// CRC32 (IEEE 802.3) for corruption detection.
#if DODA_PERSIST_HAS_CRC
static uint32_t crc32_update(uint32_t crc, const uint8_t *data, size_t len) {
    crc = ~crc;
    for (size_t i = 0; i < len; ++i) {
        crc ^= (uint32_t)data[i];
        for (int b = 0; b < 8; ++b) {
            uint32_t mask = -(crc & 1u);
            crc = (crc >> 1) ^ (0xEDB88320u & mask);
        }
    }
    return ~crc;
}
#endif

#define DODA_MAGIC 0x41444F44u /* 'DODA' */

typedef struct {
    uint32_t magic;
    uint16_t version;
    uint16_t header_bytes;
    uint16_t column_count;
    uint16_t row_count;      // count of non-deleted rows stored
    uint16_t max_rows;
    uint16_t max_cols;
    uint16_t max_name_len;
    uint16_t max_text_len;
    uint16_t hash_size;
    uint32_t payload_bytes;
#if DODA_PERSIST_HAS_CRC
    uint32_t payload_crc32; // CRC32 of schema+index+payload (everything after header)
#endif
} DodaPersistHeader;

static bool coltype_persistable(ColumnType ct) {
    if (ct == COL_INT || ct == COL_BOOL) return true;
#ifndef DRIVERSQL_NO_TEXT
    if (ct == COL_TEXT) return true;
#endif
#ifndef DRIVERSQL_NO_FLOAT
    if (ct == COL_FLOAT) return true;
#endif
#ifndef DRIVERSQL_NO_DOUBLE
    if (ct == COL_DOUBLE) return true;
#endif
#ifndef DRIVERSQL_NO_POINTER_COLUMN
    if (ct == COL_POINTER) return false;
#endif
    return false;
}

static size_t bytes_per_cell(ColumnType ct) {
    switch (ct) {
        case COL_INT: return sizeof(int32_t);
        case COL_BOOL: return sizeof(uint8_t);
#ifndef DRIVERSQL_NO_FLOAT
        case COL_FLOAT: return sizeof(float);
#endif
#ifndef DRIVERSQL_NO_DOUBLE
        case COL_DOUBLE: return sizeof(double);
#endif
#ifndef DRIVERSQL_NO_TEXT
        case COL_TEXT: return (size_t)MAX_TEXT_LEN;
#endif
        default: return 0;
    }
}

size_t doda_persist_estimate_max_bytes(const DodaTable *t) {
    if (!t) return 0;
    // header + schema (names+types) + row index list + full row payload
    size_t schema = (size_t)t->column_count * ((size_t)MAX_NAME_LEN + 1u);
    size_t per_row = 0;
    for (int c = 0; c < t->column_count; ++c) per_row += bytes_per_cell(t->columns[c].type);
    size_t max_rows = (size_t)MAX_ROWS;
    return sizeof(DodaPersistHeader) + schema + (max_rows * sizeof(uint16_t)) + (max_rows * per_row);
}

DodaPersistStatus doda_persist_save_table(const DodaTable *t, const DodaStorage *st) {
    if (!t || !st || !st->write_all) return DODA_PERSIST_ERR_INVALID;
    if (st->erase && !st->erase(st->ctx)) return DODA_PERSIST_ERR_IO;

    // Validate persistable schema
    for (int c = 0; c < t->column_count; ++c) {
        if (!coltype_persistable(t->columns[c].type)) return DODA_PERSIST_ERR_UNSUPPORTED;
    }

    // Count non-deleted rows
    uint16_t row_count = 0;
    for (size_t r = 0; r < t->count; ++r) if (!is_deleted(t, r)) row_count++;

    // Compute sizes
    size_t schema_bytes = (size_t)t->column_count * ((size_t)MAX_NAME_LEN + 1u);
    size_t per_row = 0;
    for (int c = 0; c < t->column_count; ++c) per_row += bytes_per_cell(t->columns[c].type);
    size_t index_bytes = (size_t)row_count * sizeof(uint16_t);
    size_t payload_bytes = schema_bytes + index_bytes + ((size_t)row_count * per_row);

#if DODA_PERSIST_HAS_CRC
    uint32_t crc = 0u;
#endif

    // First pass: schema
    for (int c = 0; c < t->column_count; ++c) {
        uint8_t sb[MAX_NAME_LEN + 1u];
        memset(sb, 0, sizeof(sb));
        memcpy(sb, t->columns[c].name, strnlen(t->columns[c].name, MAX_NAME_LEN));
        sb[MAX_NAME_LEN] = (uint8_t)t->columns[c].type;
#if DODA_PERSIST_HAS_CRC
        crc = crc32_update(crc, sb, sizeof(sb));
#endif
    }

    // First pass: index list
    for (size_t r = 0; r < t->count; ++r) {
        if (is_deleted(t, r)) continue;
        uint8_t ib[2]; wr_u16(ib, (uint16_t)r);
#if DODA_PERSIST_HAS_CRC
        crc = crc32_update(crc, ib, sizeof(ib));
#endif
    }

    // First pass: row payload
    for (size_t r = 0; r < t->count; ++r) {
        if (is_deleted(t, r)) continue;
        for (int c = 0; c < t->column_count; ++c) {
            const Column *col = &t->columns[c];
            switch (col->type) {
                case COL_INT: {
                    int32_t v = (int32_t)col->data.int_data[r];
                    uint8_t b[4]; wr_u32(b, (uint32_t)v);
#if DODA_PERSIST_HAS_CRC
                    crc = crc32_update(crc, b, sizeof(b));
#endif
                    break;
                }
                case COL_BOOL: {
                    uint8_t v = col->data.bool_data[r];
#if DODA_PERSIST_HAS_CRC
                    crc = crc32_update(crc, &v, 1);
#endif
                    break;
                }
#ifndef DRIVERSQL_NO_FLOAT
                case COL_FLOAT: {
#if DODA_PERSIST_HAS_CRC
                    crc = crc32_update(crc, (const uint8_t *)&col->data.float_data[r], sizeof(float));
#endif
                    break;
                }
#endif
#ifndef DRIVERSQL_NO_DOUBLE
                case COL_DOUBLE: {
#if DODA_PERSIST_HAS_CRC
                    crc = crc32_update(crc, (const uint8_t *)&col->data.double_data[r], sizeof(double));
#endif
                    break;
                }
#endif
#ifndef DRIVERSQL_NO_TEXT
                case COL_TEXT: {
                    char buf[MAX_TEXT_LEN];
                    memset(buf, 0, sizeof(buf));
                    memcpy(buf, col->data.text_data[r], strnlen(col->data.text_data[r], MAX_TEXT_LEN));
#if DODA_PERSIST_HAS_CRC
                    crc = crc32_update(crc, (const uint8_t *)buf, sizeof(buf));
#endif
                    break;
                }
#endif
                default:
                    return DODA_PERSIST_ERR_UNSUPPORTED;
            }
        }
    }

    DodaPersistHeader h;
    memset(&h, 0, sizeof(h));
    h.magic = DODA_MAGIC;
    h.version = (uint16_t)DODA_PERSIST_VERSION;
    h.header_bytes = (uint16_t)sizeof(DodaPersistHeader);
    h.column_count = (uint16_t)t->column_count;
    h.row_count = row_count;
    h.max_rows = (uint16_t)MAX_ROWS;
    h.max_cols = (uint16_t)MAX_COLUMNS;
    h.max_name_len = (uint16_t)MAX_NAME_LEN;
    h.max_text_len = (uint16_t)MAX_TEXT_LEN;
    h.hash_size = (uint16_t)HASH_SIZE;
    h.payload_bytes = (uint32_t)payload_bytes;
#if DODA_PERSIST_HAS_CRC
    h.payload_crc32 = crc;
#endif

    // Write header in a fixed endian layout
    uint8_t hb[sizeof(DodaPersistHeader)];
    memset(hb, 0, sizeof(hb));
    wr_u32(&hb[0], h.magic);
    wr_u16(&hb[4], h.version);
    wr_u16(&hb[6], h.header_bytes);
    wr_u16(&hb[8], h.column_count);
    wr_u16(&hb[10], h.row_count);
    wr_u16(&hb[12], h.max_rows);
    wr_u16(&hb[14], h.max_cols);
    wr_u16(&hb[16], h.max_name_len);
    wr_u16(&hb[18], h.max_text_len);
    wr_u16(&hb[20], h.hash_size);
    wr_u32(&hb[22], h.payload_bytes);
#if DODA_PERSIST_HAS_CRC
    wr_u32(&hb[26], h.payload_crc32);
#endif

    if (!st->write_all(st->ctx, hb, sizeof(hb))) return DODA_PERSIST_ERR_IO;

    // Schema block: for each column: name[MAX_NAME_LEN] + '\0' pad, then 1 byte type
    for (int c = 0; c < t->column_count; ++c) {
        uint8_t sb[MAX_NAME_LEN + 1u];
        memset(sb, 0, sizeof(sb));
        memcpy(sb, t->columns[c].name, strnlen(t->columns[c].name, MAX_NAME_LEN));
        sb[MAX_NAME_LEN] = (uint8_t)t->columns[c].type;
        if (!st->write_all(st->ctx, sb, sizeof(sb))) return DODA_PERSIST_ERR_IO;
    }

    // Row index list (uint16_t row ids in original table)
    for (size_t r = 0; r < t->count; ++r) {
        if (is_deleted(t, r)) continue;
        uint8_t ib[2]; wr_u16(ib, (uint16_t)r);
        if (!st->write_all(st->ctx, ib, sizeof(ib))) return DODA_PERSIST_ERR_IO;
    }

    // Row payload in column order
    for (size_t r = 0; r < t->count; ++r) {
        if (is_deleted(t, r)) continue;
        for (int c = 0; c < t->column_count; ++c) {
            const Column *col = &t->columns[c];
            switch (col->type) {
                case COL_INT: {
                    int32_t v = (int32_t)col->data.int_data[r];
                    uint8_t b[4]; wr_u32(b, (uint32_t)v);
                    if (!st->write_all(st->ctx, b, sizeof(b))) return DODA_PERSIST_ERR_IO;
                    break;
                }
                case COL_BOOL: {
                    uint8_t v = col->data.bool_data[r];
                    if (!st->write_all(st->ctx, &v, 1)) return DODA_PERSIST_ERR_IO;
                    break;
                }
#ifndef DRIVERSQL_NO_FLOAT
                case COL_FLOAT: {
                    float v = col->data.float_data[r];
                    if (!st->write_all(st->ctx, &v, sizeof(v))) return DODA_PERSIST_ERR_IO;
                    break;
                }
#endif
#ifndef DRIVERSQL_NO_DOUBLE
                case COL_DOUBLE: {
                    double v = col->data.double_data[r];
                    if (!st->write_all(st->ctx, &v, sizeof(v))) return DODA_PERSIST_ERR_IO;
                    break;
                }
#endif
#ifndef DRIVERSQL_NO_TEXT
                case COL_TEXT: {
                    char buf[MAX_TEXT_LEN];
                    memset(buf, 0, sizeof(buf));
                    memcpy(buf, col->data.text_data[r], strnlen(col->data.text_data[r], MAX_TEXT_LEN));
                    if (!st->write_all(st->ctx, buf, sizeof(buf))) return DODA_PERSIST_ERR_IO;
                    break;
                }
#endif
                default:
                    return DODA_PERSIST_ERR_UNSUPPORTED;
            }
        }
    }

    return DODA_PERSIST_OK;
}

DodaPersistStatus doda_persist_load_table(DodaTable *out, const DodaStorage *st) {
    if (!out || !st || !st->read_all) return DODA_PERSIST_ERR_INVALID;

    uint8_t hb[sizeof(DodaPersistHeader)];
    if (!st->read_all(st->ctx, hb, sizeof(hb))) return DODA_PERSIST_ERR_IO;

    DodaPersistHeader h;
    memset(&h, 0, sizeof(h));
    h.magic = rd_u32(&hb[0]);
    h.version = rd_u16(&hb[4]);
    h.header_bytes = rd_u16(&hb[6]);
    h.column_count = rd_u16(&hb[8]);
    h.row_count = rd_u16(&hb[10]);
    h.max_rows = rd_u16(&hb[12]);
    h.max_cols = rd_u16(&hb[14]);
    h.max_name_len = rd_u16(&hb[16]);
    h.max_text_len = rd_u16(&hb[18]);
    h.hash_size = rd_u16(&hb[20]);
    h.payload_bytes = rd_u32(&hb[22]);
#if DODA_PERSIST_HAS_CRC
    uint32_t expected_crc = rd_u32(&hb[26]);
#endif

    if (h.magic != DODA_MAGIC) return DODA_PERSIST_ERR_CORRUPT;
    if (h.version != (uint16_t)DODA_PERSIST_VERSION) return DODA_PERSIST_ERR_UNSUPPORTED;
    if (h.header_bytes != (uint16_t)sizeof(DodaPersistHeader)) return DODA_PERSIST_ERR_CORRUPT;
    if (h.max_rows != (uint16_t)MAX_ROWS || h.max_cols != (uint16_t)MAX_COLUMNS || h.max_name_len != (uint16_t)MAX_NAME_LEN || h.hash_size != (uint16_t)HASH_SIZE) return DODA_PERSIST_ERR_UNSUPPORTED;
#ifndef DRIVERSQL_NO_TEXT
    if (h.max_text_len != (uint16_t)MAX_TEXT_LEN) return DODA_PERSIST_ERR_UNSUPPORTED;
#endif
    if (h.column_count == 0 || h.column_count > (uint16_t)MAX_COLUMNS) return DODA_PERSIST_ERR_CORRUPT;

#if DODA_PERSIST_HAS_CRC
    uint32_t crc = 0u;
#endif

    // Read schema
    char name_bufs[MAX_COLUMNS][MAX_NAME_LEN];
    ColumnType types[MAX_COLUMNS];
    for (uint16_t c = 0; c < h.column_count; ++c) {
        uint8_t sb[MAX_NAME_LEN + 1u];
        if (!st->read_all(st->ctx, sb, sizeof(sb))) return DODA_PERSIST_ERR_IO;
        memcpy(name_bufs[c], sb, MAX_NAME_LEN);
        name_bufs[c][MAX_NAME_LEN - 1] = '\0';
        types[c] = (ColumnType)sb[MAX_NAME_LEN];
        if (!coltype_persistable(types[c])) return DODA_PERSIST_ERR_UNSUPPORTED;
#if DODA_PERSIST_HAS_CRC
        crc = crc32_update(crc, sb, sizeof(sb));
#endif
    }

    // Build pointers array for init_table
    const char *name_ptrs[MAX_COLUMNS];
    for (uint16_t c = 0; c < h.column_count; ++c) name_ptrs[c] = name_bufs[c];

    init_table(out, "loaded", (int)h.column_count, name_ptrs, types);

    // Read index list (we currently ignore original row ids; we compact on load)
    for (uint16_t i = 0; i < h.row_count; ++i) {
        uint8_t ib[2];
        if (!st->read_all(st->ctx, ib, sizeof(ib))) return DODA_PERSIST_ERR_IO;
        (void)rd_u16(ib);
#if DODA_PERSIST_HAS_CRC
        crc = crc32_update(crc, ib, sizeof(ib));
#endif
    }

    // Read row payload and insert into compacted table
    for (uint16_t i = 0; i < h.row_count; ++i) {
        const void *vals[MAX_COLUMNS];
        int32_t int_tmp[MAX_COLUMNS];
        uint8_t bool_tmp[MAX_COLUMNS];
#ifndef DRIVERSQL_NO_FLOAT
        float float_tmp[MAX_COLUMNS];
#endif
#ifndef DRIVERSQL_NO_DOUBLE
        double double_tmp[MAX_COLUMNS];
#endif
#ifndef DRIVERSQL_NO_TEXT
        char text_tmp[MAX_COLUMNS][MAX_TEXT_LEN];
#endif

        for (uint16_t c = 0; c < h.column_count; ++c) {
            switch (types[c]) {
                case COL_INT: {
                    uint8_t b[4]; if (!st->read_all(st->ctx, b, sizeof(b))) return DODA_PERSIST_ERR_IO;
                    int_tmp[c] = (int32_t)rd_u32(b);
                    vals[c] = &int_tmp[c];
#if DODA_PERSIST_HAS_CRC
                    crc = crc32_update(crc, b, sizeof(b));
#endif
                    break;
                }
                case COL_BOOL: {
                    uint8_t v; if (!st->read_all(st->ctx, &v, 1)) return DODA_PERSIST_ERR_IO;
                    bool_tmp[c] = v;
                    int_tmp[c] = (v != 0);
                    vals[c] = &int_tmp[c];
#if DODA_PERSIST_HAS_CRC
                    crc = crc32_update(crc, &v, 1);
#endif
                    break;
                }
#ifndef DRIVERSQL_NO_FLOAT
                case COL_FLOAT: {
                    if (!st->read_all(st->ctx, &float_tmp[c], sizeof(float_tmp[c]))) return DODA_PERSIST_ERR_IO;
                    vals[c] = &float_tmp[c];
#if DODA_PERSIST_HAS_CRC
                    crc = crc32_update(crc, (const uint8_t *)&float_tmp[c], sizeof(float_tmp[c]));
#endif
                    break;
                }
#endif
#ifndef DRIVERSQL_NO_DOUBLE
                case COL_DOUBLE: {
                    if (!st->read_all(st->ctx, &double_tmp[c], sizeof(double_tmp[c]))) return DODA_PERSIST_ERR_IO;
                    vals[c] = &double_tmp[c];
#if DODA_PERSIST_HAS_CRC
                    crc = crc32_update(crc, (const uint8_t *)&double_tmp[c], sizeof(double_tmp[c]));
#endif
                    break;
                }
#endif
#ifndef DRIVERSQL_NO_TEXT
                case COL_TEXT: {
                    if (!st->read_all(st->ctx, text_tmp[c], MAX_TEXT_LEN)) return DODA_PERSIST_ERR_IO;
                    text_tmp[c][MAX_TEXT_LEN - 1] = '\0';
                    vals[c] = text_tmp[c];
#if DODA_PERSIST_HAS_CRC
                    crc = crc32_update(crc, (const uint8_t *)text_tmp[c], MAX_TEXT_LEN);
#endif
                    break;
                }
#endif
                default:
                    return DODA_PERSIST_ERR_UNSUPPORTED;
            }
        }

        DSStatus s = insert_row(out, vals);
        if (s != DS_OK) return DODA_PERSIST_ERR_CORRUPT;
    }

#if DODA_PERSIST_HAS_CRC
    if (crc != expected_crc) return DODA_PERSIST_ERR_CORRUPT;
#endif

    return DODA_PERSIST_OK;
}

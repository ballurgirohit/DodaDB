#pragma once

#include <stdbool.h>
#include <stddef.h>

typedef void (*doda_test_fn)(void);

typedef struct {
    const char *name;
    doda_test_fn fn;
} doda_test_case;

void doda_test_register(const char *name, doda_test_fn fn);
int doda_test_run_all(void);

// Assertions
void doda_test_fail(const char *file, int line, const char *expr, const char *msg);

#define DODA_ASSERT(expr) do { \
    if (!(expr)) doda_test_fail(__FILE__, __LINE__, #expr, NULL); \
} while (0)

#define DODA_ASSERT_MSG(expr, msg) do { \
    if (!(expr)) doda_test_fail(__FILE__, __LINE__, #expr, (msg)); \
} while (0)

#define DODA_ASSERT_EQ_INT(a,b) do { \
    long _aa = (long)(a); long _bb = (long)(b); \
    if (_aa != _bb) doda_test_fail(__FILE__, __LINE__, #a " == " #b, "int mismatch"); \
} while (0)

#define DODA_ASSERT_NE_PTR(a,b) do { \
    const void *_aa = (const void*)(a); const void *_bb=(const void*)(b); \
    if (_aa == _bb) doda_test_fail(__FILE__, __LINE__, #a " != " #b, "ptr unexpectedly equal"); \
} while (0)

// Test definition helper
#define DODA_TEST(name) static void name(void)

// Register helper (call from a suite init function)
#define DODA_REGISTER(name) doda_test_register(#name, name)

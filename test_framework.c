#include "test_framework.h"

#include <stdio.h>
#include <stdlib.h>

#ifndef DODA_MAX_TESTS
#define DODA_MAX_TESTS 256
#endif

static doda_test_case g_tests[DODA_MAX_TESTS];
static size_t g_test_count = 0;

static const char *g_current_test = NULL;
static int g_failures = 0;

void doda_test_register(const char *name, doda_test_fn fn) {
    if (!name || !fn) return;
    if (g_test_count >= DODA_MAX_TESTS) return;
    g_tests[g_test_count++] = (doda_test_case){ name, fn };
}

void doda_test_fail(const char *file, int line, const char *expr, const char *msg) {
    ++g_failures;
    fprintf(stderr, "[FAIL] %s:%d: %s", file, line, expr ? expr : "(no expr)");
    if (g_current_test) fprintf(stderr, " (test=%s)", g_current_test);
    if (msg) fprintf(stderr, ": %s", msg);
    fprintf(stderr, "\n");
}

int doda_test_run_all(void) {
    printf("Running %zu tests...\n", g_test_count);
    for (size_t i = 0; i < g_test_count; ++i) {
        g_current_test = g_tests[i].name;
        int before = g_failures;
        g_tests[i].fn();
        if (g_failures == before) {
            printf("[OK]   %s\n", g_tests[i].name);
        } else {
            printf("[BAD]  %s\n", g_tests[i].name);
        }
    }

    if (g_failures) {
        printf("Failures: %d\n", g_failures);
        return 1;
    }
    printf("All tests passed.\n");
    return 0;
}

#include "test_framework.h"

// Each suite exposes a register function
void doda_register_core_tests(void);
void doda_register_timeseries_tests(void);
void doda_register_persist_tests(void);

int main(void) {
    doda_register_core_tests();
    doda_register_timeseries_tests();
    doda_register_persist_tests();
    return doda_test_run_all();
}

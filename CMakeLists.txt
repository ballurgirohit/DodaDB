cmake_minimum_required(VERSION 3.15)
project(DODA C)

set(CMAKE_C_STANDARD 99)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS OFF)

option(DRIVERSQL_FIRMWARE "Build firmware-only target (no tests)" ON)
option(DRIVERSQL_TIMESERIES "Enable timeseries helpers" ON)
option(DODA_PERSIST "Build portable persistence module" ON)

# Core library (no platform storage logic)
add_library(doda_core OBJECT
    doda_engine.c
    doda_engine.h
    doda_api.h
    doda_timeseries.c
)

target_include_directories(doda_core PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

if (CMAKE_C_COMPILER_ID MATCHES "Clang|AppleClang|GNU")
    target_compile_options(doda_core PRIVATE -Wall -Wextra -Wpedantic)
endif()

if (DRIVERSQL_FIRMWARE)
    target_compile_definitions(doda_core PRIVATE DRIVERSQL_NO_STDIO DRIVERSQL_NO_POINTER_COLUMN)
endif()

if (DRIVERSQL_TIMESERIES)
    target_compile_definitions(doda_core PRIVATE DRIVERSQL_TIMESERIES)
endif()

# Optional persistence module (portable serializer; storage backend provided by app)
if (DODA_PERSIST)
    add_library(doda_persist OBJECT
        doda_persist.c
        doda_persist.h
    )
    target_include_directories(doda_persist PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
    if (CMAKE_C_COMPILER_ID MATCHES "Clang|AppleClang|GNU")
        target_compile_options(doda_persist PRIVATE -Wall -Wextra -Wpedantic)
    endif()
endif()

# Flash backend template is provided as source files for developers to copy.
# It is not compiled by default.
option(DODA_BUILD_FLASH_STUB "Build example flash storage stub (template)" OFF)

if (DODA_BUILD_FLASH_STUB)
    add_library(doda_flash_stub OBJECT
        doda_storage_flash_stub.c
        doda_storage_flash_stub.h
    )
    target_include_directories(doda_flash_stub PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
endif()

if (NOT DRIVERSQL_FIRMWARE)
    add_executable(doda
        tests.c
        $<TARGET_OBJECTS:doda_core>
        $<$<BOOL:${DODA_PERSIST}>:$<TARGET_OBJECTS:doda_persist>>
        $<$<BOOL:${DODA_BUILD_FLASH_STUB}>:$<TARGET_OBJECTS:doda_flash_stub>>
    )
    target_include_directories(doda PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

    if (DRIVERSQL_TIMESERIES)
        target_compile_definitions(doda PRIVATE DRIVERSQL_TIMESERIES)
    endif()

    if (CMAKE_C_COMPILER_ID MATCHES "Clang|AppleClang|GNU")
        target_compile_options(doda PRIVATE -Wall -Wextra -Wpedantic)
    endif()
endif()

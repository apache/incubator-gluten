macro(add_gtest_sources prefix common_path)
    add_glob(${prefix}_sources ${common_path}/gtest*.cpp)
endmacro()

set(USE_INTERNAL_GTEST_LIBRARY 0)
set(BENCHMARK_ENABLE_TESTING OFF)

enable_testing()
include(CTest)

include_directories(${GTEST_INCLUDE_DIRS})

set(TEST_DATA_DIR "${ClickHouse_SOURCE_DIR}/utils/extern-local-engine/tests")

configure_file(
        ${CMAKE_CURRENT_SOURCE_DIR}/testConfig.h.in
        ${CMAKE_CURRENT_SOURCE_DIR}/testConfig.h
)
set(HAVE_POSIX_REGEX 1)

set(LOCAL_ENGINE_DIR "${ClickHouse_SOURCE_DIR}/utils/extern-local-engine")

add_gtest_sources(local_engine_gtest .)
if (ENABLE_LOCAL_UDFS)
    file(GLOB children CONFIGURE_DEPENDS RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} ../Parser/*_udf/tests)
    foreach(child ${children})
        add_gtest_sources(local_engine_gtest ${child})
    endforeach()

    file(GLOB children CONFIGURE_DEPENDS RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} ../Parser/*_udf)
    foreach(child ${children})
        add_headers_and_sources(local_udfs_test ${child})
    endforeach()
endif ()

message("gtest_sources ${local_engine_gtest_sources}")
message("local UDFs: ${local_udfs_test_sources}")

add_executable(unit_tests_local_engine ${local_engine_gtest_sources} ${local_udfs_test_sources})

add_executable(benchmark_local_engine benchmark_local_engine.cpp benchmark_parquet_read.cpp benchmark_spark_row.cpp)

target_include_directories(unit_tests_local_engine PRIVATE
    ${GTEST_INCLUDE_DIRS}/include
    ${ClickHouse_SOURCE_DIR}/utils/extern-local_engine
)

include_directories(benchmark_local_engine SYSTEM PUBLIC
    ${ClickHouse_SOURCE_DIR}/utils/extern-local_engine
)

target_link_libraries(unit_tests_local_engine PRIVATE gluten_clickhouse_backend_libs _gtest_all clickhouse_parsers loggers)
target_link_libraries(benchmark_local_engine PRIVATE gluten_clickhouse_backend_libs ch_contrib::gbenchmark_all loggers)
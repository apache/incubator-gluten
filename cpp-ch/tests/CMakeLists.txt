macro (grep_gtest_sources BASE_DIR DST_VAR)
    # Cold match files that are not in tests/ directories
    file(GLOB_RECURSE "${DST_VAR}" RELATIVE "${BASE_DIR}" "gtest*.cpp")
endmacro()

set(USE_INTERNAL_GTEST_LIBRARY 0)
set(BENCHMARK_ENABLE_TESTING OFF)
set(GTEST_INCLUDE_DIRS ${CH_SOURCE_DIR}/contrib/googletest/googletest/include)

enable_testing()
include(CTest)


set(TEST_DATA_DIR "${CMAKE_SOURCE_DIR}/tests/data")

configure_file(
        ${CMAKE_SOURCE_DIR}/tests/testConfig.h.in
        ${CMAKE_SOURCE_DIR}/tests/testConfig.h
)
#set(HAVE_POSIX_REGEX 1)
#include(FetchContent)
#FetchContent_Declare(googlebenchmark GIT_REPOSITORY https://github.com/google/benchmark GIT_TAG main)
#FetchContent_MakeAvailable(googlebenchmark)
#include_directories(
#        ${builder_headers}
#        ${parser_headers}
#)
#
#target_compile_options(benchmark PUBLIC
#        -Wno-extra-semi-stmt
#        -Wno-format-nonliteral
#        -Wno-missing-noreturn
#        -Wno-old-style-cast
#        -Wno-undef
#        -Wno-used-but-marked-unused
#        -Wno-zero-as-null-pointer-constant
#        )

macro (grep_gtest_sources BASE_DIR DST_VAR)
    # Cold match files that are not in tests/ directories
    file(GLOB_RECURSE "${DST_VAR}" CONFIGURE_DEPENDS RELATIVE "${BASE_DIR}" "gtest*.cpp")
endmacro()

grep_gtest_sources("${CMAKE_SOURCE_DIR}/tests" local_engine_gtest_sources)

add_executable(unit_tests_local_engine ${local_engine_gtest_sources})

target_include_directories(unit_tests_local_engine
        PRIVATE
        ${GTEST_INCLUDE_DIRS}
        )

LIST(APPEND GTEST_LIB
        ${CH_BINARY_DIR}/contrib/googletest-cmake/lib_gtest.a
#        ${CH_BINARY_DIR}/contrib/googletest-cmake/lib_gtest_main.a
        )

target_compile_options(unit_tests_local_engine PUBLIC )
target_link_libraries(unit_tests_local_engine PUBLIC
        ${GTEST_LIB}
        ch
        ${THIRD_PARTY_LIBS}
        substrait
        ch_parquet
        substait_source
        )


#add_executable(benchmark_local_engine benchmark_local_engine.cpp benchmark_parquet_read.cpp benchmark_spark_row.cpp)

#include_directories(benchmark_local_engine SYSTEM PUBLIC ${FETCH_CONTENT_SOURCE_DIR_GOOGLEBENCHMARK}/include ${ClickHouse_SOURCE_DIR}/utils/local_engine)

#target_link_libraries(benchmark_local_engine PRIVATE ${LOCALENGINE_SHARED_LIB} benchmark::benchmark)

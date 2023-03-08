cmake_minimum_required(VERSION 3.20)
set(CH_SOURCE_DIR ${CMAKE_SOURCE_DIR}/ClickHouse CACHE STRING "ClickHouse source dir")

project(libch LANGUAGES C CXX ASM)
file(GLOB clickhouse_files "${CH_SOURCE_DIR}/*")
if ("${CH_SOURCE_DIR}" STREQUAL "${CMAKE_SOURCE_DIR}/ClickHouse")
    if ("${CH_SOURCE_DIR}" STREQUAL "${CMAKE_SOURCE_DIR}/ClickHouse")
        if (NOT clickhouse_files)
            execute_process(COMMAND git clone --recursive --depth 1 https://github.com/Kyligence/ClickHouse.git ${CH_SOURCE_DIR}
                            OUTPUT_VARIABLE download_ch)
        endif()
    endif()
else()
    if (NOT clickhouse_files)
        # Checking out *all* submodules takes > 5 min. Therefore, the smoke build ("FastTest") in CI initializes only the set of
        # submodules minimally needed for a build and we cannot assume here that all submodules are populated.
        message(ERROR "clickhouse ${CH_SOURCE_DIR} is missing or empty. to fix try run:")
        message(STATUS "    git clone --recursive --depth 1 https://github.com/Kyligence/ClickHouse.git ${CMAKE_SOURCE_DIR}")
    endif()
endif()

set(CH_BINARY_DIR "${CH_SOURCE_DIR}/build")

set (CMAKE_CXX_STANDARD 20)
set (CMAKE_CXX_EXTENSIONS ON) # Same as gnu++2a (ON) vs c++2a (OFF): https://cmake.org/cmake/help/latest/prop_tgt/CXX_EXTENSIONS.html
set (CMAKE_CXX_STANDARD_REQUIRED ON)

set (CMAKE_C_STANDARD 11)
set (CMAKE_C_EXTENSIONS ON)
set (CMAKE_C_STANDARD_REQUIRED ON)

set(ENABLE_THINLTO ON)
if (ENABLE_TESTS)
    set(ENABLE_THINLTO OFF)
endif()

include(${CH_SOURCE_DIR}/cmake/add_warning.cmake)
include(${CH_SOURCE_DIR}/cmake/warnings.cmake)
include(cmake/toolchain-x86_64.cmake)

add_custom_command(
        USES_TERMINAL
        COMMAND
        bash -c
        \"cmake -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
        -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
        -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
        -DENABLE_LIBPQXX=OFF
        -DENABLE_LDAP=OFF
        -DENABLE_BROTLI=OFF
        -DENABLE_BZIP2=OFF
        -DENABLE_CAPNP=OFF
        -DENABLE_S2_GEOMETRY=OFF
        -DENABLE_H3=OFF
        -DENABLE_NLP=OFF
        -DENABLE_CASSANDRA=OFF
        -DENABLE_KAFKA=OFF
        -DENABLE_ODBC=OFF
        -DENABLE_MYSQL=OFF
        -DENABLE_DATASKETCHES=OFF
        -DENABLE_SQLITE=OFF
        -DENABLE_GRPC=OFF
        -DENABLE_HIVE=OFF
        -DENABLE_PROTOBUF=ON
        -DENABLE_EMBEDDED_COMPILER=ON
        -DENABLE_TESTS=OFF
        -DENABLE_JEMALLOC=ON
        -DENABLE_MULTITARGET_CODE=ON
        -DENABLE_CLICKHOUSE_ALL=OFF
        -DENABLE_CLICKHOUSE_ODBC_BRIDGE=OFF
        -DENABLE_RUST=OFF
        -DENABLE_TESTS=${ENABLE_TESTS}
        -DENABLE_THINLTO=${ENABLE_THINLTO}
        -S ${CH_SOURCE_DIR} -G Ninja -B ${CH_BINARY_DIR} &&
        cmake --build ${CH_BINARY_DIR} --target build_clickhouse_dep\"
        OUTPUT _build_ch

)

add_custom_target(build_ch ALL DEPENDS _build_ch)


include(cmake/default-libs.cmake)
link_libraries(global-group)

# Find java/jni
include(FindJava)
include(UseJava)
include(FindJNI)

file(GLOB THIRD_PARTY ${CH_BINARY_DIR}/utils/clickhouse-dep/deps/*.a)
file(GLOB THIRD_PARTY_OBJECTS ${CH_BINARY_DIR}/utils/clickhouse-dep/objects/*.o)
list(APPEND THIRD_PARTY_LIBS ${THIRD_PARTY})
add_subdirectory(src/proto)

macro(add_glob cur_list)
    file(GLOB __tmp CONFIGURE_DEPENDS RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} ${ARGN})
    list(APPEND ${cur_list} ${__tmp})
endmacro()

macro(add_headers_and_sources prefix common_path)
    add_glob(${prefix}_headers ${CMAKE_CURRENT_SOURCE_DIR} ${common_path}/*.h)
    add_glob(${prefix}_sources ${common_path}/*.cpp ${common_path}/*.c ${common_path}/*.h)
endmacro()

add_headers_and_sources(builder src/Builder)
add_headers_and_sources(parser src/Parser)
add_headers_and_sources(storages src/Storages)
add_headers_and_sources(common src/Common)
add_headers_and_sources(shuffle src/Shuffle)
add_headers_and_sources(operator src/Operator)
add_headers_and_sources(jni src/jni)
add_headers_and_sources(aggregate src/AggregateFunctions)
add_headers_and_sources(functions src/Functions)
LIST(APPEND LIBCH_SOURCES
        ${builder_sources}
        ${parser_sources}
        ${storages_sources}
        ${common_sources}
        ${shuffle_sources}
        ${jni_sources}
        ${substrait_source}
        ${operator_sources}
        ${aggregate_sources}
        ${functions_sources}
        ${CMAKE_CURRENT_SOURCE_DIR}/src/local_engine_jni.cpp)

include_directories(
        ${JNI_INCLUDE_DIRS}
        ${CH_SOURCE_DIR}/contrib/thrift/lib/cpp/src
        ${CH_BINARY_DIR}/contrib/thrift-cmake
        ${CMAKE_CURRENT_SOURCE_DIR}/src
        ${CH_SOURCE_DIR}/contrib/orc/c++/include
        ${CH_SOURCE_DIR}/contrib/boost
        ${CH_SOURCE_DIR}/contrib/poco/Foundation/include
        ${CH_SOURCE_DIR}/contrib/fmtlib/include
        ${CH_SOURCE_DIR}/contrib/cityhash102/include
        ${CH_SOURCE_DIR}/contrib/poco/JSON/include
        ${CH_SOURCE_DIR}/contrib/poco/Net/include
        ${CH_SOURCE_DIR}/contrib/poco/Util/include
        ${CH_SOURCE_DIR}/contrib/poco/XML/include
        ${CH_SOURCE_DIR}/contrib/magic_enum/include
        ${CH_SOURCE_DIR}/contrib/pdqsort
        ${CH_SOURCE_DIR}/contrib/double-conversion
        ${CH_SOURCE_DIR}/contrib/miniselect/include
        ${CH_SOURCE_DIR}/contrib/dragonbox/include
        ${CH_SOURCE_DIR}/contrib/abseil-cpp
        ${CH_SOURCE_DIR}/contrib/protobuf/src
        ${CH_SOURCE_DIR}/contrib/llvm-project/llvm/include
        ${CH_BINARY_DIR}/contrib/llvm-project/llvm/include
        ${CH_SOURCE_DIR}/contrib/re2
        ${CH_BINARY_DIR}/contrib/re2-cmake
        ${CH_SOURCE_DIR}/contrib/croaring/include
        ${CH_SOURCE_DIR}/contrib/croaring/cpp
        ${CH_SOURCE_DIR}/contrib/libhdfs3/include
        ${CH_SOURCE_DIR}/contrib/aws/aws-cpp-sdk-core/include
        ${CH_SOURCE_DIR}/contrib/aws/aws-cpp-sdk-s3/include
        ${CH_SOURCE_DIR}/contrib/aws-crt-cpp/include
        ${CH_SOURCE_DIR}/contrib/aws-c-common/include
        ${CH_SOURCE_DIR}/contrib/aws-c-io/include
        ${CH_SOURCE_DIR}/contrib/aws-c-mqtt/include
        ${CH_SOURCE_DIR}/contrib/aws-c-auth/include
        ${CH_SOURCE_DIR}/contrib/aws-c-sdkutils/include
        ${CH_BINARY_DIR}/contrib/aws-cmake/include
        ${CH_SOURCE_DIR}/contrib/arrow/cpp/src
        ${CH_SOURCE_DIR}/contrib/arrow-cmake/cpp/src
        ${CH_BINARY_DIR}/includes/configs
        ${CH_BINARY_DIR}/contrib/orc/c++/include
        ${CH_SOURCE_DIR}/contrib/azure/sdk/storage/azure-storage-blobs/inc
        ${CH_SOURCE_DIR}/contrib/azure/sdk/core/azure-core/inc
        ${CH_SOURCE_DIR}/contrib/azure/sdk/storage/azure-storage-common/inc
        ${CH_SOURCE_DIR}/contrib/libfarmhash
        ${CH_SOURCE_DIR}/contrib/libmetrohash/src
        ${CH_SOURCE_DIR}/contrib/wyhash
        ${CH_SOURCE_DIR}/contrib/murmurhash/include
        ${CH_SOURCE_DIR}/contrib/xxHash
        ${CH_SOURCE_DIR}/contrib/boringssl/include
        ${CH_SOURCE_DIR}/src
        ${CH_SOURCE_DIR}/base
        ${CH_SOURCE_DIR}/base/pcg-random

)

add_subdirectory(src/Storages/ch_parquet)
add_subdirectory(src/Storages/SubstraitSource)

add_library(ch SHARED
        ${LIBCH_SOURCES}
)

target_compile_definitions(ch
        PUBLIC
        POCO_ENABLE_CPP11
        POCO_OS_FAMILY_UNIX
        XXH_INLINE_ALL
        )

target_compile_options(ch
        PUBLIC
        -fPIC
        -Wno-shorten-64-to-32
        -Wno-implicit-const-int-float-conversion
        -Wno-sign-compare
        -Wno-unused-but-set-variable
        -Wno-deprecated-builtins
        -Wno-macro-redefined
        -Wno-unused-but-set-variable
        -Wno-unneeded-internal-declaration
        -Wno-unknown-warning-option
        )

# dbms.a include this file
list(REMOVE_ITEM THIRD_PARTY_OBJECTS ${CH_BINARY_DIR}/utils/clickhouse-dep/objects/indexHint.cpp.o)

target_link_libraries(ch
        PUBLIC
        ${THIRD_PARTY_OBJECTS}
        -Wl,--whole-archive
        ${THIRD_PARTY_LIBS}
        -Wl,--no-whole-archive
        substrait
        ch_parquet
        substait_source
        )

if (ENABLE_TESTS)
    add_subdirectory(tests)
endif ()


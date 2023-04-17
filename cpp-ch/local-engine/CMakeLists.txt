set(THRIFT_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/thrift/lib/cpp/src")

# Find java/jni
include(FindJava)
include(UseJava)
include(FindJNI)

#set(JNI_NATIVE_SOURCES local_engine_jni.cpp)
set(LOCALENGINE_SHARED_LIB ch)
set (ENABLE_CURL_BUILD OFF)

add_subdirectory(proto)

add_headers_and_sources(builder Builder)
add_headers_and_sources(parser Parser)
add_headers_and_sources(storages Storages)
add_headers_and_sources(common Common)
add_headers_and_sources(external External)
add_headers_and_sources(shuffle Shuffle)
add_headers_and_sources(operator Operator)
add_headers_and_sources(jni jni)
add_headers_and_sources(aggregate_functions AggregateFunctions)
add_headers_and_sources(functions Functions)

include_directories(
        ${JNI_INCLUDE_DIRS}
        ${CMAKE_CURRENT_BINARY_DIR}/proto
        ${THRIFT_INCLUDE_DIR}
        ${CMAKE_BINARY_DIR}/contrib/thrift-cmake
        ${ClickHouse_SOURCE_DIR}/utils/local-engine
        ${ClickHouse_SOURCE_DIR}/src
        ${ClickHouse_SOURCE_DIR}/base
        ${ClickHouse_SOURCE_DIR}/contrib/orc/c++/include
        ${CMAKE_BINARY_DIR}/contrib/orc/c++/include
        ${ClickHouse_SOURCE_DIR}/contrib/azure/sdk/storage/azure-storage-blobs/inc
        ${ClickHouse_SOURCE_DIR}/contrib/azure/sdk/core/azure-core/inc
        ${ClickHouse_SOURCE_DIR}/contrib/azure/sdk/storage/azure-storage-common/inc
)

add_subdirectory(Storages/ch_parquet)
add_subdirectory(Storages/SubstraitSource)

add_library(${LOCALENGINE_SHARED_LIB} SHARED
        ${builder_sources}
        ${parser_sources}
        ${storages_sources}
        ${common_sources}
        ${external_sources}
        ${shuffle_sources}
        ${jni_sources}
        ${substrait_source}
        ${operator_sources}
        ${aggregate_functions_sources}
        ${functions_sources}
        local_engine_jni.cpp)


target_compile_options(${LOCALENGINE_SHARED_LIB} PUBLIC -fPIC
        -Wno-shorten-64-to-32)

target_link_libraries(${LOCALENGINE_SHARED_LIB} PUBLIC
        clickhouse_aggregate_functions
        clickhouse_common_config
        clickhouse_common_io
        clickhouse_functions
        clickhouse_parsers
        clickhouse_storages_system
        substrait
        loggers
        ch_parquet
        substait_source
        xxHash
)

#set(CPACK_PACKAGE_VERSION 0.1.0)
#set(CPACK_GENERATOR "RPM")
#set(CPACK_PACKAGE_NAME "local_engine_jni")
#set(CPACK_PACKAGE_RELEASE 1)
#set(CPACK_CMAKE_GENERATOR Ninja)
#set(CPACK_PACKAGE_CONTACT "neng.liu@kyligence.io")
#set(CPACK_PACKAGE_VENDOR "Kyligence")
#set(CPACK_PACKAGING_INSTALL_PREFIX ${CMAKE_INSTALL_PREFIX})
#set(CPACK_RPM_PACKAGE_AUTOREQPROV "no")
#set(CPACK_PACKAGE_FILE_NAME "${CPACK_PACKAGE_NAME}-${CPACK_PACKAGE_VERSION}-${CPACK_PACKAGE_RELEASE}.${CMAKE_SYSTEM_PROCESSOR}")
#include(CPack)

if (ENABLE_TESTS)
    add_subdirectory(tests)
endif ()


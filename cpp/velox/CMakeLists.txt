# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

cmake_minimum_required(VERSION 3.16)

project(velox)

include(ExternalProject)
include(FindPkgConfig)
include(GNUInstallDirs)
include(CheckCXXCompilerFlag)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mavx")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-deprecated-declarations -Wno-attributes")

set(SYSTEM_LIB_PATH "/usr/lib" CACHE PATH "System Lib dir")
set(SYSTEM_LIB64_PATH "/usr/lib64" CACHE PATH "System Lib64 dir")
set(SYSTEM_LIB_MULTIARCH_PATH "/usr/lib/x86_64-linux-gnu" CACHE PATH "System Lib MultiArch dir")
set(SYSTEM_LOCAL_LIB_PATH "/usr/local/lib" CACHE PATH "System Local Lib dir")
set(SYSTEM_LOCAL_LIB64_PATH "/usr/local/lib64" CACHE PATH "System Local Lib64 dir")

set(VELOX_REALEASE_PATH "${VELOX_HOME}/_build/release")
set(VELOX_BUILD_PATH "${VELOX_REALEASE_PATH}/velox")
set(VELOX_EXEC_LIB_PATH "${VELOX_BUILD_PATH}/exec/libvelox_exec.a")
set(VELOX_CONNECTOR_LIB_PATH "${VELOX_BUILD_PATH}/connectors/libvelox_connector.a")
set(VELOX_HIVE_PART_LIB_PATH "${VELOX_BUILD_PATH}/connectors/hive/libvelox_hive_partition_function.a")
set(VELOX_HIVE_CONNECTOR_LIB_PATH "${VELOX_BUILD_PATH}/connectors/hive/libvelox_hive_connector.a")
set(VELOX_DWIO_COMMON_LIB_PATH "${VELOX_BUILD_PATH}/dwio/common/libvelox_dwio_common.a")
set(VELOX_DWIO_DWRF_PROTO_LIB_PATH "${VELOX_BUILD_PATH}/dwio/dwrf/proto/libvelox_dwio_dwrf_proto.a")
set(VELOX_CORE_LIB_PATH "${VELOX_BUILD_PATH}/core/libvelox_core.a")
set(VELOX_VECTOR_LIB_PATH "${VELOX_BUILD_PATH}/vector/libvelox_vector.a")
set(VELOX_CORE_CONFIG_LIB_PATH "${VELOX_BUILD_PATH}/core/libvelox_config.a")
set(VELOX_TYPE_LIB_PATH "${VELOX_BUILD_PATH}/type/libvelox_type.a")
set(VELOX_COMMON_BASE_LIB_PATH "${VELOX_BUILD_PATH}/common/base/libvelox_common_base.a")
set(VELOX_COMMON_BASE_EXCEPTION_LIB_PATH "${VELOX_BUILD_PATH}/common/base/libvelox_exception.a")
set(VELOX_DWIO_COMMON_EXCEPTION_LIB_PATH "${VELOX_BUILD_PATH}/dwio/common/exception/libvelox_dwio_common_exception.a")
set(VELOX_EXEC_TEST_UTIL_LIB_PATH "${VELOX_BUILD_PATH}/exec/tests/utils/libvelox_exec_test_util.a")
set(VELOX_PARSE_EXP_LIB_PATH "${VELOX_BUILD_PATH}/parse/libvelox_parse_expression.a")
set(VELOX_PARSE_PARSER_LIB_PATH "${VELOX_BUILD_PATH}/parse/libvelox_parse_parser.a")
set(VELOX_COMMON_MEM_LIB_PATH "${VELOX_BUILD_PATH}/common/memory/libvelox_memory.a")
set(VELOX_COMMON_TIME_LIB_PATH "${VELOX_BUILD_PATH}/common/time/libvelox_time.a")
set(VELOX_COMMON_PROCESS_LIB_PATH "${VELOX_BUILD_PATH}/common/process/libvelox_process.a")
set(VELOX_DWIO_DWRF_WRITER_LIB_PATH "${VELOX_BUILD_PATH}/dwio/dwrf/writer/libvelox_dwio_dwrf_writer.a")
set(VELOX_DWIO_DWRF_COMMON_LIB_PATH "${VELOX_BUILD_PATH}/dwio/dwrf/common/libvelox_dwio_dwrf_common.a")
set(VELOX_COMMON_CACHE_LIB_PATH "${VELOX_BUILD_PATH}/common/caching/libvelox_caching.a")
set(VELOX_COMMON_FILE_LIB_PATH "${VELOX_BUILD_PATH}/common/file/libvelox_file.a")
set(VELOX_DWIO_DWRF_READER_LIB_PATH "${VELOX_BUILD_PATH}/dwio/dwrf/reader/libvelox_dwio_dwrf_reader.a")
set(VELOX_EXTERNAL_DUCKDB_LIB_PATH "${VELOX_BUILD_PATH}/external/duckdb/libduckdb.a")
set(VELOX_DUCKDB_PARSER_LIB_PATH "${VELOX_BUILD_PATH}/duckdb/conversion/libvelox_duckdb_parser.a")
set(VELOX_DUCKDB_CONVERSION_LIB_PATH "${VELOX_BUILD_PATH}/duckdb/conversion/libvelox_duckdb_conversion.a")
set(VELOX_FUNCTIONS_PRESTOSQL_TYPES_LIB_PATH "${VELOX_BUILD_PATH}/functions/prestosql/types/libvelox_presto_types.a")
set(VELOX_FUNCTIONS_PRESTOSQL_LIB_PATH "${VELOX_BUILD_PATH}/functions/prestosql/registration/libvelox_functions_prestosql.a")
set(VELOX_FUNCTIONS_PRESTOSQL_IMPL_LIB_PATH "${VELOX_BUILD_PATH}/functions/prestosql/libvelox_functions_prestosql_impl.a")
set(VELOX_FUNCTIONS_PRESTOSQL_JSON_LIB_PATH "${VELOX_BUILD_PATH}/functions/prestosql/json/libvelox_functions_json.a")
set(VELOX_FUNCTIONS_PRESTOSQL_HYPERLOGLOG_LIB_PATH "${VELOX_BUILD_PATH}/functions/prestosql/hyperloglog/libvelox_functions_hyperloglog.a")
set(VELOX_FUNCTIONS_PRESTOSQL_AGG_LIB_PATH "${VELOX_BUILD_PATH}/functions/prestosql/aggregates/libvelox_aggregates.a")
set(VELOX_FUNCTIONS_LIB_PATH "${VELOX_BUILD_PATH}/functions/lib/libvelox_functions_lib.a")
set(VELOX_TYPE_TZ_LIB_PATH "${VELOX_BUILD_PATH}/type/tz/libvelox_type_tz.a")
set(VELOX_EXTERNAL_MD5_LIB_PATH "${VELOX_BUILD_PATH}/external/md5/libmd5.a")
set(VELOX_EXPRESSION_LIB_PATH "${VELOX_BUILD_PATH}/expression/libvelox_expression.a")
set(VELOX_EXPRESSION_FUNCTIONS_LIB_PATH "${VELOX_BUILD_PATH}/expression/libvelox_expression_functions.a")
set(VELOX_BUFFER_LIB_PATH "${VELOX_BUILD_PATH}/buffer/libvelox_buffer.a")
set(VELOX_ARROW_BRIDGE_LIB_PATH "${VELOX_BUILD_PATH}/vector/arrow/c/libvelox_arrow_bridge.a")
set(VELOX_SUBSTRAIT_LIB_PATH "${VELOX_BUILD_PATH}/substrait/libvelox_substrait_plan_converter.a")

# Find Folly
macro(find_folly)
  # Find the folly library
  set(CMAKE_FIND_LIBRARY_SUFFIXES ".a")
  find_library(FOLLY_LIBRARY NAMES folly PATHS ${SYSTEM_LIB_PATH} ${SYSTEM_LIB64_PATH} ${SYSTEM_LIB_MULTIARCH_PATH} ${SYSTEM_LOCAL_LIB_PATH} ${SYSTEM_LOCAL_LIB64_PATH} NO_DEFAULT_PATH)
  if (NOT FOLLY_LIBRARY)
    message(FATAL_ERROR "Folly Library Not Found")
  else()
    message(STATUS "Folly Library Can Be Found in ${FOLLY_LIBRARY}")
  endif()
  set(FOLLY_LIB_PATH "${FOLLY_LIBRARY}")
endmacro()

# Find Iberty
macro(find_iberty)
  # Find the iberty library
  set(CMAKE_FIND_LIBRARY_SUFFIXES ".a")
  find_library(IBERTY_LIBRARY NAMES iberty PATHS ${SYSTEM_LIB_PATH} ${SYSTEM_LIB64_PATH} ${SYSTEM_LIB_MULTIARCH_PATH} ${SYSTEM_LOCAL_LIB_PATH} ${SYSTEM_LOCAL_LIB64_PATH} NO_DEFAULT_PATH)
  if (NOT IBERTY_LIBRARY)
    message(FATAL_ERROR "Iberty Library Not Found")
  else()
    message(STATUS "Iberty Library Can Be Found in ${IBERTY_LIBRARY}")
  endif()
  set(IBERTY_LIB_PATH "${IBERTY_LIBRARY}")
endmacro()

# Find Glog
macro(find_glog)
  # Find the glog library
  set(CMAKE_FIND_LIBRARY_SUFFIXES ".so")
  find_library(GLOG_LIBRARY NAMES glog PATHS ${SYSTEM_LIB_PATH} ${SYSTEM_LIB64_PATH} ${SYSTEM_LIB_MULTIARCH_PATH} ${SYSTEM_LOCAL_LIB_PATH} ${SYSTEM_LOCAL_LIB64_PATH} NO_DEFAULT_PATH)
  if (NOT GLOG_LIBRARY)
    message(FATAL_ERROR "Glog Library Not Found")
  else()
    message(STATUS "Glog Library Can Be Found in ${GLOG_LIBRARY}")
  endif()
  set(GLOG_LIB_PATH "${GLOG_LIBRARY}")
endmacro()

# Find DOUBLE-CONVERSION
macro(find_double_conversion)
  # Find the double_conversion library
  set(CMAKE_FIND_LIBRARY_SUFFIXES ".so")
  find_library(DOUBLE_CONVERSION_LIBRARY NAMES double-conversion PATHS ${SYSTEM_LIB_PATH} ${SYSTEM_LIB64_PATH} ${SYSTEM_LIB_MULTIARCH_PATH} ${SYSTEM_LOCAL_LIB_PATH} ${SYSTEM_LOCAL_LIB64_PATH} NO_DEFAULT_PATH)
  if (NOT DOUBLE_CONVERSION_LIBRARY)
    message(FATAL_ERROR "Double-Conversion Library Not Found")
  else()
    message(STATUS "Double-Conversion Library Can Be Found in ${DOUBLE_CONVERSION_LIBRARY}")
  endif()
  set(DOUBLE_CONVERSION_LIB_PATH "${DOUBLE_CONVERSION_LIBRARY}")
endmacro()

# Find Fmt
macro(find_fmt)
  # Find the fmt library
  set(CMAKE_FIND_LIBRARY_SUFFIXES ".a")
  find_library(FMT_LIBRARY NAMES fmt PATHS ${SYSTEM_LIB_PATH} ${SYSTEM_LIB64_PATH} ${SYSTEM_LIB_MULTIARCH_PATH} ${SYSTEM_LOCAL_LIB_PATH} ${SYSTEM_LOCAL_LIB64_PATH} NO_DEFAULT_PATH)
  if (NOT FMT_LIBRARY)
    message(FATAL_ERROR "Fmt Library Not Found")
  else()
    message(STATUS "Fmt Library Can Be Found in ${FMT_LIBRARY}")
  endif()
  set(FMT_LIB_PATH "${FMT_LIBRARY}")
endmacro()

# Find GTEST
macro(find_gtest)
  # Find the gtest library
  set(CMAKE_FIND_LIBRARY_SUFFIXES ".a")
  find_library(GTEST_LIBRARY NAMES gtest PATHS ${SYSTEM_LIB_PATH} ${SYSTEM_LIB64_PATH} ${SYSTEM_LIB_MULTIARCH_PATH} ${SYSTEM_LOCAL_LIB_PATH} ${SYSTEM_LOCAL_LIB64_PATH} NO_DEFAULT_PATH)
  if (NOT GTEST_LIBRARY)
    message(FATAL_ERROR "Gtest Library Not Found")
  else()
    message(STATUS "Gtest Library Can Be Found in ${GTEST_LIBRARY}")
  endif()
  set(GTEST_LIB_PATH "${GTEST_LIBRARY}")
endmacro()

# Find Snappy
macro(find_snappy)
  # Find the snappy library
  set(CMAKE_FIND_LIBRARY_SUFFIXES ".so")
  find_library(SNAPPY_LIBRARY NAMES snappy PATHS ${SYSTEM_LIB_PATH} ${SYSTEM_LIB64_PATH} ${SYSTEM_LIB_MULTIARCH_PATH} ${SYSTEM_LOCAL_LIB_PATH} ${SYSTEM_LOCAL_LIB64_PATH} NO_DEFAULT_PATH)
  if (NOT SNAPPY_LIBRARY)
    message(FATAL_ERROR "Snappy Library Not Found")
  else()
    message(STATUS "Snappy Library Can Be Found in ${SNAPPY_LIBRARY}")
  endif()
  set(SNAPPY_LIB_PATH "${SNAPPY_LIBRARY}")
endmacro()

find_package(JNI REQUIRED)

macro(build_velox_exec)
  message(STATUS "Velox can be found in ${VELOX_HOME}")
  set(BINARY_RELEASE_DIR "${root_directory}/releases")
  add_library(facebook::velox::exec STATIC IMPORTED)
  add_library(facebook::velox::connector STATIC IMPORTED)
  add_library(facebook::velox::connector::hive::part STATIC IMPORTED)
  add_library(facebook::velox::dwio::common STATIC IMPORTED)
  add_library(facebook::velox::dwio::dwrf::proto STATIC IMPORTED)
  add_library(facebook::velox::core::config STATIC IMPORTED)
  add_library(facebook::velox::core STATIC IMPORTED)
  add_library(facebook::velox::vector STATIC IMPORTED)
  add_library(facebook::velox::type STATIC IMPORTED)
  add_library(facebook::velox::common::base STATIC IMPORTED)
  add_library(facebook::velox::common::base::exception STATIC IMPORTED)
  add_library(facebook::velox::exec::test::util STATIC IMPORTED)
  add_library(facebook::velox::parse::expression STATIC IMPORTED)
  add_library(facebook::velox::common::memory STATIC IMPORTED)
  add_library(facebook::velox::common::time STATIC IMPORTED)
  add_library(facebook::velox::common::process STATIC IMPORTED)
  add_library(facebook::velox::connector::hive STATIC IMPORTED)
  add_library(facebook::velox::dwio::dwrf::writer STATIC IMPORTED)
  add_library(facebook::velox::dwio::dwrf::common STATIC IMPORTED)
  add_library(facebook::velox::common::caching STATIC IMPORTED)
  add_library(facebook::velox::common::file STATIC IMPORTED)
  add_library(facebook::velox::dwio::dwrf::reader STATIC IMPORTED)
  add_library(facebook::velox::external::duckdb STATIC IMPORTED)
  add_library(facebook::velox::parse::parser STATIC IMPORTED)
  add_library(facebook::velox::duckdb::parser STATIC IMPORTED)
  add_library(facebook::velox::duckdb::conversion STATIC IMPORTED)
  add_library(facebook::velox::functions::prestosql STATIC IMPORTED)
  add_library(facebook::velox::functions::prestosql::types STATIC IMPORTED)
  add_library(facebook::velox::functions::prestosql::impl STATIC IMPORTED)
  add_library(facebook::velox::functions::json STATIC IMPORTED)
  add_library(facebook::velox::functions::hyperloglog STATIC IMPORTED)
  add_library(facebook::velox::functions::prestosql::agg STATIC IMPORTED)
  add_library(facebook::velox::functions::lib STATIC IMPORTED)
  add_library(facebook::velox::type::tz STATIC IMPORTED)
  add_library(facebook::velox::external::md5 STATIC IMPORTED)
  add_library(facebook::velox::expression STATIC IMPORTED)
  add_library(facebook::velox::expression::function STATIC IMPORTED)
  add_library(facebook::velox::buffer STATIC IMPORTED)
  add_library(facebook::velox::arrow::bridge STATIC IMPORTED)
  add_library(facebook::velox::substrait STATIC IMPORTED)
  add_library(facebook::velox::dwio::common::exception STATIC IMPORTED)
  add_library(folly STATIC IMPORTED)
  add_library(iberty STATIC IMPORTED)
  add_library(doubleconversion SHARED IMPORTED)
  add_library(snappy SHARED IMPORTED)
  add_library(glog SHARED IMPORTED)
  add_library(fmt STATIC IMPORTED)
  add_library(gtest SHARED IMPORTED)

  set_target_properties(facebook::velox::exec
                        PROPERTIES IMPORTED_LOCATION "${VELOX_EXEC_LIB_PATH}"
                                   INTERFACE_INCLUDE_DIRECTORIES
                                   "${BINARY_RELEASE_DIR}/include")
  set_target_properties(facebook::velox::connector
                        PROPERTIES IMPORTED_LOCATION "${VELOX_CONNECTOR_LIB_PATH}"
                                   INTERFACE_INCLUDE_DIRECTORIES
                                   "${BINARY_RELEASE_DIR}/include")
  set_target_properties(facebook::velox::connector::hive::part
                        PROPERTIES IMPORTED_LOCATION "${VELOX_HIVE_PART_LIB_PATH}"
                                   INTERFACE_INCLUDE_DIRECTORIES
                                   "${BINARY_RELEASE_DIR}/include")
  set_target_properties(facebook::velox::dwio::common
                        PROPERTIES IMPORTED_LOCATION "${VELOX_DWIO_COMMON_LIB_PATH}"
                                   INTERFACE_INCLUDE_DIRECTORIES
                                   "${BINARY_RELEASE_DIR}/include")
  set_target_properties(facebook::velox::dwio::dwrf::proto
                        PROPERTIES IMPORTED_LOCATION "${VELOX_DWIO_DWRF_PROTO_LIB_PATH}"
                                   INTERFACE_INCLUDE_DIRECTORIES
                                   "${BINARY_RELEASE_DIR}/include")
  set_target_properties(iberty
                        PROPERTIES IMPORTED_LOCATION "${IBERTY_LIB_PATH}"
                                   INTERFACE_INCLUDE_DIRECTORIES
                                   "${BINARY_RELEASE_DIR}/include")
  set_target_properties(folly
                        PROPERTIES IMPORTED_LOCATION "${FOLLY_LIB_PATH}"
                                   INTERFACE_INCLUDE_DIRECTORIES
                                   "${BINARY_RELEASE_DIR}/include")
  set_target_properties(facebook::velox::core::config
                        PROPERTIES IMPORTED_LOCATION "${VELOX_CORE_CONFIG_LIB_PATH}"
                                   INTERFACE_INCLUDE_DIRECTORIES
                                   "${BINARY_RELEASE_DIR}/include")
  set_target_properties(facebook::velox::core
                        PROPERTIES IMPORTED_LOCATION "${VELOX_CORE_LIB_PATH}"
                                   INTERFACE_INCLUDE_DIRECTORIES
                                   "${BINARY_RELEASE_DIR}/include")
  set_target_properties(facebook::velox::vector
                        PROPERTIES IMPORTED_LOCATION "${VELOX_VECTOR_LIB_PATH}"
                                   INTERFACE_INCLUDE_DIRECTORIES
                                   "${BINARY_RELEASE_DIR}/include")
  set_target_properties(facebook::velox::type
                        PROPERTIES IMPORTED_LOCATION "${VELOX_TYPE_LIB_PATH}"
                                   INTERFACE_INCLUDE_DIRECTORIES
                                   "${BINARY_RELEASE_DIR}/include")
  set_target_properties(glog
                        PROPERTIES IMPORTED_LOCATION "${GLOG_LIB_PATH}"
                                   INTERFACE_INCLUDE_DIRECTORIES
                                   "${BINARY_RELEASE_DIR}/include")
  set_target_properties(gtest
                        PROPERTIES IMPORTED_LOCATION "${GTEST_LIB_PATH}"
                                   INTERFACE_INCLUDE_DIRECTORIES
                                   "${BINARY_RELEASE_DIR}/include")
  set_target_properties(facebook::velox::common::base
                        PROPERTIES IMPORTED_LOCATION "${VELOX_COMMON_BASE_LIB_PATH}"
                                   INTERFACE_INCLUDE_DIRECTORIES
                                   "${BINARY_RELEASE_DIR}/include")
  set_target_properties(facebook::velox::exec::test::util
                        PROPERTIES IMPORTED_LOCATION "${VELOX_EXEC_TEST_UTIL_LIB_PATH}"
                                   INTERFACE_INCLUDE_DIRECTORIES
                                   "${BINARY_RELEASE_DIR}/include")
  set_target_properties(facebook::velox::parse::expression
                        PROPERTIES IMPORTED_LOCATION "${VELOX_PARSE_EXP_LIB_PATH}"
                                   INTERFACE_INCLUDE_DIRECTORIES
                                   "${BINARY_RELEASE_DIR}/include")
  set_target_properties(doubleconversion
                        PROPERTIES IMPORTED_LOCATION "${DOUBLE_CONVERSION_LIB_PATH}"
                                   INTERFACE_INCLUDE_DIRECTORIES
                                   "${BINARY_RELEASE_DIR}/include")                                              
  set_target_properties(fmt
                        PROPERTIES IMPORTED_LOCATION "${FMT_LIB_PATH}"
                                   INTERFACE_INCLUDE_DIRECTORIES
                                   "${BINARY_RELEASE_DIR}/include")
  set_target_properties(snappy
                        PROPERTIES IMPORTED_LOCATION "${SNAPPY_LIB_PATH}"
                                   INTERFACE_INCLUDE_DIRECTORIES
                                   "${BINARY_RELEASE_DIR}/include")
  set_target_properties(facebook::velox::common::memory
                        PROPERTIES IMPORTED_LOCATION "${VELOX_COMMON_MEM_LIB_PATH}"
                                   INTERFACE_INCLUDE_DIRECTORIES
                                   "${BINARY_RELEASE_DIR}/include")
  set_target_properties(facebook::velox::common::time
                        PROPERTIES IMPORTED_LOCATION "${VELOX_COMMON_TIME_LIB_PATH}"
                                   INTERFACE_INCLUDE_DIRECTORIES
                                   "${BINARY_RELEASE_DIR}/include")
  set_target_properties(facebook::velox::common::process
                        PROPERTIES IMPORTED_LOCATION "${VELOX_COMMON_PROCESS_LIB_PATH}"
                                   INTERFACE_INCLUDE_DIRECTORIES
                                   "${BINARY_RELEASE_DIR}/include")
  set_target_properties(facebook::velox::connector::hive
                        PROPERTIES IMPORTED_LOCATION "${VELOX_HIVE_CONNECTOR_LIB_PATH}"
                                   INTERFACE_INCLUDE_DIRECTORIES
                                   "${BINARY_RELEASE_DIR}/include")                   
  set_target_properties(facebook::velox::common::base::exception
                        PROPERTIES IMPORTED_LOCATION "${VELOX_COMMON_BASE_EXCEPTION_LIB_PATH}"
                                   INTERFACE_INCLUDE_DIRECTORIES
                                   "${BINARY_RELEASE_DIR}/include")
  set_target_properties(facebook::velox::dwio::common::exception
                                   PROPERTIES IMPORTED_LOCATION "${VELOX_DWIO_COMMON_EXCEPTION_LIB_PATH}"
                                   INTERFACE_INCLUDE_DIRECTORIES
                                   "${BINARY_RELEASE_DIR}/include")
  set_target_properties(facebook::velox::dwio::dwrf::writer
                        PROPERTIES IMPORTED_LOCATION "${VELOX_DWIO_DWRF_WRITER_LIB_PATH}"
                                   INTERFACE_INCLUDE_DIRECTORIES
                                   "${BINARY_RELEASE_DIR}/include")
  set_target_properties(facebook::velox::dwio::dwrf::common
                        PROPERTIES IMPORTED_LOCATION "${VELOX_DWIO_DWRF_COMMON_LIB_PATH}"
                                   INTERFACE_INCLUDE_DIRECTORIES
                                   "${BINARY_RELEASE_DIR}/include")
  set_target_properties(facebook::velox::common::caching
                        PROPERTIES IMPORTED_LOCATION "${VELOX_COMMON_CACHE_LIB_PATH}"
                                   INTERFACE_INCLUDE_DIRECTORIES
                                   "${BINARY_RELEASE_DIR}/include")
  set_target_properties(facebook::velox::common::file
                        PROPERTIES IMPORTED_LOCATION "${VELOX_COMMON_FILE_LIB_PATH}"
                                   INTERFACE_INCLUDE_DIRECTORIES
                                   "${BINARY_RELEASE_DIR}/include")
  set_target_properties(facebook::velox::dwio::dwrf::reader
                        PROPERTIES IMPORTED_LOCATION "${VELOX_DWIO_DWRF_READER_LIB_PATH}"
                                   INTERFACE_INCLUDE_DIRECTORIES
                                   "${BINARY_RELEASE_DIR}/include")
  set_target_properties(facebook::velox::external::duckdb
                        PROPERTIES IMPORTED_LOCATION "${VELOX_EXTERNAL_DUCKDB_LIB_PATH}"
                                   INTERFACE_INCLUDE_DIRECTORIES
                                   "${BINARY_RELEASE_DIR}/include")
  set_target_properties(facebook::velox::parse::parser
                        PROPERTIES IMPORTED_LOCATION "${VELOX_PARSE_PARSER_LIB_PATH}"
                                   INTERFACE_INCLUDE_DIRECTORIES
                                   "${BINARY_RELEASE_DIR}/include")
  set_target_properties(facebook::velox::duckdb::parser
                        PROPERTIES IMPORTED_LOCATION "${VELOX_DUCKDB_PARSER_LIB_PATH}"
                                   INTERFACE_INCLUDE_DIRECTORIES
                                   "${BINARY_RELEASE_DIR}/include")
  set_target_properties(facebook::velox::duckdb::conversion
                        PROPERTIES IMPORTED_LOCATION "${VELOX_DUCKDB_CONVERSION_LIB_PATH}"
                                   INTERFACE_INCLUDE_DIRECTORIES
                                   "${BINARY_RELEASE_DIR}/include")
  set_target_properties(facebook::velox::functions::prestosql
                        PROPERTIES IMPORTED_LOCATION "${VELOX_FUNCTIONS_PRESTOSQL_LIB_PATH}"
                                   INTERFACE_INCLUDE_DIRECTORIES
                                   "${BINARY_RELEASE_DIR}/include")
  set_target_properties(facebook::velox::functions::prestosql::types
                        PROPERTIES IMPORTED_LOCATION "${VELOX_FUNCTIONS_PRESTOSQL_TYPES_LIB_PATH}"
                                   INTERFACE_INCLUDE_DIRECTORIES
                                   "${BINARY_RELEASE_DIR}/include")
  set_target_properties(facebook::velox::functions::prestosql::impl
                        PROPERTIES IMPORTED_LOCATION "${VELOX_FUNCTIONS_PRESTOSQL_IMPL_LIB_PATH}"
                                   INTERFACE_INCLUDE_DIRECTORIES
                                   "${BINARY_RELEASE_DIR}/include")
  set_target_properties(facebook::velox::functions::json
                        PROPERTIES IMPORTED_LOCATION "${VELOX_FUNCTIONS_PRESTOSQL_JSON_LIB_PATH}"
                                   INTERFACE_INCLUDE_DIRECTORIES
                                   "${BINARY_RELEASE_DIR}/include")
  set_target_properties(facebook::velox::functions::hyperloglog
                        PROPERTIES IMPORTED_LOCATION "${VELOX_FUNCTIONS_PRESTOSQL_HYPERLOGLOG_LIB_PATH}"
                                   INTERFACE_INCLUDE_DIRECTORIES
                                   "${BINARY_RELEASE_DIR}/include")
  set_target_properties(facebook::velox::functions::prestosql::agg
                        PROPERTIES IMPORTED_LOCATION "${VELOX_FUNCTIONS_PRESTOSQL_AGG_LIB_PATH}"
                                   INTERFACE_INCLUDE_DIRECTORIES
                                   "${BINARY_RELEASE_DIR}/include")
  set_target_properties(facebook::velox::functions::lib
                        PROPERTIES IMPORTED_LOCATION "${VELOX_FUNCTIONS_LIB_PATH}"
                                   INTERFACE_INCLUDE_DIRECTORIES
                                   "${BINARY_RELEASE_DIR}/include")
  set_target_properties(facebook::velox::type::tz
                        PROPERTIES IMPORTED_LOCATION "${VELOX_TYPE_TZ_LIB_PATH}"
                                   INTERFACE_INCLUDE_DIRECTORIES
                                   "${BINARY_RELEASE_DIR}/include")
  set_target_properties(facebook::velox::external::md5
                        PROPERTIES IMPORTED_LOCATION "${VELOX_EXTERNAL_MD5_LIB_PATH}"
                                   INTERFACE_INCLUDE_DIRECTORIES
                                   "${BINARY_RELEASE_DIR}/include")
  set_target_properties(facebook::velox::expression
                        PROPERTIES IMPORTED_LOCATION "${VELOX_EXPRESSION_LIB_PATH}"
                                   INTERFACE_INCLUDE_DIRECTORIES
                                   "${BINARY_RELEASE_DIR}/include")
  set_target_properties(facebook::velox::expression::function
                        PROPERTIES IMPORTED_LOCATION "${VELOX_EXPRESSION_FUNCTIONS_LIB_PATH}"
                                   INTERFACE_INCLUDE_DIRECTORIES
                                   "${BINARY_RELEASE_DIR}/include")
  set_target_properties(facebook::velox::buffer
                        PROPERTIES IMPORTED_LOCATION "${VELOX_BUFFER_LIB_PATH}"
                                   INTERFACE_INCLUDE_DIRECTORIES
                                   "${BINARY_RELEASE_DIR}/include")
  set_target_properties(facebook::velox::arrow::bridge
                        PROPERTIES IMPORTED_LOCATION "${VELOX_ARROW_BRIDGE_LIB_PATH}"
                                   INTERFACE_INCLUDE_DIRECTORIES
                                   "${BINARY_RELEASE_DIR}/include")
  set_target_properties(facebook::velox::substrait
                        PROPERTIES IMPORTED_LOCATION "${VELOX_SUBSTRAIT_LIB_PATH}"
                                   INTERFACE_INCLUDE_DIRECTORIES
                                   "${BINARY_RELEASE_DIR}/include")

  target_link_libraries(velox
                        LINK_PUBLIC spark_columnar_jni
                        LINK_PUBLIC facebook::velox::functions::prestosql::agg
                        LINK_PUBLIC facebook::velox::exec
                        LINK_PUBLIC facebook::velox::substrait
                        LINK_PUBLIC facebook::velox::connector
                        LINK_PUBLIC facebook::velox::connector::hive::part
                        LINK_PUBLIC facebook::velox::dwio::dwrf::proto
                        LINK_PUBLIC facebook::velox::functions::prestosql
                        LINK_PUBLIC facebook::velox::functions::prestosql::impl
                        LINK_PUBLIC facebook::velox::functions::json
                        LINK_PUBLIC facebook::velox::functions::hyperloglog
                        LINK_PUBLIC facebook::velox::functions::lib
                        LINK_PUBLIC facebook::velox::vector
                        LINK_PUBLIC facebook::velox::exec::test::util
                        LINK_PUBLIC facebook::velox::parse::parser
                        LINK_PUBLIC facebook::velox::parse::expression
                        LINK_PUBLIC facebook::velox::arrow::bridge
                        LINK_PUBLIC facebook::velox::common::memory
                        LINK_PUBLIC facebook::velox::common::time
                        LINK_PUBLIC facebook::velox::common::base::exception
                        LINK_PUBLIC facebook::velox::common::process
                        LINK_PUBLIC facebook::velox::connector::hive
                        LINK_PUBLIC facebook::velox::expression
                        LINK_PUBLIC facebook::velox::expression::function
                        LINK_PUBLIC facebook::velox::dwio::dwrf::writer
                        LINK_PUBLIC facebook::velox::dwio::dwrf::reader
                        LINK_PUBLIC facebook::velox::dwio::dwrf::common
                        LINK_PUBLIC facebook::velox::common::caching
                        LINK_PUBLIC facebook::velox::dwio::common
                        LINK_PUBLIC facebook::velox::type
                        LINK_PUBLIC facebook::velox::common::file
                        LINK_PUBLIC facebook::velox::common::base
                        LINK_PUBLIC facebook::velox::duckdb::parser
                        LINK_PUBLIC facebook::velox::duckdb::conversion
                        LINK_PUBLIC facebook::velox::external::duckdb
                        LINK_PUBLIC facebook::velox::core
                        LINK_PUBLIC facebook::velox::core::config
                        LINK_PUBLIC facebook::velox::type::tz
                        LINK_PUBLIC facebook::velox::external::md5
                        LINK_PUBLIC facebook::velox::buffer
                        LINK_PUBLIC facebook::velox::functions::prestosql::types
                        LINK_PUBLIC facebook::velox::dwio::common::exception
                        LINK_PUBLIC gtest
                        LINK_PUBLIC folly
                        LINK_PUBLIC iberty
                        LINK_PUBLIC doubleconversion
                        LINK_PUBLIC snappy
                        LINK_PUBLIC glog
                        LINK_PUBLIC fmt)
endmacro()

# Build Velox backend.
set(VELOX_SRCS
    jni/jni_wrapper.cc
    compute/VeloxPlanConverter.cc
    compute/ArrowTypeUtils.cc
    compute/VeloxToRowConverter.cc
    )
add_library(velox SHARED ${VELOX_SRCS})

target_include_directories(velox PUBLIC ${CMAKE_SYSTEM_INCLUDE_PATH} ${JNI_INCLUDE_DIRS} ${CMAKE_CURRENT_SOURCE_DIR} ${root_directory}/src ${VELOX_HOME} ${VELOX_HOME}/velox/vector ${VELOX_REALEASE_PATH} ${VELOX_HOME}/third_party/xsimd/include/)

set_target_properties(velox PROPERTIES
    LIBRARY_OUTPUT_DIRECTORY ${root_directory}/releases
    )

find_folly()
find_iberty()
find_glog()
find_double_conversion()
find_fmt()
find_gtest()
find_snappy()
build_velox_exec()

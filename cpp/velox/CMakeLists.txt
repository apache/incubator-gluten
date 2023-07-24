# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

cmake_minimum_required(VERSION 3.16)

project(velox)

include(ExternalProject)
include(FindPkgConfig)
include(GNUInstallDirs)
include(CheckCXXCompilerFlag)
include(FindPackageHandleStandardArgs)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
if (CMAKE_SYSTEM_PROCESSOR MATCHES "(x86)|(X86)|(amd64)|(AMD64)")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mavx")
endif()
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-deprecated-declarations -Wno-attributes -Wno-class-memaccess")

set(SYSTEM_LIB_PATH "/usr/lib" CACHE PATH "System Lib dir")
set(SYSTEM_LIB64_PATH "/usr/lib64" CACHE PATH "System Lib64 dir")
set(SYSTEM_LOCAL_LIB_PATH "/usr/local/lib" CACHE PATH "System Local Lib dir")
set(SYSTEM_LOCAL_LIB64_PATH "/usr/local/lib64" CACHE PATH "System Local Lib64 dir")
if (CMAKE_SYSTEM_PROCESSOR MATCHES "(x86)|(X86)|(amd64)|(AMD64)")
  set(SYSTEM_LIB_MULTIARCH_PATH "/usr/lib/x86_64-linux-gnu" CACHE PATH "System Lib MultiArch dir")
elseif(CMAKE_SYSTEM_PROCESSOR STREQUAL aarch64)
  set(SYSTEM_LIB_MULTIARCH_PATH "/usr/lib/aarch64-linux-gnu" CACHE PATH "System Lib MultiArch dir")
else()
  message(FATAL_ERROR "Unsupported processor type: ${CMAKE_SYSTEM_PROCESSOR}")
endif()

if (NOT DEFINED VELOX_HOME)
  set(VELOX_HOME ${GLUTEN_HOME}/ep/build-velox/build/velox_ep)
  message(STATUS "Set VELOX_HOME to ${VELOX_HOME}")
endif()

# User can specify VELOX_BUILD_PATH, if Velox are built elsewhere.
if(NOT DEFINED VELOX_BUILD_PATH)
  if (${CMAKE_BUILD_TYPE} STREQUAL "Debug")
    set(VELOX_BUILD_PATH "${VELOX_HOME}/_build/debug" CACHE PATH "Velox build directory.")
  else()
    set(VELOX_BUILD_PATH "${VELOX_HOME}/_build/release" CACHE PATH "Velox build directory.")
  endif()
endif()

set(VELOX_COMPONENTS_PATH "${VELOX_BUILD_PATH}/velox")

function(ADD_VELOX_DEPENDENCY VELOX_DEP_LIB_NAME VELOX_DEP_LIB_PATH)
  if(NOT EXISTS ${VELOX_DEP_LIB_PATH})
    message(FATAL_ERROR "Velox library not exists: ${VELOX_DEP_LIB_PATH}")
  endif()
  set(VELOX_DEP_LIB facebook::velox::${VELOX_DEP_LIB_NAME})
  add_library(${VELOX_DEP_LIB} STATIC IMPORTED)
  set_target_properties(${VELOX_DEP_LIB} PROPERTIES
      IMPORTED_LOCATION ${VELOX_DEP_LIB_PATH})
  target_link_libraries(velox PUBLIC ${VELOX_DEP_LIB})
endfunction()

macro(ADD_VELOX_DEPENDENCIES)
  add_velox_dependency(functions::sparksql::lib "${VELOX_COMPONENTS_PATH}/functions/sparksql/libvelox_functions_spark.a")
  add_velox_dependency(functions::sparksql::agg "${VELOX_COMPONENTS_PATH}/functions/sparksql/aggregates/libvelox_functions_spark_aggregates.a")
  add_velox_dependency(functions::window::sparksql "${VELOX_COMPONENTS_PATH}/functions/sparksql/window/libvelox_functions_spark_window.a")
  add_velox_dependency(functions::prestosql::agg "${VELOX_COMPONENTS_PATH}/functions/prestosql/aggregates/libvelox_aggregates.a")
  add_velox_dependency(functions::lib::agg "${VELOX_COMPONENTS_PATH}/functions/lib/aggregates/libvelox_functions_aggregates.a")
  add_velox_dependency(functions::prestosql::window "${VELOX_COMPONENTS_PATH}/functions/prestosql/window/libvelox_window.a")
  add_velox_dependency(functions::lib::window "${VELOX_COMPONENTS_PATH}/functions/lib/window/libvelox_functions_window.a")
  add_velox_dependency(velox::buffer "${VELOX_COMPONENTS_PATH}/buffer/libvelox_buffer.a")

  add_velox_dependency(exec "${VELOX_COMPONENTS_PATH}/exec/libvelox_exec.a")
  add_velox_dependency(functions::isnull "${VELOX_COMPONENTS_PATH}/functions/lib/libvelox_is_null_functions.a")
  add_velox_dependency(functions::prestosql "${VELOX_COMPONENTS_PATH}/functions/prestosql/registration/libvelox_functions_prestosql.a")
  add_velox_dependency(functions::prestosql::impl "${VELOX_COMPONENTS_PATH}/functions/prestosql/libvelox_functions_prestosql_impl.a")
  add_velox_dependency(functions::json "${VELOX_COMPONENTS_PATH}/functions/prestosql/json/libvelox_functions_json.a")
  add_velox_dependency(functions::hyperloglog "${VELOX_COMPONENTS_PATH}/common/hyperloglog/libvelox_common_hyperloglog.a")
  add_velox_dependency(functions::lib "${VELOX_COMPONENTS_PATH}/functions/lib/libvelox_functions_lib.a")
  add_velox_dependency(common::test_util "${VELOX_COMPONENTS_PATH}/common/testutil/libvelox_test_util.a")
  if(BUILD_TESTS)
    add_velox_dependency(exec::test "${VELOX_COMPONENTS_PATH}/exec/tests/utils/libvelox_exec_test_lib.a")
    add_velox_dependency(temp::path "${VELOX_COMPONENTS_PATH}/exec/tests/utils/libvelox_temp_path.a")
    add_velox_dependency(dwio::common::test::utils "${VELOX_COMPONENTS_PATH}/dwio/common/tests/utils/libvelox_dwio_common_test_utils.a")
  endif()
  add_velox_dependency(parse::parser "${VELOX_COMPONENTS_PATH}/parse/libvelox_parse_parser.a")
  if(BUILD_TESTS)
    add_velox_dependency(duckdb::parser "${VELOX_COMPONENTS_PATH}/duckdb/conversion/libvelox_duckdb_parser.a")
  endif()
  add_velox_dependency(parse::expression "${VELOX_COMPONENTS_PATH}/parse/libvelox_parse_expression.a")
  if(BUILD_TESTS)
    add_velox_dependency(parse::utils "${VELOX_COMPONENTS_PATH}/parse/libvelox_parse_utils.a")
    add_velox_dependency(function::registry "${VELOX_COMPONENTS_PATH}/functions/libvelox_function_registry.a")
  endif()
  add_velox_dependency(vector::arrow::bridge "${VELOX_COMPONENTS_PATH}/vector/arrow/libvelox_arrow_bridge.a")
  add_velox_dependency(row "${VELOX_COMPONENTS_PATH}/row/libvelox_row_fast.a")
  add_velox_dependency(connector::hive "${VELOX_COMPONENTS_PATH}/connectors/hive/libvelox_hive_connector.a")
  add_velox_dependency(connector "${VELOX_COMPONENTS_PATH}/connectors/libvelox_connector.a")
  add_velox_dependency(connector::hive_parition "${VELOX_COMPONENTS_PATH}/connectors/hive/libvelox_hive_partition_function.a")
  if(ENABLE_HDFS)
    add_velox_dependency(connector::hive::hdfs "${VELOX_COMPONENTS_PATH}/connectors/hive/storage_adapters/hdfs/libvelox_hdfs.a")
  endif()
  if(ENABLE_S3)
    add_velox_dependency(connector::hive::s3fs "${VELOX_COMPONENTS_PATH}/connectors/hive/storage_adapters/s3fs/libvelox_s3fs.a")
  endif()
  add_velox_dependency(dwio::dwrf::test_utils "${VELOX_COMPONENTS_PATH}/dwio/dwrf/test/utils/libvelox_dwrf_test_utils.a")
  add_velox_dependency(dwio::dwrf::writer "${VELOX_COMPONENTS_PATH}/dwio/dwrf/writer/libvelox_dwio_dwrf_writer.a")
  add_velox_dependency(dwio::dwrf::reader "${VELOX_COMPONENTS_PATH}/dwio/dwrf/reader/libvelox_dwio_dwrf_reader.a")
  add_velox_dependency(dwio::dwrf::utils "${VELOX_COMPONENTS_PATH}/dwio/dwrf/utils/libvelox_dwio_dwrf_utils.a")
  add_velox_dependency(dwio::dwrf::common "${VELOX_COMPONENTS_PATH}/dwio/dwrf/common/libvelox_dwio_dwrf_common.a")
  add_velox_dependency(dwio::common::utils "${VELOX_COMPONENTS_PATH}/dwio/common/tests/utils/libvelox_dwio_common_test_utils.a")
  add_velox_dependency(parquet "${VELOX_COMPONENTS_PATH}/dwio/parquet/libvelox_dwio_parquet_reader.a")
  add_velox_dependency(parquet::reader::native "${VELOX_COMPONENTS_PATH}/dwio/parquet/reader/libvelox_dwio_native_parquet_reader.a")
  if(BUILD_BENCHMARKS)
    add_velox_dependency(parquet::reader::duckdb "${VELOX_COMPONENTS_PATH}/dwio/parquet/duckdb_reader/libvelox_dwio_duckdb_parquet_reader.a")
    add_velox_dependency(parquet::reader::duckdb_allocator "${VELOX_COMPONENTS_PATH}/duckdb/memory/libvelox_duckdb_allocator.a")
    add_velox_dependency(parquet::reader::duckdb_conversion "${VELOX_COMPONENTS_PATH}/duckdb/conversion/libvelox_duckdb_conversion.a")
    add_velox_dependency(duckdb::duckdb "${VELOX_COMPONENTS_PATH}/external/duckdb/libduckdb.a")
    add_velox_dependency(parquet::reader::duckdb_tpch_extension "${VELOX_COMPONENTS_PATH}/external/duckdb/tpch/libtpch_extension.a")
    add_velox_dependency(dbgen "${VELOX_COMPONENTS_PATH}/external/duckdb/tpch/dbgen/libdbgen.a")
  endif()

  add_velox_dependency(parquet::reader::thrift "${VELOX_COMPONENTS_PATH}/dwio/parquet/thrift/libvelox_dwio_parquet_thrift.a")

  add_velox_dependency(dwio::common "${VELOX_COMPONENTS_PATH}/dwio/common/libvelox_dwio_common.a")
  add_velox_dependency(functions::prestosql::types "${VELOX_COMPONENTS_PATH}/functions/prestosql/types/libvelox_presto_types.a")
  add_velox_dependency(expression "${VELOX_COMPONENTS_PATH}/expression/libvelox_expression.a")
  add_velox_dependency(core "${VELOX_COMPONENTS_PATH}/core/libvelox_core.a")

  add_velox_dependency(type "${VELOX_COMPONENTS_PATH}/type/libvelox_type.a")
  add_velox_dependency(vector::serializes "${VELOX_COMPONENTS_PATH}/serializers/libvelox_presto_serializer.a")
  add_velox_dependency(functions::lib::util "${VELOX_COMPONENTS_PATH}/functions/lib/libvelox_functions_util.a")
  add_velox_dependency(vector "${VELOX_COMPONENTS_PATH}/vector/libvelox_vector.a")
  add_velox_dependency(expression::function "${VELOX_COMPONENTS_PATH}/expression/libvelox_expression_functions.a")
  add_velox_dependency(expression::type_calculation "${VELOX_COMPONENTS_PATH}/expression/type_calculation/libvelox_type_calculation.a")

  add_velox_dependency(common::caching "${VELOX_COMPONENTS_PATH}/common/caching/libvelox_caching.a")
  add_velox_dependency(common::base "${VELOX_COMPONENTS_PATH}/common/base/libvelox_common_base.a")
  if(BUILD_TESTS)
    add_velox_dependency(tpch::gen "${VELOX_COMPONENTS_PATH}/tpch/gen/libvelox_tpch_gen.a")
  endif()
  add_velox_dependency(common::memory "${VELOX_COMPONENTS_PATH}/common/memory/libvelox_memory.a")
  add_velox_dependency(common::serialization "${VELOX_COMPONENTS_PATH}/common/serialization/libvelox_serialization.a")
  add_velox_dependency(common::base::exception "${VELOX_COMPONENTS_PATH}/common/base/libvelox_exception.a")

  add_velox_dependency(type::tz "${VELOX_COMPONENTS_PATH}/type/tz/libvelox_type_tz.a")
  add_velox_dependency(dwio::dwrf::proto "${VELOX_COMPONENTS_PATH}/dwio/dwrf/proto/libvelox_dwio_dwrf_proto.a")
  add_velox_dependency(dwio::common::exception "${VELOX_COMPONENTS_PATH}/dwio/common/exception/libvelox_dwio_common_exception.a")
  add_velox_dependency(dwio::common::encryption "${VELOX_COMPONENTS_PATH}/dwio/common/encryption/libvelox_dwio_common_encryption.a")

  add_velox_dependency(core::config "${VELOX_COMPONENTS_PATH}/core/libvelox_config.a")
  add_velox_dependency(common::encode "${VELOX_COMPONENTS_PATH}/common/encode/libvelox_encode.a")
  add_velox_dependency(common::time "${VELOX_COMPONENTS_PATH}/common/time/libvelox_time.a")
  add_velox_dependency(common::file "${VELOX_COMPONENTS_PATH}/common/file/libvelox_file.a")
  add_velox_dependency(common::process "${VELOX_COMPONENTS_PATH}/common/process/libvelox_process.a")

  add_velox_dependency(external::md5 "${VELOX_COMPONENTS_PATH}/external/md5/libmd5.a")
  add_velox_dependency(external::date "${VELOX_COMPONENTS_PATH}/external/date/libvelox_external_date.a")
  add_velox_dependency(velox::parquet::writer "${VELOX_COMPONENTS_PATH}/dwio/parquet/libvelox_dwio_parquet_writer.a")
  add_velox_dependency(velox::arrow::parquet::writer "${VELOX_COMPONENTS_PATH}/dwio/parquet/writer/libvelox_dwio_arrow_parquet_writer.a")
  
  if(BUILD_TESTS)
    add_velox_dependency(vector::test::util "${VELOX_COMPONENTS_PATH}/vector/tests/utils/libvelox_vector_test_lib.a")
  endif()
endmacro()

macro(find_libhdfs3)
  find_package(libhdfs3 CONFIG)
  if(libhdfs3_FOUND AND TARGET HDFS::hdfs3)
    set(LIBHDFS3_LIBRARY HDFS::hdfs3)
  else()
    find_path(libhdfs3_INCLUDE_DIR hdfs/hdfs.h)
    set(CMAKE_FIND_LIBRARY_SUFFIXES ".so")
    find_library(libhdfs3_LIBRARY NAMES hdfs3)
    find_package_handle_standard_args(libhdfs3 DEFAULT_MSG
      libhdfs3_INCLUDE_DIR
      libhdfs3_LIBRARY
    )
    add_library(HDFS::hdfs3 SHARED IMPORTED)
      set_target_properties(HDFS::hdfs3 PROPERTIES
      INTERFACE_INCLUDE_DIRECTORIES "${libhdfs3_INCLUDE_DIR}"
      IMPORTED_LOCATION "${libhdfs3_LIBRARY}"
    )
  endif()

  if (NOT libhdfs3_FOUND)
    message(FATAL_ERROR "LIBHDFS3 Library Not Found")
  endif()
endmacro()

macro(find_re2)
  find_package(re2 CONFIG)
  if(re2_FOUND AND TARGET re2::re2)
    set(RE2_LIBRARY re2::re2)
  else()
    find_library(RE2_LIBRARY NAMES re2 PATHS ${SYSTEM_LIB_PATH} ${SYSTEM_LIB64_PATH} ${SYSTEM_LIB_MULTIARCH_PATH} ${SYSTEM_LOCAL_LIB_PATH} ${SYSTEM_LOCAL_LIB64_PATH} NO_DEFAULT_PATH)
  endif()

  if (NOT RE2_LIBRARY)
     message(FATAL_ERROR "RE2 Library Not Found")
  else()
    message(STATUS "RE2 Library Can Be Found in ${RE2_LIBRARY}")
  endif()
endmacro()

macro(find_awssdk)
  set (CMAKE_FIND_LIBRARY_SUFFIXES ".a")
  find_package(AWSSDK REQUIRED COMPONENTS s3;identity-management)
endmacro()


# Build Velox backend.
set(VELOX_SRCS
    jni/VeloxJniWrapper.cc
    shuffle/VeloxShuffleReader.cc
    shuffle/VeloxShuffleWriter.cc
    compute/VeloxBackend.cc
    compute/VeloxInitializer.cc
    compute/WholeStageResultIterator.cc
    compute/VeloxPlanConverter.cc
    operators/functions/RegistrationAllFunctions.cc
    operators/serializer/VeloxColumnarToRowConverter.cc
    operators/serializer/VeloxColumnarBatchSerializer.cc
    operators/serializer/VeloxRowToColumnarConverter.cc
    operators/writer/VeloxParquetDatasource.cc
    memory/LargeMemoryPool.cc
    memory/VeloxMemoryPool.cc
    memory/VeloxColumnarBatch.cc
    jni/JniFileSystem.cc
    substrait/SubstraitParser.cc
    substrait/SubstraitToVeloxExpr.cc
    substrait/SubstraitToVeloxPlan.cc
    substrait/SubstraitToVeloxPlanValidator.cc
    substrait/VariantToVectorConverter.cc
    substrait/TypeUtils.cc
    substrait/SubstraitExtensionCollector.cc
    substrait/VeloxSubstraitSignature.cc
    substrait/VeloxToSubstraitExpr.cc
    substrait/VeloxToSubstraitPlan.cc
    substrait/VeloxToSubstraitType.cc
    utils/VeloxArrowUtils.cc
    utils/ArrowTypeUtils.cc
    utils/ConfigExtractor.cc
    )
add_library(velox SHARED ${VELOX_SRCS})

if(ENABLE_GLUTEN_VCPKG)
  # Hide symbols of static dependencies
  target_link_options(velox PRIVATE -Wl,--version-script=${CMAKE_CURRENT_SOURCE_DIR}/symbols.map)
endif()

target_include_directories(velox PUBLIC
    ${CMAKE_SYSTEM_INCLUDE_PATH}
    ${JNI_INCLUDE_DIRS}
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${root_directory}/src
    ${VELOX_HOME}
    ${VELOX_BUILD_PATH}
    ${VELOX_BUILD_PATH}/_deps/xsimd-src/include/
    ${VELOX_HOME}/velox/vector
    ${VELOX_HOME}/velox/connectors
    ${VELOX_HOME}/velox/external/xxhash/
    ${VELOX_HOME}/third_party/xsimd/include/)

set_target_properties(velox PROPERTIES
    LIBRARY_OUTPUT_DIRECTORY ${root_directory}/releases
    )

## If the folly is not builded in system directories, please add
## `-DCMAKE_PREFIX_PATH="${folly builded directory}" to the parameters of cmake.
## It is also applicable to other dependencies.
find_package(Folly REQUIRED CONFIG)

if(ENABLE_GLUTEN_VCPKG)
  find_package(gflags REQUIRED COMPONENTS static CONFIG)
else()
  find_package(gflags REQUIRED COMPONENTS shared CONFIG)
endif()

target_include_directories(velox PUBLIC
        ${GTEST_INCLUDE_DIRS}
        ${PROTOBUF_INCLUDE})

target_link_libraries(velox PUBLIC gluten)
add_velox_dependencies()
target_link_libraries(velox PUBLIC Folly::folly)
find_re2()
target_link_libraries(velox PUBLIC ${RE2_LIBRARY})

find_package(simdjson REQUIRED)
if(TARGET simdjson::simdjson AND NOT TARGET simdjson)
  add_library(simdjson INTERFACE)
  target_link_libraries(simdjson INTERFACE simdjson::simdjson)
endif()
target_link_libraries(velox PUBLIC simdjson)

if(ENABLE_GLUTEN_VCPKG)
  find_package(Thrift CONFIG)
else()
  # Prefer the shared library on system.
  set(ARROW_THRIFT_USE_SHARED ON)
  find_package(Thrift)
endif()

if(Thrift_FOUND)
  target_link_libraries(velox PUBLIC thrift::thrift)
else()
  add_velox_dependency(thrift "${ARROW_HOME}/arrow_ep/cpp/build/thrift_ep-install/lib/libthrift.a")
endif()

if(BUILD_TESTS)
  add_subdirectory(tests)
endif()

if(ENABLE_HDFS)
  add_definitions(-DENABLE_HDFS)
  find_libhdfs3()
  target_link_libraries(velox PUBLIC HDFS::hdfs3)
endif()

if(BUILD_BENCHMARKS)
  add_subdirectory(benchmarks)
endif()

if(ENABLE_S3)
  add_definitions(-DENABLE_S3)
  find_awssdk()
  target_link_libraries(velox PUBLIC ${AWSSDK_LIBRARIES})
endif()

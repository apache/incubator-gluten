cmake_minimum_required(VERSION 3.16)

project(spark_columnar_jni)

include(ExternalProject)
include(FindPkgConfig)
include(GNUInstallDirs)
include(CheckCXXCompilerFlag)

set(CMAKE_CXX_STANDARD 17)

set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_BUILD_TYPE  "Release")

set(ARROW_ROOT "/usr/local" CACHE PATH "Arrow Root dir")
set(SYSTEM_LIB_PATH "/usr/lib" CACHE PATH "System Lib dir")
set(SYSTEM_LIB64_PATH "/usr/lib64" CACHE PATH "System Lib64 dir")
set(SYSTEM_LOCAL_LIB_PATH "/usr/local/lib" CACHE PATH "System Local Lib dir")
set(SYSTEM_LOCAL_LIB64_PATH "/usr/local/lib64" CACHE PATH "System Local Lib64 dir")

set(ARROW_BFS_INSTALL_DIR "/usr/local" CACHE PATH "Arrow Build from Source dir")
set(ARROW_LIB_NAME arrow)
set(GANDIVA_LIB_NAME gandiva)
set(ARROW_SHARED_LIBRARY_SUFFIX ".so.400")
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
set(VELOX_ARROW_BRIDGE_LIB_PATH "${VELOX_BUILD_PATH}/vector/arrow/libvelox_arrow_bridge.a")

option(BUILD_ARROW "Build Arrow from Source" ON)
option(STATIC_ARROW "Build Arrow with Static Libraries" OFF)
option(BUILD_PROTOBUF "Build Protobuf from Source" ON)
option(USE_AVX512 "Build with AVX-512 optimizations" OFF)
option(TESTS "Build the tests" OFF)
option(BENCHMARKS "Build the benchmarks" OFF)
option(BUILD_JEMALLOC "Build Jemalloc from Source" OFF)
option(DEBUG "Enable Debug Info" OFF)

set(BOOST_MIN_VERSION "1.42.0")
find_package(Boost REQUIRED)
INCLUDE_DIRECTORIES(${Boost_INCLUDE_DIRS})

set(JEMALLOC_BUILD_VERSION "5.2.1")

find_package(JNI REQUIRED)
set(source_root_directory ${CMAKE_CURRENT_SOURCE_DIR})
set(proto_directory ${CMAKE_CURRENT_SOURCE_DIR}/../../jvm/src/main/resources/substrait/proto)
set(substrait_proto_directory ${proto_directory}/substrait)
message(STATUS "Set Proto Directory in ${proto_directory}")

find_program(CCACHE_FOUND ccache)
if(CCACHE_FOUND)
  set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE ccache)
  set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK ccache)
endif(CCACHE_FOUND)

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

# Building Protobuf
macro(build_protobuf)
  message(STATUS "Building Protocol Buffers from Source")
  set (PROTOBUF_SOURCE_URL
       "https://github.com/protocolbuffers/protobuf/releases/download/v3.13.0/protobuf-all-3.13.0.tar.gz"
        "https://github.com/ursa-labs/thirdparty/releases/download/latest/protobuf-v3.13.0.tar.gz"
  )
  set(PROTOBUF_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/protobuf_ep-install")
  set(PROTOBUF_INCLUDE_DIR "${PROTOBUF_PREFIX}/include")
  set(
    PROTOBUF_STATIC_LIB
    "${PROTOBUF_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}protobuf${CMAKE_STATIC_LIBRARY_SUFFIX}"
    )
  set(
    PROTOC_STATIC_LIB
    "${PROTOBUF_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}protoc${CMAKE_STATIC_LIBRARY_SUFFIX}"
    )
  set(
    PROTOC_BIN
    "${PROTOBUF_PREFIX}/bin/protoc"
    )
  set(
    PROTOBUF_INCLUDE
    "${PROTOBUF_PREFIX}/include"
    )
  set(PROTOBUF_COMPILER "${PROTOBUF_PREFIX}/bin/protoc")
  set(PROTOBUF_CONFIGURE_ARGS
      "AR=${CMAKE_AR}"
      "RANLIB=${CMAKE_RANLIB}"
      "CC=${CMAKE_C_COMPILER}"
      "CXX=${CMAKE_CXX_COMPILER}"
      "--disable-shared"
      "--prefix=${PROTOBUF_PREFIX}"
      "CFLAGS=-fPIC"
      "CXXFLAGS=-fPIC")
  set(PROTOBUF_BUILD_COMMAND ${MAKE} ${MAKE_BUILD_ARGS})
  ExternalProject_Add(protobuf_ep
                      PREFIX protobuf_ep
                      CONFIGURE_COMMAND "./configure" ${PROTOBUF_CONFIGURE_ARGS}
                      BUILD_COMMAND ${PROTOBUF_BUILD_COMMAND}
                      BUILD_IN_SOURCE 1
                      URL_MD5 cafa623d51361228c83c874d95f51992
                      URL ${PROTOBUF_SOURCE_URL}
  )

  file(MAKE_DIRECTORY "${PROTOBUF_INCLUDE_DIR}")
  add_library(protobuf::libprotobuf STATIC IMPORTED)
  set_target_properties(
    protobuf::libprotobuf
    PROPERTIES IMPORTED_LOCATION "${PROTOBUF_STATIC_LIB}" INTERFACE_INCLUDE_DIRECTORIES
               "${PROTOBUF_INCLUDE_DIR}")
  add_dependencies(protobuf::libprotobuf protobuf_ep)
endmacro()

macro(find_protobuf)
  # Find the existing Protobuf
  set(CMAKE_FIND_LIBRARY_SUFFIXES ".so")
  find_package(Protobuf)
  if ("${Protobuf_LIBRARY}" STREQUAL "Protobuf_LIBRARY-NOTFOUND")
    message(FATAL_ERROR "Protobuf Library Not Found")
  endif()
  set(PROTOC_BIN ${Protobuf_PROTOC_EXECUTABLE})
endmacro()

if(DEBUG)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -O0 -DDEBUG -DDEBUG_LEVEL_1 -DDEBUG_LEVEL_2")
endif()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mavx")

if(USE_AVX512)
  # Only enable additional instruction sets if they are supported
  message(STATUS "System processor: ${CMAKE_SYSTEM_PROCESSOR}")
  if (CMAKE_SYSTEM_PROCESSOR MATCHES "(x86)|(X86)|(amd64)|(AMD64)")
    set(AVX512_FLAG "-march=skylake-avx512")
    check_cxx_compiler_flag(${AVX512_FLAG} CXX_SUPPORTS_AVX512)
    if(NOT CXX_SUPPORTS_AVX512)
      message(FATAL_ERROR "AVX512 required but compiler doesn't support it.")
    endif()
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${AVX512_FLAG}")
    add_definitions(-DCOLUMNAR_PLUGIN_USE_AVX512)
  endif ()
endif()

# Build Arrow macro
macro(build_arrow STATIC_ARROW)
  set(ARROW_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/arrow_ep-install")
  message(STATUS "ARROW_PREFIX: ${ARROW_PREFIX}")
  set(ARROW_SOURCE_DIR "${CMAKE_CURRENT_BINARY_DIR}/arrow_ep")
  message(STATUS "ARROW_SOURCE_DIR: ${ARROW_SOURCE_DIR}")
  set(ARROW_INCLUDE_DIR "${ARROW_PREFIX}/include")
  set(BINARY_RELEASE_DIR "${root_directory}/releases")

  ExternalProject_Add(arrow_ep
                      GIT_REPOSITORY https://github.com/oap-project/arrow.git
                      SOURCE_DIR ${ARROW_SOURCE_DIR}
                      GIT_TAG arrow-4.0.0-oap
                      BUILD_IN_SOURCE 1
                      INSTALL_DIR ${ARROW_PREFIX}
                      INSTALL_COMMAND make install
                      SOURCE_SUBDIR cpp
                      CMAKE_ARGS 
                      -DARROW_BUILD_STATIC=OFF
                      -DARROW_BUILD_SHARED=ON
                      -DARROW_COMPUTE=ON
                      -DARROW_S3=ON
                      -DARROW_GANDIVA_JAVA=ON
                      -DARROW_GANDIVA=ON
                      -DARROW_PARQUET=ON
                      -DARROW_CSV=ON
                      -DARROW_HDFS=ON
                      -DARROW_BOOST_USE_SHARED=OFF
                      -DARROW_JNI=ON
                      -DARROW_DATASET=ON
                      -DARROW_WITH_PROTOBUF=ON
                      -DARROW_WITH_SNAPPY=ON
                      -DARROW_WITH_LZ4=ON
                      -DARROW_WITH_ZSTD=OFF
                      -DARROW_WITH_BROTLI=OFF
                      -DARROW_WITH_ZLIB=OFF
                      -DARROW_WITH_FASTPFOR=ON
                      -DARROW_FILESYSTEM=ON
                      -DARROW_JSON=ON
                      -DARROW_FLIGHT=OFF
                      -DARROW_JEMALLOC=ON
                      -DARROW_SIMD_LEVEL=AVX2
                      -DARROW_RUNTIME_SIMD_LEVEL=MAX
                      -DARROW_DEPENDENCY_SOURCE=BUNDLED
                      -DCMAKE_INSTALL_PREFIX=${ARROW_PREFIX}
                      -DCMAKE_INSTALL_LIBDIR=lib)

  ExternalProject_Add_Step(arrow_ep java_install
                      COMMAND mvn clean install -P arrow-jni -am -Darrow.cpp.build.dir=${ARROW_PREFIX}/lib -DskipTests -Dcheckstyle.skip
                      COMMENT "Arrow Java maven install after CPP make install"
                      DEPENDEES mkdir download update patch configure build install
                      WORKING_DIRECTORY "${ARROW_SOURCE_DIR}/java"
  )
  add_dependencies(arrow_ep jni_proto)

  file(MAKE_DIRECTORY "${ARROW_PREFIX}/include")

  if(STATIC_ARROW)
    # Load Static Arrow Library
    message(FATAL_ERROR "Not Support Static Arrow")

    set(THREADS_PREFER_PTHREAD_FLAG ON)
    find_package(Threads REQUIRED)

    set(ARROW_LIB_NAME arrow_bundled_dependencies)
    set(
      ARROW_STATIC_LIB
      "${ARROW_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}${ARROW_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}"
      )
    add_library(Arrow::arrow STATIC IMPORTED)
    set_target_properties(Arrow::arrow
                          PROPERTIES IMPORTED_LOCATION "${ARROW_STATIC_LIB}"
                                     INTERFACE_INCLUDE_DIRECTORIES
                                      "${ARROW_PREFIX}/include")
    add_dependencies(Arrow::arrow arrow_ep)

    # Load Static Gandiva Library
    set(
      GANDIVA_STATIC_LIB
      "${ARROW_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}${GANDIVA_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}"
      )
    add_library(Arrow::gandiva STATIC IMPORTED)
    set_target_properties(Arrow::gandiva
                          PROPERTIES IMPORTED_LOCATION "${GANDIVA_STATIC_LIB}"
                                     INTERFACE_INCLUDE_DIRECTORIES
                                      "${ARROW_PREFIX}/include")
    add_dependencies(Arrow::gandiva arrow_ep)
    target_link_libraries(spark_columnar_jni PRIVATE Arrow::arrow Arrow::gandiva Threads::Threads)

  else()

    ExternalProject_Add_Step(arrow_ep copy_arrow_binary_400_0_0
                      COMMAND cp -a ${ARROW_PREFIX}/lib/${CMAKE_SHARED_LIBRARY_PREFIX}${ARROW_LIB_NAME}${ARROW_SHARED_LIBRARY_SUFFIX} ${root_directory}/releases/
                      COMMENT "Copy libarrow.so.400.0.0 to releases/"
                      DEPENDEES mkdir download update patch configure build install java_install
                      WORKING_DIRECTORY "${ARROW_PREFIX}/"
    )

    ExternalProject_Add_Step(arrow_ep copy_gandiva_binary_400_0_0
                      COMMAND cp -a ${ARROW_PREFIX}/lib/${CMAKE_SHARED_LIBRARY_PREFIX}${GANDIVA_LIB_NAME}${ARROW_SHARED_LIBRARY_SUFFIX} ${root_directory}/releases/
                      COMMENT "Copy libgandiva.so.400.0.0 to releases/"
                      DEPENDEES mkdir download update patch configure build install java_install
                      WORKING_DIRECTORY "${ARROW_PREFIX}/"
    )


    # Copy Arrow Headers to releases/include
    ExternalProject_Add_Step(arrow_ep copy_arrow_header
                      COMMAND cp -rf ${ARROW_PREFIX}/include/ ${root_directory}/releases/
                      COMMENT "Arrow Header to releases/include"
                      DEPENDEES mkdir download update patch configure build install java_install
                      WORKING_DIRECTORY "${ARROW_PREFIX}/"
    )

    # Set up Arrow Shared Library Directory
    set(
      ARROW_SHARED_LIB
      "${root_directory}/releases/${CMAKE_SHARED_LIBRARY_PREFIX}${ARROW_LIB_NAME}${ARROW_SHARED_LIBRARY_SUFFIX}"
      )
    add_library(Arrow::arrow SHARED IMPORTED)
    set_target_properties(Arrow::arrow
                          PROPERTIES IMPORTED_LOCATION "${ARROW_SHARED_LIB}"
                                     INTERFACE_INCLUDE_DIRECTORIES
                                      "${root_directory}/releases/include")
    add_dependencies(Arrow::arrow arrow_ep)

    # Set up Gandiva Shared Library Directory
    set(
      GANDIVA_SHARED_LIB
      "${root_directory}/releases/${CMAKE_SHARED_LIBRARY_PREFIX}${GANDIVA_LIB_NAME}${ARROW_SHARED_LIBRARY_SUFFIX}"
      )
    add_library(Arrow::gandiva SHARED IMPORTED)
    set_target_properties(Arrow::gandiva
                          PROPERTIES IMPORTED_LOCATION "${GANDIVA_SHARED_LIB}"
                                     INTERFACE_INCLUDE_DIRECTORIES
                                      "${root_directory}/releases/include")
    add_dependencies(Arrow::gandiva arrow_ep)

    target_link_libraries(spark_columnar_jni
                      LINK_PUBLIC Arrow::arrow Arrow::gandiva)
  endif()
endmacro()

# Find the existing Arrow library by using ARROW_RROT path
macro(find_arrow)
  set(ARROW_BFS_LIB_DIR "${ARROW_BFS_INSTALL_DIR}/lib")
  set(ARROW_LIB_DIR "${ARROW_ROOT}/lib")
  set(ARROW_LIB64_DIR "${ARROW_ROOT}/lib64")
  message(STATUS "Set Arrow Library Directory in ${ARROW_BFS_LIB_DIR} or ${ARROW_LIB_DIR} or ${ARROW_LIB64_DIR}")
  set(ARROW_BFS_INCLUDE_DIR "${ARROW_BFS_INSTALL_DIR}/include")
  set(ARROW_INCLUDE_DIR "${ARROW_ROOT}/include")
  message(STATUS "Set Arrow Include Directory in ${ARROW_BFS_INCLUDE_DIR} or ${ARROW_INCLUDE_DIR}")
  set(CMAKE_FIND_LIBRARY_SUFFIXES ".so")
  find_library(ARROW_LIB NAMES ${CMAKE_SHARED_LIBRARY_PREFIX}${ARROW_LIB_NAME}${ARROW_SHARED_LIBRARY_SUFFIX} PATHS ${ARROW_BFS_LIB_DIR} ${ARROW_LIB_DIR} ${ARROW_LIB64_DIR} NO_DEFAULT_PATH)
  if(NOT ARROW_LIB)
    message(FATAL_ERROR "Arrow Library Not Found")
  else()
    message(STATUS "Arrow Library Can Be Found in ${ARROW_LIB}")
  endif()

  find_library(GANDIVA_LIB NAMES ${CMAKE_SHARED_LIBRARY_PREFIX}${GANDIVA_LIB_NAME}${ARROW_SHARED_LIBRARY_SUFFIX} PATHS ${ARROW_BFS_LIB_DIR} ${ARROW_LIB_DIR} ${ARROW_LIB64_DIR} NO_DEFAULT_PATH)
  if(NOT GANDIVA_LIB)
    message(FATAL_ERROR "Gandiva Library Not Found")
  else()
    message(STATUS "Gandiva Library Can Be Found in ${GANDIVA_LIB}")
  endif()

  file(COPY ${ARROW_LIB} DESTINATION ${root_directory}/releases/ FOLLOW_SYMLINK_CHAIN)
  file(COPY ${GANDIVA_LIB} DESTINATION ${root_directory}/releases/ FOLLOW_SYMLINK_CHAIN)

  if(EXISTS ${ARROW_BFS_INCLUDE_DIR})
    message(STATUS "COPY and Set Arrow Header to: ${ARROW_BFS_INCLUDE_DIR}")
    file(COPY ${ARROW_BFS_INCLUDE_DIR}/arrow DESTINATION ${root_directory}/releases/include)
    file(COPY ${ARROW_BFS_INCLUDE_DIR}/gandiva DESTINATION ${root_directory}/releases/include)
    file(COPY ${ARROW_BFS_INCLUDE_DIR}/parquet DESTINATION ${root_directory}/releases/include)
  else()
    message(STATUS "COPY and Set Arrow Header to: ${ARROW_INCLUDE_DIR}")
    file(COPY ${ARROW_INCLUDE_DIR}/arrow DESTINATION ${root_directory}/releases/include)
    file(COPY ${ARROW_INCLUDE_DIR}/gandiva DESTINATION ${root_directory}/releases/include)
    file(COPY ${ARROW_INCLUDE_DIR}/parquet DESTINATION ${root_directory}/releases/include)
  endif()

  # Set up Arrow Shared Library Directory
  set(
    ARROW_SHARED_LIB
    "${root_directory}/releases/${CMAKE_SHARED_LIBRARY_PREFIX}${ARROW_LIB_NAME}${ARROW_SHARED_LIBRARY_SUFFIX}"
    )

  add_library(Arrow::arrow SHARED IMPORTED)
  set_target_properties(Arrow::arrow
                        PROPERTIES IMPORTED_LOCATION "${ARROW_SHARED_LIB}"
                                   INTERFACE_INCLUDE_DIRECTORIES
                                    "${root_directory}/releases/include")

  # Set up Gandiva Shared Library Directory
  set(
    GANDIVA_SHARED_LIB
    "${root_directory}/releases/${CMAKE_SHARED_LIBRARY_PREFIX}${GANDIVA_LIB_NAME}${ARROW_SHARED_LIBRARY_SUFFIX}"
    )
  add_library(Arrow::gandiva SHARED IMPORTED)
  set_target_properties(Arrow::gandiva
                        PROPERTIES IMPORTED_LOCATION "${GANDIVA_SHARED_LIB}"
                                   INTERFACE_INCLUDE_DIRECTORIES
                                    "${root_directory}/releases/include")

  target_link_libraries(spark_columnar_jni
                    LINK_PUBLIC Arrow::arrow Arrow::gandiva)

endmacro()

# Building Jemalloc
macro(build_jemalloc)
  message(STATUS "Building Jemalloc from Source")
  set (JEMALLOC_SOURCE_URL
       "https://github.com/jemalloc/jemalloc/releases/download/${JEMALLOC_BUILD_VERSION}/jemalloc-${JEMALLOC_BUILD_VERSION}.tar.bz2"
       "https://github.com/ursa-labs/thirdparty/releases/download/latest/jemalloc-${JEMALLOC_BUILD_VERSION}.tar.bz2"
  )
  set(JEMALLOC_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/jemalloc_ep-install")
  set(JEMALLOC_LIB_DIR "${JEMALLOC_PREFIX}/lib")
  set(JEMALLOC_INCLUDE_DIR "${JEMALLOC_PREFIX}/include")
  set(
    JEMALLOC_STATIC_LIB
    "${JEMALLOC_LIB_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}jemalloc_pic${CMAKE_STATIC_LIBRARY_SUFFIX}"
    )
  set(
    JEMALLOC_INCLUDE
    "${JEMALLOC_PREFIX}/include"
    )
  set(JEMALLOC_CONFIGURE_ARGS
      "AR=${CMAKE_AR}"
      "CC=${CMAKE_C_COMPILER}"
      "--prefix=${JEMALLOC_PREFIX}"
      "--libdir=${JEMALLOC_LIB_DIR}"
      "--with-jemalloc-prefix=je_gazelle_"
      "--with-private-namespace=je_gazelle_private_"
      "--without-export"
      "--disable-shared"
      "--disable-cxx"
      "--disable-libdl"
      "--disable-initial-exec-tls"
      "CFLAGS=-fPIC"
      "CXXFLAGS=-fPIC")
  set(JEMALLOC_BUILD_COMMAND ${MAKE} ${MAKE_BUILD_ARGS})
  ExternalProject_Add(jemalloc_ep
                      URL ${JEMALLOC_SOURCE_URL}
                      PATCH_COMMAND touch doc/jemalloc.3 doc/jemalloc.html
                      CONFIGURE_COMMAND "./configure" ${JEMALLOC_CONFIGURE_ARGS}
                      BUILD_COMMAND ${JEMALLOC_BUILD_COMMAND}
                      BUILD_IN_SOURCE 1
                      BUILD_BYPRODUCTS "${JEMALLOC_STATIC_LIB}"
                      INSTALL_COMMAND make install)

  file(MAKE_DIRECTORY "${JEMALLOC_INCLUDE_DIR}")
  add_library(jemalloc::libjemalloc STATIC IMPORTED)
  set_target_properties(
    jemalloc::libjemalloc
    PROPERTIES INTERFACE_LINK_LIBRARIES Threads::Threads
               IMPORTED_LOCATION "${JEMALLOC_STATIC_LIB}"
               INTERFACE_INCLUDE_DIRECTORIES
               "${JEMALLOC_INCLUDE_DIR}")
  add_dependencies(jemalloc::libjemalloc protobuf_ep)
endmacro()

# Find Jemalloc
macro(find_jemalloc)
  # Find the existing Protobuf
  set(CMAKE_FIND_LIBRARY_SUFFIXES ".a")
  find_package(jemalloc_pic)
  if ("${Jemalloc_LIBRARY}" STREQUAL "Jemalloc_LIBRARY-NOTFOUND")
    message(FATAL_ERROR "Jemalloc Library Not Found")
  endif()
  set(PROTOC_BIN ${Jemalloc_PROTOC_EXECUTABLE})
endmacro()

# Find Folly
macro(find_folly)
  # Find the folly library
  set(CMAKE_FIND_LIBRARY_SUFFIXES ".a")
  find_library(FOLLY_LIBRARY NAMES folly PATHS ${SYSTEM_LIB_PATH} ${SYSTEM_LIB64_PATH} ${SYSTEM_LOCAL_LIB_PATH} ${SYSTEM_LOCAL_LIB64_PATH} NO_DEFAULT_PATH)
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
  find_library(IBERTY_LIBRARY NAMES iberty PATHS ${SYSTEM_LIB_PATH} ${SYSTEM_LIB64_PATH} ${SYSTEM_LOCAL_LIB_PATH} ${SYSTEM_LOCAL_LIB64_PATH} NO_DEFAULT_PATH)
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
  find_library(GLOG_LIBRARY NAMES glog PATHS ${SYSTEM_LIB_PATH} ${SYSTEM_LIB64_PATH} ${SYSTEM_LOCAL_LIB_PATH} ${SYSTEM_LOCAL_LIB64_PATH} NO_DEFAULT_PATH)
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
  find_library(DOUBLE_CONVERSION_LIBRARY NAMES double-conversion PATHS ${SYSTEM_LIB_PATH} ${SYSTEM_LIB64_PATH} ${SYSTEM_LOCAL_LIB_PATH} ${SYSTEM_LOCAL_LIB64_PATH} NO_DEFAULT_PATH)
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
  find_library(FMT_LIBRARY NAMES fmt PATHS ${SYSTEM_LIB_PATH} ${SYSTEM_LIB64_PATH} ${SYSTEM_LOCAL_LIB_PATH} ${SYSTEM_LOCAL_LIB64_PATH} NO_DEFAULT_PATH)
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
  set(CMAKE_FIND_LIBRARY_SUFFIXES ".so")
  find_library(GTEST_LIBRARY NAMES gtest PATHS ${SYSTEM_LIB_PATH} ${SYSTEM_LIB64_PATH} ${SYSTEM_LOCAL_LIB_PATH} ${SYSTEM_LOCAL_LIB64_PATH} NO_DEFAULT_PATH)
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
  find_library(SNAPPY_LIBRARY NAMES snappy PATHS ${SYSTEM_LIB_PATH} ${SYSTEM_LIB64_PATH} ${SYSTEM_LOCAL_LIB_PATH} ${SYSTEM_LOCAL_LIB64_PATH} NO_DEFAULT_PATH)
  if (NOT SNAPPY_LIBRARY)
    message(FATAL_ERROR "Snappy Library Not Found")
  else()
    message(STATUS "Snappy Library Can Be Found in ${SNAPPY_LIBRARY}")
  endif()
  set(SNAPPY_LIB_PATH "${SNAPPY_LIBRARY}")
endmacro()

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

  target_link_libraries(spark_columnar_jni
                    LINK_PUBLIC facebook::velox::functions::prestosql::agg
                    LINK_PUBLIC facebook::velox::exec
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
                    LINK_PUBLIC facebook::velox::common::process
                    LINK_PUBLIC facebook::velox::common::base::exception
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
                    LINK_PUBLIC gtest
                    LINK_PUBLIC folly
                    LINK_PUBLIC iberty
                    LINK_PUBLIC doubleconversion
                    LINK_PUBLIC snappy
                    LINK_PUBLIC glog
                    LINK_PUBLIC fmt)
endmacro()

# Set up Proto
set(PROTO_OUTPUT_DIR "${CMAKE_CURRENT_BINARY_DIR}/proto")
file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/proto)
file(GLOB PROTO_FILES ${proto_directory}/*.proto
                      ${substrait_proto_directory}/*.proto
                      ${substrait_proto_directory}/extensions/*.proto)
FOREACH(PROTO ${PROTO_FILES})
  file(RELATIVE_PATH REL_PROTO ${proto_directory} ${PROTO})
  string(REGEX REPLACE "\\.proto" "" PROTO_NAME ${REL_PROTO})
  LIST(APPEND PROTO_SRCS "${PROTO_OUTPUT_DIR}/${PROTO_NAME}.pb.cc")
  LIST(APPEND PROTO_HDRS "${PROTO_OUTPUT_DIR}/${PROTO_NAME}.pb.h")
ENDFOREACH()
set(PROTO_OUTPUT_FILES ${PROTO_HDRS} ${PROTO_SRCS})
set_source_files_properties(${PROTO_OUTPUT_FILES} PROPERTIES GENERATED TRUE)

get_filename_component(PROTO_DIR ${proto_directory}/,
                       DIRECTORY)

set(CODEGEN_HEADERS
    third_party/
    )
set(THIRD_PARTY_INCLUDE ${CMAKE_CURRENT_SOURCE_DIR}/third_party)
include_directories(${CMAKE_CURRENT_SOURCE_DIR} ${THIRD_PARTY_INCLUDE} ${VELOX_HOME} ${VELOX_REALEASE_PATH})
file(MAKE_DIRECTORY ${root_directory}/releases/include)
file(MAKE_DIRECTORY ${root_directory}/releases/include/codegen/common/)
file(MAKE_DIRECTORY ${root_directory}/releases/include/codegen/third_party/)
file(MAKE_DIRECTORY ${root_directory}/releases/include/codegen/precompile/)
file(MAKE_DIRECTORY ${root_directory}/releases/include/codegen/utils/)
file(MAKE_DIRECTORY ${root_directory}/releases/include/codegen/arrow_compute/ext/)
file(COPY utils/result_iterator.h DESTINATION ${root_directory}/releases/include/common/)

add_definitions(-DNATIVESQL_SRC_PATH="${root_directory}/releases")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-deprecated-declarations -Wno-attributes")
set(SPARK_COLUMNAR_PLUGIN_SRCS
        jni/jni_wrapper.cc
        ${PROTO_SRCS}
        compute/protobuf_utils.cc
        compute/substrait_utils.cc
        compute/substrait_to_velox_expr.cc
        compute/substrait_to_velox_plan.cc
        operators/c2r/columnar_to_row_converter.cc
        operators/c2r/velox_to_row_converter.cc
        operators/c2r/conversion_utils.cc
        operators/shuffle/splitter.cc
        operators/bridge/velox_bridge.cc
        )

file(MAKE_DIRECTORY ${root_directory}/releases)
add_library(spark_columnar_jni SHARED ${SPARK_COLUMNAR_PLUGIN_SRCS})
add_dependencies(spark_columnar_jni jni_proto)

if(BUILD_PROTOBUF)
  build_protobuf()
  message(STATUS "Building ProtoBuf from Source: ${BUILD_PROTOBUF}")
  target_link_libraries(spark_columnar_jni
                        LINK_PRIVATE protobuf::libprotobuf)
else()
  find_protobuf()
  message(STATUS "Use existing ProtoBuf libraries: ${PROTOBUF_LIBRARY}")
  target_link_libraries(spark_columnar_jni
                        LINK_PUBLIC ${PROTOBUF_LIBRARY})
endif()

add_custom_command(OUTPUT ${PROTO_OUTPUT_FILES}
                   COMMAND ${PROTOC_BIN}
                           --proto_path
                           ${proto_directory}/
                           --cpp_out
                           ${PROTO_OUTPUT_DIR}
                           ${PROTO_FILES}
                   DEPENDS  ${PROTO_DIR}
                   COMMENT "Running PROTO compiler"
                   VERBATIM)
add_custom_target(jni_proto ALL DEPENDS ${PROTO_OUTPUT_FILES})
add_dependencies(jni_proto protobuf::libprotobuf)
target_include_directories(spark_columnar_jni PUBLIC ${CMAKE_SYSTEM_INCLUDE_PATH} ${JNI_INCLUDE_DIRS} ${source_root_directory} ${PROTO_OUTPUT_DIR} ${PROTOBUF_INCLUDE})
set_target_properties(spark_columnar_jni PROPERTIES
                      LIBRARY_OUTPUT_DIRECTORY ${root_directory}/releases
)

# Build Velox
find_folly()
find_iberty()
find_glog()
find_double_conversion()
find_fmt()
find_gtest()
find_snappy()
build_velox_exec()

# Build Arrow
#message(STATUS "Building ARROW from Source: ${BUILD_ARROW}")
if(BUILD_ARROW)
  build_arrow(${STATIC_ARROW})
  message(STATUS "Building Static ARROW: ${STATIC_ARROW}")
else() #
  find_arrow()
  message(STATUS "Use existing ARROW libraries")
endif()

# Build Jemalloc
if(BUILD_JEMALLOC)
  build_jemalloc(${STATIC_JEMALLOC})
  message(STATUS "Building Jemalloc: ${STATIC_JEMALLOC}")
else() #
  find_jemalloc()
  message(STATUS "Use existing Jemalloc libraries")
endif()

if(DEFINED ENV{HADOOP_HOME})
  set(LIBHDFS3_DESTINATION $ENV{HADOOP_HOME}/lib/native)
else()
  set(LIBHDFS3_DESTINATION ${CMAKE_INSTALL_LIBDIR})
endif()

install(TARGETS spark_columnar_jni
        DESTINATION ${CMAKE_INSTALL_LIBDIR})
install(FILES ${source_root_directory}/resources/libhdfs.so
        DESTINATION ${LIBHDFS3_DESTINATION})
install(FILES ${source_root_directory}/resources/libprotobuf.so.13
        DESTINATION ${CMAKE_INSTALL_LIBDIR})

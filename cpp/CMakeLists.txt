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
message(STATUS "Building using CMake version: ${CMAKE_VERSION}")

# The set(CACHE) command does not remove any normal variable of the same name from the current scope
# https://cmake.org/cmake/help/latest/policy/CMP0126.html
if(POLICY CMP0126)
  cmake_policy(SET CMP0126 NEW)
endif()

if (CMAKE_VERSION VERSION_GREATER_EQUAL "3.24.0")
  cmake_policy(SET CMP0135 NEW)
endif()

if(NOT DEFINED CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE
      Release
      CACHE STRING "Choose the type of build to Release.")
endif()

set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/CMake" ${CMAKE_MODULE_PATH})
set(CMAKE_EXPORT_COMPILE_COMMANDS ON CACHE INTERNAL "")

project(gluten)

option(BUILD_VELOX_BACKEND "Build Velox backend" ON)
option(BUILD_TESTS "Build Tests" OFF)
option(BUILD_EXAMPLES "Build Examples" OFF)
option(BUILD_BENCHMARKS "Build Benchmarks" OFF)
option(BUILD_PROTOBUF "Build Protobuf from Source" OFF)
option(BUILD_JEMALLOC "Build Jemalloc from Source" OFF)
option(USE_AVX512 "Build with AVX-512 optimizations" OFF)
option(ENABLE_HBM "Enable HBM allocator" OFF)
option(ENABLE_QAT "Enable QAT for de/compression" OFF)
option(ENABLE_IAA "Enable IAA for de/compression" OFF)
option(ENABLE_S3 "Enable S3" OFF)
option(ENABLE_HDFS "Enable HDFS" OFF)

set(root_directory ${PROJECT_BINARY_DIR})
get_filename_component(GLUTEN_HOME ${CMAKE_SOURCE_DIR} DIRECTORY)

if (NOT DEFINED ARROW_HOME)
  if (${CMAKE_BUILD_TYPE} STREQUAL "Debug")
    set(ARROW_HOME ${GLUTEN_HOME}/ep/build-velox/build/velox_ep/_build/debug/third_party/arrow_ep)
  else()
    set(ARROW_HOME ${GLUTEN_HOME}/ep/build-velox/build/velox_ep/_build/release/third_party/arrow_ep)
  endif()
endif()

#
# Compiler flags
#

if (${CMAKE_BUILD_TYPE} STREQUAL "RelWithDebInfo")
# keep this original logic
  add_definitions(-DGLUTEN_PRINT_DEBUG)
  message(STATUS "Add definition GLUTEN_PRINT_DEBUG")
elseif (${CMAKE_BUILD_TYPE} STREQUAL "Debug")
  add_definitions(-DGLUTEN_PRINT_DEBUG)
  message(STATUS "Add definition GLUTEN_PRINT_DEBUG")
  set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -ggdb -O0")
  message(STATUS "CMAKE_CXX_FLAGS_DEBUG=${CMAKE_CXX_FLAGS_DEBUG}")
else ()
  add_definitions(-DNDEBUG)
  message(STATUS "Add definition NDEBUG")
endif()

add_compile_options(-Wall)
add_compile_options(-Wno-sign-compare)
add_compile_options(-Wno-comment)

add_compile_options(-Werror)
add_compile_options(-Wno-error=parentheses)
add_compile_options(-Wno-error=unused-function)
add_compile_options(-Wno-error=unused-variable)

if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
  add_compile_options(-Wno-error=unused-but-set-variable)
  if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS 11)
    add_compile_options(-Wno-error=maybe-uninitialized)
  endif()
elseif ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
  # Experimental
  add_compile_options(-Wno-implicit-int-float-conversion)
  add_compile_options(-Wno-nullability-completeness)
  add_compile_options(-Wno-mismatched-tags)
else()
  message("Unsupported compiler ID: ${CMAKE_CXX_COMPILER_ID}")
endif()

# see https://issues.apache.org/jira/browse/ARROW-4665
if (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
  add_compile_options(-Wno-macro-redefined)
  add_compile_options(-Wno-nullability-completeness)
  add_compile_options(-Wno-pessimizing-move)
endif()

#
# Dependencies
#

include(ConfigArrow)

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

find_package(JNI REQUIRED)

if(BUILD_TESTS)
  set(GLUTEN_GTEST_MIN_VERSION "1.13.0")
  find_package(GTest ${GLUTEN_GTEST_MIN_VERSION} CONFIG)
  if(NOT GTest_FOUND)
    include(BuildGTest)
  endif()
  include(GoogleTest)
  enable_testing()
  find_package(glog REQUIRED)
endif()

function(ADD_TEST_CASE TEST_NAME)
  set(options)
  set(one_value_args)
  set(multi_value_args
      SOURCES
      EXTRA_LINK_LIBS
      EXTRA_INCLUDES
      EXTRA_DEPENDENCIES)

  cmake_parse_arguments(ARG
      "${options}"
      "${one_value_args}"
      "${multi_value_args}"
      ${ARGN})

  if(ARG_SOURCES)
    set(SOURCES ${ARG_SOURCES})
  else()
    message(FATAL_ERROR "No sources specified for test ${TEST_NAME}")
  endif()

  add_executable(${TEST_NAME} ${SOURCES})
  target_link_libraries(${TEST_NAME} gluten GTest::gtest GTest::gtest_main Threads::Threads)
  target_include_directories(${TEST_NAME} PRIVATE ${CMAKE_SOURCE_DIR}/core)

  if(ARG_EXTRA_LINK_LIBS)
    target_link_libraries(${TEST_NAME} ${ARG_EXTRA_LINK_LIBS})
  endif()

  if(ARG_EXTRA_INCLUDES)
    target_include_directories(${TEST_NAME} SYSTEM PUBLIC ${ARG_EXTRA_INCLUDES})
  endif()

  if(ARG_EXTRA_DEPENDENCIES)
    add_dependencies(${TEST_NAME} ${ARG_EXTRA_DEPENDENCIES})
  endif()

  gtest_discover_tests(${TEST_NAME})
endfunction()

if(BUILD_TESTS OR BUILD_BENCHMARKS)
  set(GLUTEN_GBENCHMARKS_MIN_VERSION "1.6.0")
  find_package(benchmark ${GLUTEN_GBENCHMARK_MIN_VERSION} CONFIG)
  if(NOT benchmark_FOUND)
    include(BuildGoogleBenchmark)
  endif()
endif()

if(ENABLE_QAT)
  add_definitions(-DGLUTEN_ENABLE_QAT)
endif()

if(ENABLE_IAA)
  add_definitions(-DGLUTEN_ENABLE_IAA)
endif()

#
# Subdirectories
#

add_subdirectory(core)

if(BUILD_VELOX_BACKEND)
  add_subdirectory(velox)
endif()

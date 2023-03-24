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

include(ExternalProject)

macro(build_benchmark)
  message(STATUS "Building benchmark from source")
  if(CMAKE_VERSION VERSION_LESS 3.6)
    message(FATAL_ERROR "Building gbenchmark from source requires at least CMake 3.6")
  endif()

  if(DEFINED ENV{GLUTEN_GBENCHMARK_URL})
    set(GBENCHMARK_SOURCE_URL "$ENV{GLUTEN_GBENCHMARK_URL}")
  else()
    set(GBENCHMARK_BUILD_VERSION "v1.6.0")
    set(GBENCHMARK_SOURCE_URL
            "https://github.com/google/benchmark/archive/refs/tags/${GBENCHMARK_BUILD_VERSION}.tar.gz"
            "https://github.com/ursa-labs/thirdparty/releases/download/latest/gbenchmark-${GBENCHMARK_BUILD_VERSION}.tar.gz")
  endif()
  set(GBENCHMARK_BUILD_SHA256_CHECKSUM "1f71c72ce08d2c1310011ea6436b31e39ccab8c2db94186d26657d41747c85d6")


  set(GBENCHMARK_PREFIX
      "${CMAKE_CURRENT_BINARY_DIR}/gbenchmark_ep/src/gbenchmark_ep-install")
  set(GBENCHMARK_INCLUDE_DIR "${GBENCHMARK_PREFIX}/include")
  set(GBENCHMARK_STATIC_LIB
      "${GBENCHMARK_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}benchmark${CMAKE_STATIC_LIBRARY_SUFFIX}"
      )
  set(GBENCHMARK_MAIN_STATIC_LIB
      "${GBENCHMARK_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}benchmark_main${CMAKE_STATIC_LIBRARY_SUFFIX}"
      )
  set(GBENCHMARK_CMAKE_ARGS
      "-DCMAKE_INSTALL_PREFIX=${GBENCHMARK_PREFIX}"
      -DCMAKE_INSTALL_LIBDIR=lib
      -DBENCHMARK_ENABLE_TESTING=OFF
      -DCMAKE_CXX_FLAGS="-fPIC"
      -DCMAKE_BUILD_TYPE=Release)

  ExternalProject_Add(gbenchmark_ep
      URL ${GBENCHMARK_SOURCE_URL}
      URL_HASH "SHA256=${GBENCHMARK_BUILD_SHA256_CHECKSUM}"
      BUILD_BYPRODUCTS "${GBENCHMARK_STATIC_LIB}"
      "${GBENCHMARK_MAIN_STATIC_LIB}"
      CMAKE_ARGS ${GBENCHMARK_CMAKE_ARGS})

  # The include directory must exist before it is referenced by a target.
  file(MAKE_DIRECTORY "${GBENCHMARK_INCLUDE_DIR}")

  add_library(benchmark::benchmark STATIC IMPORTED)
  set_target_properties(benchmark::benchmark
      PROPERTIES IMPORTED_LOCATION "${GBENCHMARK_STATIC_LIB}"
      INTERFACE_INCLUDE_DIRECTORIES
      "${GBENCHMARK_INCLUDE_DIR}")

  add_library(benchmark::benchmark_main STATIC IMPORTED)
  set_target_properties(benchmark::benchmark_main
      PROPERTIES IMPORTED_LOCATION "${GBENCHMARK_MAIN_STATIC_LIB}"
      INTERFACE_INCLUDE_DIRECTORIES
      "${GBENCHMARK_INCLUDE_DIR}")

  add_dependencies(benchmark::benchmark gbenchmark_ep)
  add_dependencies(benchmark::benchmark_main gbenchmark_ep)
endmacro()

build_benchmark()

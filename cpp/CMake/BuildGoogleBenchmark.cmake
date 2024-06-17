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

include_guard(GLOBAL)
include(FetchContent)

set(GLUTEN_GBENCHMARK_BUILD_VERSION "v1.6.0")
set(GLUTEN_GBENCHMARK_SOURCE_URL
    "https://github.com/google/benchmark/archive/refs/tags/${GLUTEN_GBENCHMARK_BUILD_VERSION}.tar.gz"
    "https://github.com/ursa-labs/thirdparty/releases/download/latest/gbenchmark-${GLUTEN_GBENCHMARK_BUILD_VERSION}.tar.gz"
)
set(GLUTEN_GBENCHMARK_BUILD_SHA256_CHECKSUM
    "1f71c72ce08d2c1310011ea6436b31e39ccab8c2db94186d26657d41747c85d6")

resolve_dependency_url(GBENCHMARK)

set(GBENCHMARK_CMAKE_ARGS "-fPIC -w")

message(STATUS "Building google benchmark from source")
FetchContent_Declare(
  gbenchmark
  URL ${GLUTEN_GBENCHMARK_SOURCE_URL}
  URL_HASH "${GLUTEN_GBENCHMARK_BUILD_SHA256_CHECKSUM}")

if(NOT gbenchmark_POPULATED)
  # We don't want to build tests.
  set(BENCHMARK_ENABLE_TESTING
      OFF
      CACHE BOOL "Disable google benchmark tests" FORCE)
  set(CMAKE_CXX_FLAGS_BKP "${CMAKE_CXX_FLAGS}")
  string(APPEND CMAKE_CXX_FLAGS " ${GBENCHMARK_CMAKE_ARGS}")
  set(CMAKE_BUILD_TYPE_BKP "${CMAKE_BUILD_TYPE}")
  set(CMAKE_BUILD_TYPE "Release")

  # Fetch the content using previously declared details
  FetchContent_Populate(gbenchmark)

  add_subdirectory(${gbenchmark_SOURCE_DIR} ${gbenchmark_BINARY_DIR})
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS_BKP}")
  set(CMAKE_BUILD_TYPE "${CMAKE_BUILD_TYPE_BKP}")
endif()

FetchContent_MakeAvailable(gbenchmark)

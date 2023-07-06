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

message(STATUS "Building zstd from source")
set(ZSTD_SOURCE_URL "https://github.com/facebook/zstd/releases/download/v1.5.5/zstd-1.5.5.tar.gz")
set(ZSTD_BUILD_SHA256_CHECKSUM "9c4396cc829cfae319a6e2615202e82aad41372073482fce286fac78646d3ee4")
set(ZSTD_LIB_NAME "zstd")

set(ZSTD_PREFIX
    "${CMAKE_CURRENT_BINARY_DIR}/zstd_ep-install")
set(ZSTD_SOURCE_DIR "${ZSTD_PREFIX}/src/zstd_ep")
set(ZSTD_INCLUDE_DIR "${ZSTD_SOURCE_DIR}/lib")
set(ZSTD_STATIC_LIB_NAME "${CMAKE_STATIC_LIBRARY_PREFIX}${ZSTD_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}")
set(ZSTD_STATIC_LIB "${ZSTD_SOURCE_DIR}/lib/${ZSTD_STATIC_LIB_NAME}")

ExternalProject_Add(qatzstd_zstd_ep
    PREFIX ${ZSTD_PREFIX}
    URL ${ZSTD_SOURCE_URL}
    URL_HASH "SHA256=${ZSTD_BUILD_SHA256_CHECKSUM}"
    SOURCE_DIR ${ZSTD_SOURCE_DIR}
    CONFIGURE_COMMAND ""
    BUILD_COMMAND env CFLAGS=-fPIC CXXFLAGS=-fPIC $(MAKE)
    INSTALL_COMMAND ""
    BUILD_BYPRODUCTS ${ZSTD_STATIC_LIB}
    BUILD_IN_SOURCE 1)

# The include directory must exist before it is referenced by a target.
file(MAKE_DIRECTORY "${ZSTD_INCLUDE_DIR}")

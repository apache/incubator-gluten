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

# ZSTD_HOME environmental variable is used to check for Zstd headers and static library

# ZSTD_INCLUDE_DIR: directory containing headers
# ZSTD_LIBRARY: path to libzstd.so
# ZSTD_FOUND: whether zstd has been found

if (NOT "$ENV{ZSTD_HOME}" STREQUAL "")
  file(TO_CMAKE_PATH "$ENV{ZSTD_HOME}" _zstd_path)
  message(STATUS "ZSTD_HOME: ${_zstd_path}")
else()
  set(_zstd_path "/usr/local")
endif()

find_path(ZSTD_INCLUDE_DIR zstd.h HINTS
    ${_zstd_path}
    PATH_SUFFIXES "include")

find_library (ZSTD_LIBRARY NAMES zstd HINTS
    ${_zstd_path}
    PATH_SUFFIXES "lib")

if (ZSTD_INCLUDE_DIR AND ZSTD_LIBRARY)
  set(ZSTD_FOUND TRUE)
  set(ZSTD_HEADER_NAME zstd.h)
  set(ZSTD_HEADER ${ZSTD_INCLUDE_DIR}/${ZSTD_HEADER_NAME})
else ()
  set(ZSTD_FOUND FALSE)
endif ()

if (ZSTD_FOUND)
  message(STATUS "Found the zstd header: ${ZSTD_HEADER}")
  message(STATUS "Found the zstd static library: ${ZSTD_LIBRARY}")
else ()
  message(FATAL_ERROR ZSTD_ERR_MSG "Could not find zstd. Looked in ${_zstd_path}.")
endif ()

mark_as_advanced(
    ZSTD_INCLUDE_DIR
    ZSTD_LIBRARY)


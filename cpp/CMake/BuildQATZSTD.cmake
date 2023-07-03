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

macro(build_qatzstd)
  message(STATUS "Building QAT-ZSTD from source")
  set(QATZSTD_BUILD_VERSION "v0.0.1")
  set(QATZSTD_BUILD_SHA256_CHECKSUM "c15aa561d5139d49f896315cbd38d02a51b636e448abfa58e4d3a011fe0424f2")
  set(QATZSTD_SOURCE_URL
      "https://github.com/intel/QAT-ZSTD-Plugin/archive/refs/tags/${QATZSTD_BUILD_VERSION}.tar.gz")
#  set(QATZSTD_SHARED_LIB_MAJOR_VERSION ".3")
#  set(QATZSTD_SHARED_LIB_FULL_VERSION ".3.0.1")
  set(QATZSTD_LIB_NAME "qatseqprod")

  set(QATZSTD_PREFIX
      "${CMAKE_CURRENT_BINARY_DIR}/qatzstd_ep-install")
  set(QATZSTD_SOURCE_DIR "${QATZSTD_PREFIX}/src/qatzstd_ep")
  set(QATZSTD_INCLUDE_DIR "${QATZSTD_SOURCE_DIR}/src")
  set(QATZSTD_STATIC_LIB_NAME "${CMAKE_STATIC_LIBRARY_PREFIX}${QATZSTD_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}")
  set(QATZSTD_STATIC_LIB_TARGETS "${QATZSTD_SOURCE_DIR}/src/${QATZSTD_STATIC_LIB_NAME}")
#  set(QATZSTD_SHARED_LIB_NAME "${CMAKE_SHARED_LIBRARY_PREFIX}${QATZSTD_LIB_NAME}${CMAKE_SHARED_LIBRARY_SUFFIX}${QATZSTD_SHARED_LIB_MAJOR_VERSION}")
#  set(QATZSTD_SHARED_LIB_FULL_NAME "${CMAKE_SHARED_LIBRARY_PREFIX}${QATZSTD_LIB_NAME}${CMAKE_SHARED_LIBRARY_SUFFIX}${QATZSTD_SHARED_LIB_FULL_VERSION}")
#  set(QATZSTD_SHARED_LIB_TARGETS
#      "${QATZSTD_SOURCE_DIR}/src/${QATZSTD_SHARED_LIB_NAME}"
#      "${QATZSTD_SOURCE_DIR}/src/${QATZSTD_SHARED_LIB_FULL_NAME}"
#      )
  set(QATZSTD_MAKE_ARGS "ENABLE_USDM_DRV=1")

  ExternalProject_Add(qatzstd_ep
      PREFIX ${QATZSTD_PREFIX}
      URL ${QATZSTD_SOURCE_URL}
      URL_HASH "SHA256=${QATZSTD_BUILD_SHA256_CHECKSUM}"
      SOURCE_DIR ${QATZSTD_SOURCE_DIR}
      CONFIGURE_COMMAND ""
      BUILD_COMMAND $(MAKE) ${QATZSTD_MAKE_ARGS}
      INSTALL_COMMAND ""
      BUILD_BYPRODUCTS ${QATZSTD_STATIC_LIB_TARGETS}
      BUILD_IN_SOURCE 1)

  ExternalProject_Add_Step(qatzstd_ep post-build
      COMMAND cp -P ${QATZSTD_STATIC_LIB_TARGETS} ${root_directory}/releases/
      DEPENDEES build
      DEPENDERS install
      WORKING_DIRECTORY ${QATZSTD_SOURCE_DIR})

  # The include directory must exist before it is referenced by a target.
  file(MAKE_DIRECTORY "${QATZSTD_INCLUDE_DIR}")

  # Find ZSTD
  include(FindZstd)
  set(QATZSTD_INCLUDE_DIRS
      "${QATZSTD_INCLUDE_DIR}"
      "${ZSTD_INCLUDE_DIR}")

  add_library(qatzstd::qatzstd STATIC IMPORTED)
  set_target_properties(qatzstd::qatzstd
      PROPERTIES IMPORTED_LOCATION
      "${root_directory}/releases/${QATZSTD_STATIC_LIB_NAME}"
      INTERFACE_INCLUDE_DIRECTORIES
      "${QATZSTD_INCLUDE_DIRS}"
      INTERFACE_LINK_LIBRARIES
      "${ZSTD_LIBRARY}")

  add_dependencies(qatzstd::qatzstd qatzstd_ep)
endmacro()

build_qatzstd()


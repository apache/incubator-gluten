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

if("${MAKE}" STREQUAL "")
  if(NOT MSVC)
    find_program(MAKE make)
  endif()
endif()

macro(build_qatzip)
  message(STATUS "Building QATzip from source")
  set(QATZIP_BUILD_VERSION "v1.1.1")
  set(QATZIP_BUILD_SHA256_CHECKSUM
      "679f5522deb35e7ffa36f227ae49d07ef2d69a83e56bfda849303829b274e79b")
  set(QATZIP_SOURCE_URL
      "https://github.com/intel/QATzip/archive/refs/tags/${QATZIP_BUILD_VERSION}.tar.gz"
  )
  set(QATZIP_LIB_NAME "qatzip")

  set(QATZIP_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/qatzip_ep-install")
  set(QATZIP_SOURCE_DIR "${QATZIP_PREFIX}/src/qatzip_ep")
  set(QATZIP_INCLUDE_DIR "${QATZIP_SOURCE_DIR}/include")
  set(QATZIP_STATIC_LIB_NAME
      "${CMAKE_STATIC_LIBRARY_PREFIX}${QATZIP_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}"
  )
  set(QATZIP_STATIC_LIB_TARGETS
      "${QATZIP_SOURCE_DIR}/src/.libs/${QATZIP_STATIC_LIB_NAME}")
  set(QATZIP_CONFIGURE_ARGS "--prefix=${QATZIP_PREFIX}" "--with-pic"
                            "--with-ICP_ROOT=$ENV{ICP_ROOT}")

  ExternalProject_Add(
    qatzip_ep
    PREFIX ${QATZIP_PREFIX}
    URL ${QATZIP_SOURCE_URL}
    URL_HASH "SHA256=${QATZIP_BUILD_SHA256_CHECKSUM}"
    SOURCE_DIR ${QATZIP_SOURCE_DIR}
    CONFIGURE_COMMAND ${CMAKE_COMMAND} -E env QZ_ROOT=${QATZIP_SOURCE_DIR}
                      ./configure ${QATZIP_CONFIGURE_ARGS}
    BUILD_COMMAND ${MAKE} all
    BUILD_BYPRODUCTS ${QATZIP_STATIC_LIB_TARGETS}
    BUILD_IN_SOURCE 1)

  ExternalProject_Add_Step(
    qatzip_ep pre-configure
    COMMAND ./autogen.sh
    DEPENDEES download
    DEPENDERS configure
    WORKING_DIRECTORY ${QATZIP_SOURCE_DIR})

  # The include directory must exist before it is referenced by a target.
  file(MAKE_DIRECTORY "${QATZIP_INCLUDE_DIR}")

  set(QATZIP_LINK_LIBRARIES
      "${ZLIB_LIBRARY}"
      "${LZ4_LIBRARY}"
      "${UDEV_LIBRARY}"
      "${QAT_LIBRARY}"
      "${USDM_DRV_LIBRARY}"
      "${ADF_LIBRARY}"
      "${OSAL_LIBRARY}"
      Threads::Threads)

  # Fix libudev.so not get linked.
  set(QATZIP_LINK_OPTIONS "-Wl,--no-as-needed")

  add_library(qatzip::qatzip STATIC IMPORTED)
  set_target_properties(
    qatzip::qatzip
    PROPERTIES IMPORTED_LOCATION "${QATZIP_STATIC_LIB_TARGETS}"
               INTERFACE_INCLUDE_DIRECTORIES "${QATZIP_INCLUDE_DIR}"
               INTERFACE_LINK_LIBRARIES "${QATZIP_LINK_LIBRARIES}"
               INTERFACE_LINK_OPTIONS "${QATZIP_LINK_OPTIONS}")

  add_dependencies(qatzip::qatzip qatzip_ep)
endmacro()

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

find_library(ZLIB_LIBRARY REQUIRED NAMES z)
find_library(LZ4_LIBRARY REQUIRED NAMES lz4)
find_library(UDEV_LIBRARY REQUIRED NAMES udev)
find_library(
  USDM_DRV_LIBRARY REQUIRED
  NAMES usdm_drv
  PATHS "$ENV{ICP_ROOT}/build"
  NO_DEFAULT_PATH)
find_library(
  QAT_LIBRARY REQUIRED
  NAMES qat
  PATHS "$ENV{ICP_ROOT}/build"
  NO_DEFAULT_PATH)
find_library(
  OSAL_LIBRARY REQUIRED
  NAMES osal
  PATHS "$ENV{ICP_ROOT}/build"
  NO_DEFAULT_PATH)
find_library(
  ADF_LIBRARY REQUIRED
  NAMES adf
  PATHS "$ENV{ICP_ROOT}/build"
  NO_DEFAULT_PATH)

message(STATUS "Found zlib: ${ZLIB_LIBRARY}")
message(STATUS "Found lz4: ${LZ4_LIBRARY}")
message(STATUS "Found udev: ${UDEV_LIBRARY}")
message(STATUS "Found usdm_drv: ${USDM_DRV_LIBRARY}")
message(STATUS "Found qat: ${QAT_LIBRARY}")

build_qatzip()

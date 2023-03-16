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

macro(build_qatzip)
  message(STATUS "Building QATzip from source")
  set(QATZIP_BUILD_VERSION "v1.1.1")
  set(QATZIP_BUILD_SHA256_CHECKSUM "679f5522deb35e7ffa36f227ae49d07ef2d69a83e56bfda849303829b274e79b")
  set(QATZIP_SOURCE_URL
      "https://github.com/intel/QATzip/archive/refs/tags/${QATZIP_BUILD_VERSION}.tar.gz")
  set(QATZIP_SHARED_LIB_MAJOR_VERSION ".3")
  set(QATZIP_SHARED_LIB_FULL_VERSION ".3.0.1")
  set(QATZIP_LIB_NAME "qatzip")

  set(QATZIP_PREFIX
      "${CMAKE_CURRENT_BINARY_DIR}/qatzip_ep-install")
  set(QATZIP_SOURCE_DIR "${QATZIP_PREFIX}/src/qatzip_ep")
  set(QATZIP_INCLUDE_DIR "${QATZIP_SOURCE_DIR}/include")
  set(QATZIP_SHARED_LIB_NAME "${CMAKE_SHARED_LIBRARY_PREFIX}${QATZIP_LIB_NAME}${CMAKE_SHARED_LIBRARY_SUFFIX}${QATZIP_SHARED_LIB_MAJOR_VERSION}")
  set(QATZIP_SHARED_LIB_FULL_NAME "${CMAKE_SHARED_LIBRARY_PREFIX}${QATZIP_LIB_NAME}${CMAKE_SHARED_LIBRARY_SUFFIX}${QATZIP_SHARED_LIB_FULL_VERSION}")
  set(QATZIP_SHARED_LIB_TARGETS
      "${QATZIP_SOURCE_DIR}/src/.libs/${QATZIP_SHARED_LIB_NAME}"
      "${QATZIP_SOURCE_DIR}/src/.libs/${QATZIP_SHARED_LIB_FULL_NAME}"
      )
  set(QATZIP_CONFIGURE_ARGS
      "--prefix=${QATZIP_PREFIX}"
      "--with-ICP_ROOT=$ENV{ICP_ROOT}")

  ExternalProject_Add(qatzip_ep
      PREFIX ${QATZIP_PREFIX}
      URL ${QATZIP_SOURCE_URL}
      URL_HASH "SHA256=${QATZIP_BUILD_SHA256_CHECKSUM}"
      SOURCE_DIR ${QATZIP_SOURCE_DIR}
      CONFIGURE_COMMAND ${CMAKE_COMMAND} -E env QZ_ROOT=${QATZIP_SOURCE_DIR} ./configure ${QATZIP_CONFIGURE_ARGS}
      BUILD_COMMAND $(MAKE) all
      BUILD_BYPRODUCTS ${QATZIP_SHARED_LIB_TARGETS}
      BUILD_IN_SOURCE 1)

  ExternalProject_Add_Step(qatzip_ep pre-configure
      COMMAND ./autogen.sh
      DEPENDEES download
      DEPENDERS configure
      WORKING_DIRECTORY ${QATZIP_SOURCE_DIR})

  ExternalProject_Add_Step(qatzip_ep post-build
      COMMAND cp -P ${QATZIP_SHARED_LIB_TARGETS} ${root_directory}/releases/
      DEPENDEES build
      DEPENDERS install 
      WORKING_DIRECTORY ${QATZIP_SOURCE_DIR})

  # The include directory must exist before it is referenced by a target.
  file(MAKE_DIRECTORY "${QATZIP_INCLUDE_DIR}")

  add_library(qatzip::qatzip SHARED IMPORTED)
  set_target_properties(qatzip::qatzip
      PROPERTIES IMPORTED_LOCATION
      "${root_directory}/releases/${QATZIP_SHARED_LIB_NAME}"
      INTERFACE_INCLUDE_DIRECTORIES
      "${QATZIP_INCLUDE_DIR}")

  add_dependencies(qatzip::qatzip qatzip_ep)
endmacro()

build_qatzip()


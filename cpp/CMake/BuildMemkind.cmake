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

macro(build_hwloc)
  message(STATUS "Building hwloc from source")
  set(HWLOC_BUILD_VERSION "2.8.0")
  set(HWLOC_BUILD_SHA256_CHECKSUM
      "311d44e99bbf6d269c2cbc569d073978d88352bc31d51e31457d4df94783172d")
  set(HWLOC_SOURCE_URL
      "https://github.com/open-mpi/hwloc/archive/refs/tags/hwloc-${HWLOC_BUILD_VERSION}.tar.gz"
  )
  set(HWLOC_LIB_NAME "hwloc")
  set(HWLOC_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/hwloc_ep-install")
  set(HWLOC_SOURCE_DIR "${HWLOC_PREFIX}/src/hwloc_ep")
  set(HWLOC_INCLUDE_DIR "${HWLOC_SOURCE_DIR}/include")
  set(HWLOC_LIB_DIR "${HWLOC_SOURCE_DIR}/hwloc/.libs")
  set(HWLOC_STATIC_LIB_NAME
      "${CMAKE_STATIC_LIBRARY_PREFIX}${HWLOC_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}"
  )
  set(HWLOC_STATIC_LIB_TARGETS
      "${HWLOC_SOURCE_DIR}/src/.libs/${HWLOC_STATIC_LIB_NAME}")
  set(HWLOC_CONFIGURE_ARGS
      "--prefix=${HWLOC_PREFIX}" "--with-pic" "--enable-static"
      "--disable-shared" "--enable-plugins")
  ExternalProject_Add(
    hwloc_ep
    PREFIX ${HWLOC_PREFIX}
    URL ${HWLOC_SOURCE_URL}
    URL_HASH "SHA256=${HWLOC_BUILD_SHA256_CHECKSUM}"
    SOURCE_DIR ${HWLOC_SOURCE_DIR}
    CONFIGURE_COMMAND ./configure ${HWLOC_CONFIGURE_ARGS}
    BUILD_COMMAND ${MAKE}
    BUILD_BYPRODUCTS ${HWLOC_STATIC_LIB_TARGETS}
    BUILD_IN_SOURCE 1)

  ExternalProject_Add_Step(
    hwloc_ep pre-configure
    COMMAND ./autogen.sh
    DEPENDEES download
    DEPENDERS configure
    WORKING_DIRECTORY ${HWLOC_SOURCE_DIR})

  # The include directory must exist before it is referenced by a target.
  file(MAKE_DIRECTORY "${HWLOC_INCLUDE_DIR}")

  add_library(hwloc::hwloc STATIC IMPORTED)
  set_target_properties(
    hwloc::hwloc
    PROPERTIES IMPORTED_LOCATION "${HWLOC_LIB_DIR}/${HWLOC_STATIC_LIB_NAME}"
               INTERFACE_INCLUDE_DIRECTORIES "${HWLOC_INCLUDE_DIR}")

  add_dependencies(hwloc::hwloc hwloc_ep)
endmacro()

macro(build_memkind)
  message(STATUS "Building Memkind from source")
  set(MEMKIND_BUILD_VERSION "v1.14.0")
  set(MEMKIND_BUILD_SHA256_CHECKSUM
      "ab366b20b5a87ea655483631fc762ba6eb59eb6c3a08652e643f1ee3f06a6a12")
  set(MEMKIND_SOURCE_URL
      "https://github.com/memkind/memkind/archive/refs/tags/${MEMKIND_BUILD_VERSION}.tar.gz"
  )
  set(MEMKIND_LIB_NAME "memkind")
  set(MEMKIND_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/memkind_ep-install")
  set(MEMKIND_SOURCE_DIR "${MEMKIND_PREFIX}/src/memkind_ep")
  set(MEMKIND_INCLUDE_DIR "${MEMKIND_SOURCE_DIR}/include")
  set(MEMKIND_LIB_DIR "${MEMKIND_SOURCE_DIR}/.libs")
  set(MEMKIND_STATIC_LIB_NAME
      "${CMAKE_STATIC_LIBRARY_PREFIX}${MEMKIND_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}"
  )
  set(MEMKIND_STATIC_LIB_TARGETS
      "${MEMKIND_SOURCE_DIR}/src/.libs/${MEMKIND_STATIC_LIB_NAME}")
  set(MEMKIND_CONFIGURE_ARGS "--prefix=${MEMKIND_PREFIX}" "--with-pic"
                             "--enable-static")
  ExternalProject_Add(
    memkind_ep
    PREFIX ${MEMKIND_PREFIX}
    URL ${MEMKIND_SOURCE_URL}
    URL_HASH "SHA256=${MEMKIND_BUILD_SHA256_CHECKSUM}"
    SOURCE_DIR ${MEMKIND_SOURCE_DIR}
    CONFIGURE_COMMAND
      ${CMAKE_COMMAND} -E env LDFLAGS=-L${HWLOC_LIB_DIR} env
      CFLAGS=-I${HWLOC_INCLUDE_DIR} env CXXFLAGS=-I${HWLOC_INCLUDE_DIR}
      ./configure ${MEMKIND_CONFIGURE_ARGS}
    BUILD_COMMAND ${MAKE}
    BUILD_BYPRODUCTS ${MEMKIND_STATIC_LIB_TARGETS}
    BUILD_IN_SOURCE 1)

  ExternalProject_Add_Step(
    memkind_ep pre-configure
    COMMAND ./autogen.sh
    DEPENDEES download
    DEPENDERS configure
    WORKING_DIRECTORY ${MEMKIND_SOURCE_DIR})

  add_dependencies(memkind_ep hwloc::hwloc)

  # The include directory must exist before it is referenced by a target.
  file(MAKE_DIRECTORY "${MEMKIND_INCLUDE_DIR}")

  add_library(memkind::memkind STATIC IMPORTED)
  set_target_properties(
    memkind::memkind
    PROPERTIES IMPORTED_LOCATION "${MEMKIND_LIB_DIR}/${MEMKIND_STATIC_LIB_NAME}"
               INTERFACE_INCLUDE_DIRECTORIES "${MEMKIND_INCLUDE_DIR}")
  target_link_libraries(memkind::memkind INTERFACE hwloc::hwloc dl numa pthread
                                                   daxctl)

  add_dependencies(memkind::memkind memkind_ep)
endmacro()

build_hwloc()
build_memkind()

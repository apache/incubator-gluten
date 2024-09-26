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

macro(find_jemalloc)
  set(SHARED_LIBRARY_SUFFIX ".so")
  set(LIB_NAME "jemalloc")
  set(LIB_FULL_NAME
      ${CMAKE_SHARED_LIBRARY_PREFIX}${LIB_NAME}${SHARED_LIBRARY_SUFFIX})
  find_library(JEMALLOC_LIBRARY NAMES ${LIB_FULL_NAME})
  if("${JEMALLOC_LIBRARY}" STREQUAL "JEMALLOC_LIBRARY-NOTFOUND")
    message(STATUS "Jemalloc Library Not Found.")
    set(JEMALLOC_NOT_FOUND TRUE)
  else()
    message(STATUS "Found jemalloc: ${JEMALLOC_LIBRARY}")
    find_path(JEMALLOC_INCLUDE_DIR jemalloc/jemalloc.h)
    add_library(jemalloc::jemalloc SHARED IMPORTED)
    set_target_properties(
      jemalloc::jemalloc
      PROPERTIES INTERFACE_INCLUDE_DIRECTORIES "${JEMALLOC_INCLUDE_DIR}"
                 IMPORTED_LOCATION "${JEMALLOC_LIBRARY}")
  endif()
endmacro()

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

set(ARROW_STATIC_LIBRARY_SUFFIX ".a")

set(ARROW_LIB_NAME "arrow")
set(PARQUET_LIB_NAME "parquet")
set(ARROW_BUNDLED_DEPS "arrow_bundled_dependencies")

set(ARROW_INSTALL_DIR "${ARROW_HOME}/install")
set(ARROW_LIB_DIR "${ARROW_INSTALL_DIR}/lib")
set(ARROW_LIB64_DIR "${ARROW_INSTALL_DIR}/lib64")
set(ARROW_INCLUDE_DIR "${ARROW_INSTALL_DIR}/include")

function(FIND_ARROW_LIB LIB_NAME)
  if(NOT TARGET Arrow::${LIB_NAME})
    set(ARROW_LIB_FULL_NAME
        ${CMAKE_SHARED_LIBRARY_PREFIX}${LIB_NAME}${ARROW_STATIC_LIBRARY_SUFFIX})
    add_library(Arrow::${LIB_NAME} STATIC IMPORTED)
    # Firstly find the lib from velox's arrow build path. If not found, try to
    # find it from system.
    find_library(
      ARROW_LIB_${LIB_NAME}
      NAMES ${ARROW_LIB_FULL_NAME}
      PATHS ${ARROW_LIB_DIR} ${ARROW_LIB64_DIR}
      NO_DEFAULT_PATH)
    if(NOT ARROW_LIB_${LIB_NAME})
      find_library(ARROW_LIB_${LIB_NAME} NAMES ${ARROW_LIB_FULL_NAME})
    endif()
    if(NOT ARROW_LIB_${LIB_NAME})
      message(FATAL_ERROR "Arrow library Not Found: ${ARROW_LIB_FULL_NAME}")
    endif()
    message(STATUS "Found Arrow library: ${ARROW_LIB_${LIB_NAME}}")
    if(LIB_NAME STREQUAL ${ARROW_BUNDLED_DEPS})
      set_target_properties(
        Arrow::${LIB_NAME} PROPERTIES IMPORTED_LOCATION
                                      ${ARROW_LIB_${LIB_NAME}})
    else()
      set_target_properties(
        Arrow::${LIB_NAME}
        PROPERTIES IMPORTED_LOCATION ${ARROW_LIB_${LIB_NAME}}
                   INTERFACE_INCLUDE_DIRECTORIES ${ARROW_HOME}/install/include)
    endif()
  endif()
endfunction()

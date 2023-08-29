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

if (${CMAKE_SYSTEM_NAME} STREQUAL "Darwin")
  set(ARROW_SHARED_LIBRARY_SUFFIX ".800.dylib")
  set(ARROW_SHARED_LIBRARY_PARENT_SUFFIX ".800.0.0.dylib")
else()
  set(ARROW_SHARED_LIBRARY_SUFFIX ".so.800")
  set(ARROW_SHARED_LIBRARY_PARENT_SUFFIX ".so.800.0.0")
endif()

set(ARROW_LIB_NAME "arrow")
set(PARQUET_LIB_NAME "parquet")
set(ARROW_DATASET_LIB_NAME "arrow_dataset")
set(ARROW_SUBSTRAIT_LIB_NAME "arrow_substrait")

function(FIND_ARROW_LIB LIB_NAME)
  if(NOT TARGET Arrow::${LIB_NAME})
    set(ARROW_LIB_FULL_NAME ${CMAKE_SHARED_LIBRARY_PREFIX}${LIB_NAME}${ARROW_SHARED_LIBRARY_SUFFIX})
    add_library(Arrow::${LIB_NAME} SHARED IMPORTED)
    set_target_properties(Arrow::${LIB_NAME}
        PROPERTIES IMPORTED_LOCATION "${ARROW_HOME}/install/lib/${ARROW_LIB_FULL_NAME}"
        INTERFACE_INCLUDE_DIRECTORIES
        "${ARROW_HOME}/install/include")
    find_library(ARROW_LIB_${LIB_NAME}
        NAMES ${ARROW_LIB_FULL_NAME}
        PATHS ${ARROW_LIB_DIR} ${ARROW_LIB64_DIR}
        NO_DEFAULT_PATH)
    if(NOT ARROW_LIB_${LIB_NAME})
        message(FATAL_ERROR "Arrow Library Not Found: ${ARROW_LIB_FULL_NAME}")
    else()
        message(STATUS "Found Arrow Library: ${ARROW_LIB_${LIB_NAME}}")
    endif()
    file(COPY ${ARROW_LIB_${LIB_NAME}} DESTINATION ${root_directory}/releases/ FOLLOW_SYMLINK_CHAIN)
  endif()
endfunction()

message(STATUS "Use existing ARROW libraries")

set(ARROW_INSTALL_DIR "${ARROW_HOME}/install")
set(ARROW_LIB_DIR "${ARROW_INSTALL_DIR}/lib")
set(ARROW_LIB64_DIR "${ARROW_INSTALL_DIR}/lib64")
set(ARROW_INCLUDE_DIR "${ARROW_INSTALL_DIR}/include")

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

cmake_minimum_required(VERSION 3.16)

project(gazelle_cpp)

include(ExternalProject)
include(FindPkgConfig)
include(GNUInstallDirs)
include(CheckCXXCompilerFlag)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(JNI REQUIRED)

set(ARROW_ROOT "/usr/local" CACHE PATH "Arrow Root dir")
set(ARROW_LIB_DIR "${ARROW_ROOT}/lib")
set(ARROW_LIB64_DIR "${ARROW_ROOT}/lib64")

set(ARROW_SUBSTRAIT_LIB_NAME "arrow_substrait")
set(ARROW_SHARED_LIBRARY_SUFFIX ".so.800")
find_library(ARROW_SUBSTRAIT_LIB NAMES ${CMAKE_SHARED_LIBRARY_PREFIX}${ARROW_SUBSTRAIT_LIB_NAME}${ARROW_SHARED_LIBRARY_SUFFIX}  PATHS ${ARROW_LIB_DIR} ${ARROW_LIB64_DIR})
if (NOT ARROW_SUBSTRAIT_LIB)
  message(FATAL_ERROR "Arrow Engine Library Not Found")
else ()
  message(STATUS "Arrow Engine Library Can Be Found in ${ARROW_SUBSTRAIT_LIB}")
endif ()
file(COPY ${ARROW_SUBSTRAIT_LIB} DESTINATION ${root_directory}/releases/ FOLLOW_SYMLINK_CHAIN)

# Set up Arrow Engine Shared Library Directory
set(
    ARROW_SUBSTRAIT_SHARED_LIB
    "${root_directory}/releases/${CMAKE_SHARED_LIBRARY_PREFIX}${ARROW_SUBSTRAIT_LIB_NAME}${ARROW_SHARED_LIBRARY_SUFFIX}"
)

add_library(Arrow::arrow_substrait SHARED IMPORTED)
set_target_properties(Arrow::arrow_substrait
    PROPERTIES IMPORTED_LOCATION "${ARROW_SUBSTRAIT_SHARED_LIB}"
    INTERFACE_INCLUDE_DIRECTORIES
    "${root_directory}/releases/include")

set(GAZELLE_CPP_JNI_SRCS
    jni/jni_wrapper.cc
    compute/substrait_arrow.cc
    )

add_library(gazelle_cpp SHARED ${GAZELLE_CPP_JNI_SRCS})

target_include_directories(gazelle_cpp PUBLIC ${CMAKE_SYSTEM_INCLUDE_PATH} ${JNI_INCLUDE_DIRS} ${CMAKE_CURRENT_SOURCE_DIR} ${root_directory}/src)
target_link_libraries(gazelle_cpp spark_columnar_jni Arrow::arrow_substrait)

set_target_properties(gazelle_cpp PROPERTIES
    LIBRARY_OUTPUT_DIRECTORY ${root_directory}/releases
    )

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

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_arrow_lib(${ARROW_SUBSTRAIT_LIB_NAME})
find_arrow_lib(${PARQUET_LIB_NAME})
find_arrow_lib(${ARROW_DATASET_LIB_NAME})

set(GAZELLE_CPP_JNI_SRCS
    jni/JniWrapper.cc
    compute/SubstraitArrow.cc
    )

add_library(gazelle_cpp SHARED ${GAZELLE_CPP_JNI_SRCS})

target_include_directories(gazelle_cpp PUBLIC ${CMAKE_SYSTEM_INCLUDE_PATH} ${JNI_INCLUDE_DIRS} ${CMAKE_CURRENT_SOURCE_DIR} ${root_directory}/src)
target_link_libraries(gazelle_cpp spark_columnar_jni Arrow::arrow_substrait Arrow::parquet Arrow::arrow_dataset)

set_target_properties(gazelle_cpp PROPERTIES
    LIBRARY_OUTPUT_DIRECTORY ${root_directory}/releases
    )

if(BUILD_TESTS)
    add_test_case(test_substrait SOURCES tests/TestSubstrait.cc EXTRA_LINK_LIBS gazelle_cpp)
endif()

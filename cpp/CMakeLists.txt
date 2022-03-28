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
message(STATUS "Building using CMake version: ${CMAKE_VERSION}")

if (POLICY CMP0126)
  cmake_policy(SET CMP0126 NEW)
endif ()

if (NOT DEFINED CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE
      Release
      CACHE STRING "Choose the type of build.")
endif ()

project(spark_columnar_plugin)

option(DEBUG "Enable Debug Info" OFF)
option(BUILD_TESTS "Build Tests" OFF)
option(BUILD_GAZELLE_CPP "Build the gazelle-cpp libraries" OFF)

set(root_directory ${PROJECT_BINARY_DIR})

#
# Compiler flags
#

if (DEBUG)
  add_definitions(-DDEBUG)
endif ()

#
# Subdirectories
#

add_subdirectory(src)

if (BUILD_GAZELLE_CPP)
  add_subdirectory(gazelle-cpp)
endif ()

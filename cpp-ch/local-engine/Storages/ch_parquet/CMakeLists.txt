# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set(ARROW_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/arrow/cpp/src")

macro(add_headers_and_sources_including_cc prefix common_path)
    add_glob(${prefix}_headers ${CMAKE_CURRENT_SOURCE_DIR} ${common_path}/*.h)
    add_glob(${prefix}_sources ${common_path}/*.cpp ${common_path}/*.c ${common_path}/*.cc ${common_path}/*.h)
endmacro()

add_headers_and_sources(ch_parquet .)
add_headers_and_sources_including_cc(ch_parquet arrow)

add_library(ch_parquet ${ch_parquet_sources})

target_compile_options(ch_parquet PRIVATE -fPIC
    -Wno-shorten-64-to-32
    -Wno-shadow-field-in-constructor
    -Wno-return-type
    -Wno-reserved-identifier
    -Wno-extra-semi-stmt
    -Wno-extra-semi
    -Wno-unused-result
    -Wno-unreachable-code-return
    -Wno-unused-parameter
    -Wno-unreachable-code
    -Wno-pessimizing-move
    -Wno-unreachable-code-break
    -Wno-unused-variable
    -Wno-inconsistent-missing-override
    -Wno-shadow-uncaptured-local
    -Wno-suggest-override
    -Wno-unused-member-function
    -Wno-deprecated-this-capture
    -Wno-tautological-unsigned-zero-compare
)

target_link_libraries(ch_parquet PUBLIC
    boost::headers_only
    clickhouse_common_io
)

target_include_directories(ch_parquet SYSTEM BEFORE PUBLIC 
    ${ARROW_INCLUDE_DIR} 
    ${CMAKE_BINARY_DIR}/contrib/arrow-cmake/cpp/src
    ${ClickHouse_SOURCE_DIR}/contrib/arrow-cmake/cpp/src
)
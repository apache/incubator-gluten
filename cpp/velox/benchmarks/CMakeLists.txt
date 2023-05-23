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

find_arrow_lib(${PARQUET_LIB_NAME})

function(add_velox_benchmark BM_EXEC BM_FILE)
  add_executable(${BM_EXEC} ${BM_FILE} BenchmarkUtils.cc)
  target_include_directories(${BM_EXEC} PRIVATE ${CMAKE_SOURCE_DIR}/velox ${CMAKE_SOURCE_DIR}/src)
  target_link_libraries(${BM_EXEC} gflags Arrow::parquet velox benchmark::benchmark simdjson)
endfunction()

# Query benchmark
add_velox_benchmark(query_benchmark QueryBenchmark.cc)

# Generic benchmark
add_velox_benchmark(generic_benchmark GenericBenchmark.cc)

add_velox_benchmark(columnar_to_row_benchmark ColumnarToRowBenchmark.cc)

add_velox_benchmark(parquet_write_benchmark ParquetWriteBenchmark.cc)

add_velox_benchmark(shuffle_split_benchmark ShuffleSplitBenchmark.cc)

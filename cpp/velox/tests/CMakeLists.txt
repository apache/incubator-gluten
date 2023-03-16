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

function(add_velox_test TEST_EXEC)
  set(options)
  set(one_value_args)
  set(multi_value_args
    SOURCES
  )
  cmake_parse_arguments(ARG
  "${options}"
  "${one_value_args}"
  "${multi_value_args}"
  ${ARGN})

  if(ARG_SOURCES)
    set(SOURCES ${ARG_SOURCES})
  else()
    message(FATAL_ERROR "No sources specified for test ${TEST_NAME}")
  endif()
  add_executable(${TEST_EXEC} ${SOURCES})
  target_include_directories(${TEST_EXEC} PRIVATE ${CMAKE_SOURCE_DIR}/velox ${CMAKE_SOURCE_DIR}/src)
  target_link_libraries(${TEST_EXEC} gflags_shared velox gtest gtest_main glog::glog protobuf)
endfunction()

# velox test
add_velox_test(velox_splitter_test SOURCES VeloxSplitterTest.cc)
add_velox_test(velox_converter SOURCES ArrowToVeloxTest.cc VeloxColumnarToRowTest.cc)

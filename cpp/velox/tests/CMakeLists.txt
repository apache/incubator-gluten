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

function(add_velox_test TEST_EXEC)
  set(options)
  set(one_value_args)
  set(multi_value_args SOURCES)
  cmake_parse_arguments(ARG "${options}" "${one_value_args}"
                        "${multi_value_args}" ${ARGN})

  if(ARG_SOURCES)
    set(SOURCES ${ARG_SOURCES})
  else()
    message(FATAL_ERROR "No sources specified for test ${TEST_NAME}")
  endif()
  add_executable(${TEST_EXEC} ${SOURCES} ${VELOX_TEST_COMMON_SRCS})
  target_include_directories(${TEST_EXEC} PRIVATE ${CMAKE_SOURCE_DIR}/velox
                                                  ${CMAKE_SOURCE_DIR}/src)
  target_link_libraries(${TEST_EXEC} velox GTest::gtest GTest::gtest_main)
  gtest_discover_tests(${TEST_EXEC} DISCOVERY_MODE PRE_TEST)
endfunction()

set(VELOX_TEST_COMMON_SRCS JsonToProtoConverter.cc FilePathGenerator.cc)

add_velox_test(velox_shuffle_writer_test SOURCES VeloxShuffleWriterTest.cc)

add_velox_test(velox_shuffle_writer_spill_test SOURCES
               VeloxShuffleWriterSpillTest.cc)

# TODO: ORC is not well supported. add_velox_test(orc_test SOURCES OrcTest.cc)
add_velox_test(
  velox_operators_test
  SOURCES
  VeloxColumnarToRowTest.cc
  VeloxRowToColumnarTest.cc
  VeloxColumnarBatchSerializerTest.cc
  VeloxColumnarBatchTest.cc
  VeloxBatchResizerTest.cc)
add_velox_test(
  velox_plan_conversion_test
  SOURCES
  Substrait2VeloxPlanConversionTest.cc
  Substrait2VeloxPlanValidatorTest.cc
  Substrait2VeloxValuesNodeConversionTest.cc
  SubstraitExtensionCollectorTest.cc
  VeloxSubstraitRoundTripTest.cc
  VeloxSubstraitSignatureTest.cc
  VeloxToSubstraitTypeTest.cc)
add_velox_test(spark_functions_test SOURCES SparkFunctionTest.cc
               FunctionTest.cc)
add_velox_test(runtime_test SOURCES RuntimeTest.cc)
add_velox_test(velox_memory_test SOURCES MemoryManagerTest.cc)
add_velox_test(buffer_outputstream_test SOURCES BufferOutputStreamTest.cc)
if(BUILD_EXAMPLES)
  add_velox_test(my_udf_test SOURCES MyUdfTest.cc)
endif()

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

add_executable(nyc_taxi nyc_taxi.cc)
set_target_properties(nyc_taxi PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(nyc_taxi PUBLIC ${CMAKE_SYSTEM_INCLUDE_PATH} ${JNI_INCLUDE_DIRS} ${CMAKE_CURRENT_SOURCE_DIR} ${root_directory}/src)
target_link_libraries(nyc_taxi gazelle_cpp)

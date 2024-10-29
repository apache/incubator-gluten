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

clickhouse_add_executable(signal_demo signal_demo.cpp)
target_link_libraries(signal_demo PRIVATE gluten_clickhouse_backend_libs
                                          loggers)

clickhouse_add_executable(splittable_bzip2_read_buffer
                          splittable_bzip2_read_buffer.cpp)
target_link_libraries(splittable_bzip2_read_buffer
                      PRIVATE gluten_clickhouse_backend_libs loggers)

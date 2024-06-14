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

add_headers_and_sources(gluten_spark_functions .)

add_library(gluten_spark_functions_obj OBJECT ${gluten_spark_functions_sources})
list(
  APPEND
  PRIVATE_LIBS
  boost::headers_only
  pcg_random
  Poco::Foundation
  Poco::Util
  Poco::Net
  Poco::JSON
  ch_contrib::cctz
  ch_contrib::fmt
  ch_contrib::pdqsort
  ch_contrib::miniselect
  ch_contrib::magic_enum
  ch_contrib::double_conversion
  ch_contrib::dragonbox_to_chars
  ch_contrib::re2
  ch_contrib::abseil_swiss_tables
  ch_contrib::sparsehash
  ch_contrib::metrohash
  ch_contrib::murmurhash
  ch_contrib::wyhash
  ch_contrib::cityhash
  ch_contrib::farmhash
  ch_contrib::xxHash
  OpenSSL::SSL)

if(TARGET ch_contrib::vectorscan)
  list(APPEND PRIVATE_LIBS ch_contrib::vectorscan)
endif()

if(TARGET ch_contrib::rapidjson)
  list(APPEND PRIVATE_LIBS ch_contrib::rapidjson)
endif()

if(TARGET ch_contrib::simdjson)
  list(APPEND PRIVATE_LIBS ch_contrib::simdjson)
endif()

if(TARGET ch_rust::blake3)
  list(APPEND PRIVATE_LIBS ch_rust::blake3)
endif()

if(TARGET ch_contrib::gwp_asan)
  list(APPEND PRIVATE_LIBS ch_contrib::gwp_asan)
endif()

list(APPEND OBJECT_LIBS $<TARGET_OBJECTS:gluten_spark_functions_obj>)

target_link_libraries(gluten_spark_functions_obj PRIVATE ${PRIVATE_LIBS})

add_library(gluten_spark_functions INTERFACE)
target_link_libraries(gluten_spark_functions INTERFACE ${OBJECT_LIBS})

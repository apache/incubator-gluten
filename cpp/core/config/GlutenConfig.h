/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <jni.h>
#include <string>
#include <unordered_map>

namespace gluten {
const std::string kGlutenSaveDir = "spark.gluten.saveDir";

const std::string kCaseSensitive = "spark.sql.caseSensitive";

const std::string kSparkOffHeapMemory = "spark.gluten.memory.offHeap.size.in.bytes";

const std::string kSparkTaskOffHeapMemory = "spark.gluten.memory.task.offHeap.size.in.bytes";

const std::string kSparkBatchSize = "spark.gluten.sql.columnar.maxBatchSize";

const std::string kParquetBlockSize = "parquet.block.size";

const std::string kParquetCompressionCodec = "spark.sql.parquet.compression.codec";

std::unordered_map<std::string, std::string> getConfMap(JNIEnv* env, jbyteArray planArray);
} // namespace gluten

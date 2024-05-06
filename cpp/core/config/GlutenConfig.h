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

// store configurations that are general to all backend types
const std::string kDebugModeEnabled = "spark.gluten.sql.debug";

const std::string kGlutenSaveDir = "spark.gluten.saveDir";

const std::string kCaseSensitive = "spark.sql.caseSensitive";

const std::string kLegacySize = "spark.sql.legacy.sizeOfNull";

const std::string kSessionTimezone = "spark.sql.session.timeZone";

const std::string kAllowPrecisionLoss = "spark.sql.decimalOperations.allowPrecisionLoss";

const std::string kIgnoreMissingFiles = "spark.sql.files.ignoreMissingFiles";

const std::string kDefaultSessionTimezone = "spark.gluten.sql.session.timeZone.default";

const std::string kSparkOffHeapMemory = "spark.gluten.memory.offHeap.size.in.bytes";

const std::string kSparkTaskOffHeapMemory = "spark.gluten.memory.task.offHeap.size.in.bytes";

const std::string kSparkBatchSize = "spark.gluten.sql.columnar.maxBatchSize";

const std::string kParquetBlockSize = "parquet.block.size";

const std::string kParquetBlockRows = "parquet.block.rows";

const std::string kParquetGzipWindowSize = "parquet.gzip.windowSize";
const std::string kGzipWindowSize4k = "4096";

const std::string kParquetCompressionCodec = "spark.sql.parquet.compression.codec";

const std::string kUGIUserName = "spark.gluten.ugi.username";
const std::string kUGITokens = "spark.gluten.ugi.tokens";

const std::string kShuffleCompressionCodec = "spark.gluten.sql.columnar.shuffle.codec";
const std::string kShuffleCompressionCodecBackend = "spark.gluten.sql.columnar.shuffle.codecBackend";
const std::string kQatBackendName = "qat";
const std::string kIaaBackendName = "iaa";

// Velox conf
const std::string kGlogVerboseLevel = "spark.gluten.sql.columnar.backend.velox.glogVerboseLevel";
const uint32_t kGlogVerboseLevelDefault = 0;
const uint32_t kGlogVerboseLevelMaximum = 99;
const std::string kGlogSeverityLevel = "spark.gluten.sql.columnar.backend.velox.glogSeverityLevel";
const uint32_t kGlogSeverityLevelDefault = 1;

std::unordered_map<std::string, std::string>
parseConfMap(JNIEnv* env, const uint8_t* planData, const int32_t planDataLength);

std::string printConfig(const std::unordered_map<std::string, std::string>& conf);
} // namespace gluten

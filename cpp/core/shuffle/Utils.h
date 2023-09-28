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

#include <arrow/array.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/filesystem/localfs.h>
#include <arrow/filesystem/path_util.h>
#include <arrow/ipc/writer.h>
#include <arrow/type.h>
#include <arrow/util/io_util.h>
#include <chrono>

namespace gluten {

const std::string kGlutenSparkLocalDirs = "GLUTEN_SPARK_LOCAL_DIRS";

std::string generateUuid();

std::string getSpilledShuffleFileDir(const std::string& configuredDir, int32_t subDirId);

arrow::Result<std::vector<std::string>> getConfiguredLocalDirs();

arrow::Result<std::string> createTempShuffleFile(const std::string& dir);

arrow::Result<std::vector<std::shared_ptr<arrow::DataType>>> toShuffleWriterTypeId(
    const std::vector<std::shared_ptr<arrow::Field>>& fields);

int64_t getBufferSizes(const std::shared_ptr<arrow::Array>& array);

arrow::Status writeEos(arrow::io::OutputStream* os);

} // namespace gluten

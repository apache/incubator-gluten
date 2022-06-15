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

#include "compute/protobuf_utils.h"

/// Initilize the Velox backend.
void InitVeloxBackend();

/// Get the location of a file in this project.
std::string getExampleFilePath(const std::string& fileName);

/// Read binary data from a json file.
arrow::Result<std::shared_ptr<arrow::Buffer>> readFromFile(const std::string& msgPath);

/// Get the file paths, starts, lengths from a directory.
/// Use fileFormat to specify the format to read, eg., orc, parquet.
void getFileInfos(const std::string datasetPath, const std::string fileFormat,
                  std::vector<std::string>& paths, std::vector<u_int64_t>& starts,
                  std::vector<u_int64_t>& lengths);

/// Return whether the data ends with suffix.
bool EndsWith(const std::string& data, const std::string& suffix);

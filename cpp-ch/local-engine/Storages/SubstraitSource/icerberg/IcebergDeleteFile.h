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
#include<string>
#include<vector>
#include<unordered_map>

namespace local_engine::iceberg
{

enum class FileContent {
  kData,
  kPositionalDeletes,
  kEqualityDeletes,
};

// TODO: support orc

struct IcebergDeleteFile
{
  FileContent content;
  const std::string filePath;
  uint64_t recordCount;
  uint64_t fileSizeInBytes;
  // The field ids for the delete columns for equality delete files
  std::vector<int32_t> equalityFieldIds;
  // The lower bounds of the in-file positions for the deleted rows, identified
  // by each column's field id. E.g. The deleted rows for a column with field id
  // 1 is in range [10, 50], where 10 and 50 are the deleted row positions in
  // the data file, then lowerBounds would contain entry <1, "10">
  std::unordered_map<int32_t, std::string> lowerBounds;
  // The upper bounds of the in-file positions for the deleted rows, identified
  // by each column's field id. E.g. The deleted rows for a column with field id
  // 1 is in range [10, 50], then upperBounds will contain entry <1, "50">
  std::unordered_map<int32_t, std::string> upperBounds;

  IcebergDeleteFile(
    FileContent _content,
    const std::string& _filePath,
    uint64_t _recordCount,
    uint64_t _fileSizeInBytes,
      const std::vector<int32_t> & _equalityFieldIds = {},
      const std::unordered_map<int32_t, std::string> & _lowerBounds = {},
      const std::unordered_map<int32_t, std::string> & _upperBounds = {})
    : content(_content),
      filePath(_filePath),
      recordCount(_recordCount),
      fileSizeInBytes(_fileSizeInBytes),
      equalityFieldIds(_equalityFieldIds),
      lowerBounds(_lowerBounds),
      upperBounds(_upperBounds) {}
};

}
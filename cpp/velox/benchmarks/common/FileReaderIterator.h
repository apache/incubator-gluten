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

#include <arrow/io/api.h>
#include <arrow/record_batch.h>
#include <arrow/util/range.h>

#include "BenchmarkUtils.h"
#include "compute/ResultIterator.h"
#include "memory/ColumnarBatch.h"
#include "memory/ColumnarBatchIterator.h"
#include "utils/DebugOut.h"

namespace gluten {

static const std::string kOrcSuffix = ".orc";
static const std::string kParquetSuffix = ".parquet";

enum FileReaderType { kBuffered, kStream, kNone };

class FileReaderIterator : public ColumnarBatchIterator {
 public:
  explicit FileReaderIterator(const std::string& path) : path_(path) {}

  virtual ~FileReaderIterator() = default;

  virtual void createReader() = 0;

  virtual std::shared_ptr<arrow::Schema> getSchema() = 0;

  int64_t getCollectBatchTime() const {
    return collectBatchTime_;
  }

 protected:
  int64_t collectBatchTime_ = 0;
  std::string path_;
};

std::shared_ptr<gluten::ResultIterator> getInputIteratorFromFileReader(
    const std::string& path,
    FileReaderType readerType);

} // namespace gluten

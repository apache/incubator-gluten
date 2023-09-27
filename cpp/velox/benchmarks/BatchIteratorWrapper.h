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

#include "BenchmarkUtils.h"

#include "compute/ResultIterator.h"
#include "memory/ColumnarBatch.h"
#include "memory/ColumnarBatchIterator.h"
#include "utils/DebugOut.h"

#include <arrow/adapters/orc/adapter.h>
#include <arrow/c/bridge.h>
#include <arrow/io/api.h>
#include <arrow/record_batch.h>
#include <arrow/util/range.h>
#include <parquet/arrow/reader.h>

namespace gluten {

using GetInputFunc = std::shared_ptr<gluten::ResultIterator>(const std::string&);

class BatchIterator : public ColumnarBatchIterator {
 public:
  explicit BatchIterator(const std::string& path) : path_(path) {}

  virtual ~BatchIterator() = default;

  virtual void createReader() = 0;

  virtual std::shared_ptr<arrow::Schema> getSchema() = 0;

  int64_t getCollectBatchTime() const {
    return collectBatchTime_;
  }

 protected:
  int64_t collectBatchTime_ = 0;
  std::string path_;
};

class ParquetBatchIterator : public BatchIterator {
 public:
  explicit ParquetBatchIterator(const std::string& path) : BatchIterator(getExampleFilePath(path)) {}

  void createReader() override {
    parquet::ArrowReaderProperties properties = parquet::default_arrow_reader_properties();
    GLUTEN_THROW_NOT_OK(parquet::arrow::FileReader::Make(
        arrow::default_memory_pool(), parquet::ParquetFileReader::OpenFile(path_), properties, &fileReader_));
    GLUTEN_THROW_NOT_OK(
        fileReader_->GetRecordBatchReader(arrow::internal::Iota(fileReader_->num_row_groups()), &recordBatchReader_));

    auto schema = recordBatchReader_->schema();
    std::cout << "schema:\n" << schema->ToString() << std::endl;
  }

  std::shared_ptr<arrow::Schema> getSchema() override {
    return recordBatchReader_->schema();
  }

 protected:
  std::unique_ptr<parquet::arrow::FileReader> fileReader_;
  std::shared_ptr<arrow::RecordBatchReader> recordBatchReader_;
};

class OrcBatchIterator : public BatchIterator {
 public:
  explicit OrcBatchIterator(const std::string& path) : BatchIterator(path) {}

  void createReader() override {
    // Open File
    auto input = arrow::io::ReadableFile::Open(path_);
    GLUTEN_THROW_NOT_OK(input);

    // Open ORC File Reader
    auto maybeReader = arrow::adapters::orc::ORCFileReader::Open(*input, arrow::default_memory_pool());
    GLUTEN_THROW_NOT_OK(maybeReader);
    fileReader_.reset((*maybeReader).release());

    // get record batch Reader
    auto recordBatchReader = fileReader_->GetRecordBatchReader(4096, std::vector<std::string>());
    GLUTEN_THROW_NOT_OK(recordBatchReader);
    recordBatchReader_ = *recordBatchReader;
  }

  std::shared_ptr<arrow::Schema> getSchema() override {
    auto schema = fileReader_->ReadSchema();
    GLUTEN_THROW_NOT_OK(schema);
    return *schema;
  }

 protected:
  std::unique_ptr<arrow::adapters::orc::ORCFileReader> fileReader_;
  std::shared_ptr<arrow::RecordBatchReader> recordBatchReader_;
};

} // namespace gluten

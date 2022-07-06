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

#include <arrow/util/range.h>
#include <parquet/arrow/reader.h>
#include <velox/common/memory/Memory.h>
#include <velox/substrait/SubstraitToVeloxPlan.h>

#include <utility>

#include "compute/protobuf_utils.h"
#include "jni/exec_backend.h"
#include "velox/common/memory/Memory.h"

/// Initilize the Velox backend.
void InitVeloxBackend(facebook::velox::memory::MemoryPool* pool);

/// Get the location of a file in this project.
std::string getExampleFilePath(const std::string& fileName);

/// Read binary data from a json file.
arrow::Result<std::shared_ptr<arrow::Buffer>> getPlanFromFile(
    const std::string& filePath);

/// Get the file paths, starts, lengths from a directory.
/// Use fileFormat to specify the format to read, eg., orc, parquet.
/// Return a split info.
std::shared_ptr<facebook::velox::substrait::SplitInfo> getFileInfos(
    const std::string& datasetPath, const std::string& fileFormat);

/// Return whether the data ends with suffix.
bool EndsWith(const std::string& data, const std::string& suffix);

class BatchIteratorWrapper {
 public:
  explicit BatchIteratorWrapper(std::string path) : path_(std::move(path)) {}

  virtual arrow::Result<std::shared_ptr<arrow::RecordBatch>> Next() = 0;

  void CreateReader() {
    ::parquet::ArrowReaderProperties properties =
        ::parquet::default_arrow_reader_properties();
    GLUTEN_THROW_NOT_OK(::parquet::arrow::FileReader::Make(
        arrow::default_memory_pool(), ::parquet::ParquetFileReader::OpenFile(path_),
        properties, &fileReader_));
    GLUTEN_THROW_NOT_OK(fileReader_->GetRecordBatchReader(
        arrow::internal::Iota(fileReader_->num_row_groups()), &recordBatchReader_));
  }

 protected:
  std::string path_;
  std::unique_ptr<::parquet::arrow::FileReader> fileReader_;
  std::shared_ptr<arrow::RecordBatchReader> recordBatchReader_;
};

class BatchVectorIterator : public BatchIteratorWrapper {
 public:
  explicit BatchVectorIterator(std::string path) : BatchIteratorWrapper(std::move(path)) {
    CreateReader();
    GLUTEN_ASSIGN_OR_THROW(batches_, recordBatchReader_->ToRecordBatches());
    iter_ = batches_.begin();
#ifdef DEBUG
    std::cout << "Number of input batches: " << std::to_string(batches_.size())
              << std::endl;
#endif
  }

  arrow::Result<std::shared_ptr<arrow::RecordBatch>> Next() override {
    return iter_ == batches_.cend() ? nullptr : *iter_++;
  }

 private:
  arrow::RecordBatchVector batches_;
  std::vector<std::shared_ptr<arrow::RecordBatch>>::const_iterator iter_;
};

class BatchStreamIterator : public BatchIteratorWrapper {
 public:
  explicit BatchStreamIterator(std::string path) : BatchIteratorWrapper(std::move(path)) {
    CreateReader();
  }

  arrow::Result<std::shared_ptr<arrow::RecordBatch>> Next() override {
    return recordBatchReader_->Next();
  }
};

std::shared_ptr<gluten::RecordBatchResultIterator> getInputFromBatchVector(
    const std::string& path);

std::shared_ptr<gluten::RecordBatchResultIterator> getInputFromBatchStream(
    const std::string& path);

void setCpu(uint32_t cpuindex);

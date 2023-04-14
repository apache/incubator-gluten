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

#include <arrow/memory_pool.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <folly/executors/IOThreadPoolExecutor.h>

#include "operators/c2r/ArrowColumnarToRowConverter.h"
#include "operators/c2r/ColumnarToRow.h"

#include "velox/common/file/FileSystems.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/dwrf/reader/DwrfReader.h"
#include "velox/dwio/dwrf/writer/Writer.h"
#include "velox/vector/ComplexVector.h"

namespace gluten {

class DwrfDatasource {
 public:
  DwrfDatasource(
      const std::string& file_path,
      std::shared_ptr<arrow::Schema> schema,
      facebook::velox::memory::MemoryPool* pool)
      : file_path_(file_path), schema_(schema), pool_(pool) {}

  void Init(const std::unordered_map<std::string, std::string>& sparkConfs);
  std::shared_ptr<arrow::Schema> InspectSchema();
  void Write(const std::shared_ptr<arrow::RecordBatch>& rb);
  void Close();

 private:
  std::string file_path_;
  std::string final_path_;
  int32_t count_ = 0;
  int64_t num_rbs_ = 0;
  std::shared_ptr<arrow::Schema> schema_;
  std::vector<facebook::velox::RowVectorPtr> row_vecs_;
  std::shared_ptr<const facebook::velox::Type> type_;
  std::shared_ptr<facebook::velox::dwrf::Writer> writer_;
  facebook::velox::memory::MemoryPool* pool_;
};

} // namespace gluten

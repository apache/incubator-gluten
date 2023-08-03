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

#include <arrow/filesystem/filesystem.h>
#include <arrow/memory_pool.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <arrow/util/type_fwd.h>
#include <boost/algorithm/string.hpp>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <parquet/properties.h>

#include "memory/ColumnarBatch.h"
#include "memory/VeloxColumnarBatch.h"
#include "operators/writer/Datasource.h"

#include "velox/common/file/FileSystems.h"
#ifdef ENABLE_HDFS
// #include "velox/connectors/hive/storage_adapters/hdfs/HdfsFileSink.h"
#endif
#include "velox/dwio/common/DataSink.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/dwrf/reader/DwrfReader.h"
#include "velox/dwio/parquet/writer/Writer.h"
#include "velox/vector/ComplexVector.h"

namespace gluten {

class VeloxParquetDatasource final : public Datasource {
 public:
  VeloxParquetDatasource(
      const std::string& filePath,
      std::shared_ptr<facebook::velox::memory::MemoryPool> veloxPool,
      std::shared_ptr<arrow::Schema> schema)
      : Datasource(filePath, schema), filePath_(filePath), schema_(schema), pool_(std::move(veloxPool)) {}

  void init(const std::unordered_map<std::string, std::string>& sparkConfs) override;
  void inspectSchema(struct ArrowSchema* out) override;
  void write(const std::shared_ptr<ColumnarBatch>& cb) override;
  void close() override;
  std::shared_ptr<arrow::Schema> getSchema() override {
    return schema_;
  }

 private:
  int64_t maxRowGroupBytes_ = 134217728; // 128MB
  int64_t maxRowGroupRows_ = 100000000; // 100M

  std::string filePath_;
  std::shared_ptr<arrow::Schema> schema_;
  std::shared_ptr<const facebook::velox::Type> type_;
  std::shared_ptr<facebook::velox::parquet::Writer> parquetWriter_;
  std::shared_ptr<facebook::velox::memory::MemoryPool> pool_;
  std::unique_ptr<facebook::velox::dwio::common::DataSink> sink_;
};

} // namespace gluten

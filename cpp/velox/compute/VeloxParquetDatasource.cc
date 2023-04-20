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

#include "VeloxParquetDatasource.h"

#include <arrow/array/array_base.h>
#include <arrow/buffer.h>
#include <arrow/type_traits.h>

#include <cstdlib>
#include <cstring>
#include <string>

#include "ArrowTypeUtils.h"
#include "compute/Backend.h"
#include "compute/VeloxBackend.h"
#include "include/arrow/c/bridge.h"
#include "memory/MemoryAllocator.h"
#include "memory/VeloxMemoryPool.h"
#include "velox/dwio/common/Options.h"
#include "velox/row/UnsafeRowDynamicSerializer.h"
#include "velox/row/UnsafeRowSerializer.h"
#include "velox/vector/arrow/Bridge.h"

using namespace facebook;

namespace gluten {

void VeloxParquetDatasource::Init(const std::unordered_map<std::string, std::string>& sparkConfs) {
  std::string output_path;
  auto fs = arrow::fs::FileSystemFromUri(file_path_, &output_path).ValueOrDie();
  std::cout << "the output path is " << output_path << "\n";
  auto backend = std::dynamic_pointer_cast<gluten::VeloxBackend>(gluten::CreateBackend());

  auto veloxPool = AsWrappedVeloxMemoryPool(gluten::DefaultMemoryAllocator().get(), backend->GetMemoryPoolOptions());
  pool_ = veloxPool->addChild("velox_parquet_write");

  // Construct the file path and writer
  std::string local_path = "";
  if (strncmp(file_path_.c_str(), "file:", 5) == 0) {
    local_path = file_path_.substr(5);
  } else {
    throw std::runtime_error("The path is not local file path when Write data with DWRF format!");
  }

  auto pos = local_path.find("_temporary", 0);
  std::string dir_path = local_path.substr(0, pos - 1);

  final_path_ = dir_path + "/" + file_name_;
  std::string command = "touch " + final_path_;
  std::cout << "the dir_path is " << dir_path << " and the file name is " << file_name_ << "\n" << std::flush;
  auto ret = system(command.c_str());
  (void)(ret); // suppress warning

  ArrowSchema c_schema{};
  arrow::Status status = arrow::ExportSchema(*(schema_.get()), &c_schema);
  if (!status.ok()) {
    throw std::runtime_error("Failed to export from Arrow record batch");
  }

  type_ = velox::importFromArrow(c_schema);
  auto sink = std::make_unique<velox::dwio::common::LocalFileSink>(final_path_);
  auto config = std::make_shared<velox::dwrf::Config>();

  for (auto iter = sparkConfs.begin(); iter != sparkConfs.end(); iter++) {
    auto key = iter->first;
    if (strcmp(key.c_str(), "hive.exec.orc.stripe.size") == 0) {
      config->set(velox::dwrf::Config::STRIPE_SIZE, static_cast<uint64_t>(stoi(iter->second)));
    } else if (strcmp(key.c_str(), "hive.exec.orc.row.index.stride") == 0) {
      config->set(velox::dwrf::Config::ROW_INDEX_STRIDE, static_cast<uint32_t>(stoi(iter->second)));
    } else if (strcmp(key.c_str(), "hive.exec.orc.compress") == 0) {
      // Currently velox only support ZLIB and ZSTD and the default is ZSTD.
      if (strcasecmp(iter->second.c_str(), "ZLIB") == 0) {
        config->set(velox::dwrf::Config::COMPRESSION, velox::dwio::common::CompressionKind::CompressionKind_ZLIB);
      } else {
        config->set(velox::dwrf::Config::COMPRESSION, velox::dwio::common::CompressionKind::CompressionKind_ZSTD);
      }
    }
  }
  const int64_t writerMemoryCap = std::numeric_limits<int64_t>::max();

  velox::dwrf::WriterOptions options;
  options.config = config;
  options.schema = type_;
  options.memoryBudget = writerMemoryCap;
  options.flushPolicyFactory = nullptr;
  options.layoutPlannerFactory = nullptr;

  parquetWriter_ = std::make_unique<velox::parquet::Writer>(std::move(sink), *(pool_), 2048);
}

std::shared_ptr<arrow::Schema> VeloxParquetDatasource::InspectSchema() {
  velox::dwio::common::ReaderOptions reader_options(pool_.get());
  auto format = velox::dwio::common::FileFormat::PARQUET; // DWRF
  reader_options.setFileFormat(format);

  // Creates a file system: local, hdfs or s3.
  auto fs = velox::filesystems::getFileSystem(file_path_, nullptr);
  std::shared_ptr<velox::ReadFile> readFile{std::move(fs->openFileForRead(file_path_))};

  std::unique_ptr<velox::dwio::common::Reader> reader =
      velox::dwio::common::getReaderFactory(reader_options.getFileFormat())
          ->createReader(
              std::make_unique<velox::dwio::common::BufferedInput>(
                  std::make_shared<velox::dwio::common::ReadFileInputStream>(readFile), *pool_.get()),
              reader_options);
  return toArrowSchema(reader->rowType());
}

void VeloxParquetDatasource::Close() {
  if (parquetWriter_ != nullptr) {
    parquetWriter_->close();
  }
}

void VeloxParquetDatasource::Write(const std::shared_ptr<ColumnarBatch>& cb) {
  auto veloxBatch = std::dynamic_pointer_cast<VeloxColumnarBatch>(cb);
  if (veloxBatch != nullptr) {
    parquetWriter_->write(veloxBatch->getFlattenedRowVector());
  } else {
    // convert arrow record batch to velox row vector
    auto rb = arrow::ImportRecordBatch(cb->exportArrowArray().get(), cb->exportArrowSchema().get()).ValueOrDie();
    std::vector<velox::VectorPtr> vecs;

    for (int col_idx = 0; col_idx < rb->num_columns(); col_idx++) {
      auto array = rb->column(col_idx);
      ArrowArray c_array{};
      ArrowSchema c_schema{};
      arrow::Status status = arrow::ExportArray(*array, &c_array, &c_schema);
      if (!status.ok()) {
        throw std::runtime_error("Failed to export from Arrow record batch");
      }

      velox::VectorPtr vec = velox::importFromArrowAsOwner(c_schema, c_array, pool_.get());
      vecs.push_back(vec);
    }

    auto row_vec = std::make_shared<velox::RowVector>(pool_.get(), type_, nullptr, rb->num_rows(), vecs, 0);
    parquetWriter_->write(row_vec);
  }
}

} // namespace gluten

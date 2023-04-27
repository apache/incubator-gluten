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
#include "config/GlutenConfig.h"
#include "include/arrow/c/bridge.h"
#include "memory/MemoryAllocator.h"
#include "memory/VeloxMemoryPool.h"
#include "velox/dwio/common/Options.h"
#include "velox/vector/arrow/Bridge.h"

using namespace facebook;

namespace gluten {

void VeloxParquetDatasource::Init(const std::unordered_map<std::string, std::string>& sparkConfs) {
  auto backend = std::dynamic_pointer_cast<gluten::VeloxBackend>(gluten::CreateBackend());

  auto veloxPool = AsWrappedVeloxAggregateMemoryPool(gluten::DefaultMemoryAllocator().get());
  pool_ = veloxPool->addLeafChild("velox_parquet_write");

  // Construct the file path and writer
  std::string local_path = "";
  if (strncmp(file_path_.c_str(), "file:", 5) == 0) {
    local_path = file_path_.substr(5);
  } else {
    throw std::runtime_error("The path is not local file path when Write data with parquet format in velox backend!");
  }

  auto pos = local_path.find("_temporary", 0);
  std::string dir_path = local_path.substr(0, pos - 1);

  final_path_ = dir_path + "/" + file_name_;
  std::string command = "touch " + final_path_;
  auto ret = system(command.c_str());
  (void)(ret); // suppress warning

  ArrowSchema c_schema{};
  arrow::Status status = arrow::ExportSchema(*(schema_.get()), &c_schema);
  if (!status.ok()) {
    throw std::runtime_error("Failed to export from Arrow record batch");
  }

  type_ = velox::importFromArrow(c_schema);
  auto sink = std::make_unique<velox::dwio::common::LocalFileSink>(final_path_);

  auto blockSize = 1024;
  if (sparkConfs.find(kParquetBlockSize) != sparkConfs.end()) {
    blockSize = static_cast<int64_t>(stoi(sparkConfs.find(kParquetBlockSize)->second));
  }
  auto compressionCodec = arrow::Compression::UNCOMPRESSED;
  if (sparkConfs.find(kParquetCompressionCodec) != sparkConfs.end()) {
    auto compressionCodecStr = sparkConfs.find(kParquetCompressionCodec)->second;
    // spark support uncompressed snappy, gzip, lzo, brotli, lz4, zstd.
    if (boost::iequals(compressionCodecStr, "snappy")) {
      compressionCodec = arrow::Compression::SNAPPY;
    } else if (boost::iequals(compressionCodecStr, "gzip")) {
      compressionCodec = arrow::Compression::GZIP;
    } else if (boost::iequals(compressionCodecStr, "lzo")) {
      compressionCodec = arrow::Compression::LZO;
    } else if (boost::iequals(compressionCodecStr, "brotli")) {
      compressionCodec = arrow::Compression::BROTLI;
    } else if (boost::iequals(compressionCodecStr, "lz4")) {
      compressionCodec = arrow::Compression::LZ4;
    } else if (boost::iequals(compressionCodecStr, "zstd")) {
      compressionCodec = arrow::Compression::ZSTD;
    }
  }

  auto properities =
      ::parquet::WriterProperties::Builder().write_batch_size(blockSize)->compression(compressionCodec)->build();

  parquetWriter_ = std::make_unique<velox::parquet::Writer>(std::move(sink), *(pool_), 2048, properities);
}

std::shared_ptr<arrow::Schema> VeloxParquetDatasource::InspectSchema() {
  velox::dwio::common::ReaderOptions reader_options(pool_.get());
  auto format = velox::dwio::common::FileFormat::PARQUET;
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

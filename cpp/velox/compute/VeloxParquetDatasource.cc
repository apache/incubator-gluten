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
#include "arrow/c/bridge.h"
#include "compute/Backend.h"
#include "compute/VeloxBackend.h"
#include "config/GlutenConfig.h"
#include "memory/MemoryAllocator.h"
#include "memory/VeloxMemoryPool.h"
#include "velox/core/Context.h"
#include "velox/core/QueryConfig.h"
#include "velox/core/QueryCtx.h"
#include "velox/dwio/common/Options.h"
#include "velox/vector/arrow/Bridge.h"

using namespace facebook;

namespace gluten {

void VeloxParquetDatasource::init(const std::unordered_map<std::string, std::string>& sparkConfs) {
  auto backend = std::dynamic_pointer_cast<gluten::VeloxBackend>(gluten::createBackend());

  auto veloxPool = asWrappedVeloxAggregateMemoryPool(gluten::defaultMemoryAllocator().get());
  pool_ = veloxPool->addLeafChild("velox_parquet_write");

  // Construct the file path and writer
  std::string localPath = "";
  if (strncmp(filePath_.c_str(), "file:", 5) == 0) {
    localPath = filePath_.substr(5);
  } else {
    throw std::runtime_error("The path is not local file path when Write data with parquet format in velox backend!");
  }

  auto pos = localPath.find("_temporary", 0);
  std::string dirPath = localPath.substr(0, pos - 1);

  finalPath_ = dirPath + "/" + fileName_;
  std::string command = "touch " + finalPath_;
  auto ret = system(command.c_str());
  (void)(ret); // suppress warning

  ArrowSchema cSchema{};
  arrow::Status status = arrow::ExportSchema(*(schema_.get()), &cSchema);
  if (!status.ok()) {
    throw std::runtime_error("Failed to export from Arrow record batch");
  }

  type_ = velox::importFromArrow(cSchema);
  auto sink = std::make_unique<velox::dwio::common::LocalFileSink>(finalPath_);

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

  // Setting the ratio to 2 here refers to the grow strategy in the reserve() method of MemoryPool on the arrow side.
  std::unordered_map<std::string, std::string> configData({{velox::core::QueryConfig::kDataBufferGrowRatio, "2"}});
  auto queryCtxConfig = std::make_shared<velox::core::MemConfig>(configData);
  auto queryCtx = std::make_shared<velox::core::QueryCtx>(nullptr, queryCtxConfig);

  parquetWriter_ = std::make_unique<velox::parquet::Writer>(std::move(sink), *(pool_), 2048, properities, queryCtx);
}

std::shared_ptr<arrow::Schema> VeloxParquetDatasource::inspectSchema() {
  velox::dwio::common::ReaderOptions readerOptions(pool_.get());
  auto format = velox::dwio::common::FileFormat::PARQUET;
  readerOptions.setFileFormat(format);

  // Creates a file system: local, hdfs or s3.
  auto fs = velox::filesystems::getFileSystem(filePath_, nullptr);
  std::shared_ptr<velox::ReadFile> readFile{fs->openFileForRead(filePath_)};

  std::unique_ptr<velox::dwio::common::Reader> reader =
      velox::dwio::common::getReaderFactory(readerOptions.getFileFormat())
          ->createReader(
              std::make_unique<velox::dwio::common::BufferedInput>(
                  std::make_shared<velox::dwio::common::ReadFileInputStream>(readFile), *pool_.get()),
              readerOptions);
  return toArrowSchema(reader->rowType());
}

void VeloxParquetDatasource::close() {
  if (parquetWriter_ != nullptr) {
    parquetWriter_->close();
  }
}

void VeloxParquetDatasource::write(const std::shared_ptr<ColumnarBatch>& cb) {
  auto veloxBatch = std::dynamic_pointer_cast<VeloxColumnarBatch>(cb);
  if (veloxBatch != nullptr) {
    parquetWriter_->write(veloxBatch->getFlattenedRowVector());
  } else {
    // convert arrow record batch to velox row vector
    auto rb = arrow::ImportRecordBatch(cb->exportArrowArray().get(), cb->exportArrowSchema().get()).ValueOrDie();
    std::vector<velox::VectorPtr> vecs;

    for (int colIdx = 0; colIdx < rb->num_columns(); colIdx++) {
      auto array = rb->column(colIdx);
      ArrowArray cArray{};
      ArrowSchema cSchema{};
      arrow::Status status = arrow::ExportArray(*array, &cArray, &cSchema);
      if (!status.ok()) {
        throw std::runtime_error("Failed to export from Arrow record batch");
      }

      velox::VectorPtr vec = velox::importFromArrowAsOwner(cSchema, cArray, pool_.get());
      vecs.push_back(vec);
    }

    auto rowVec = std::make_shared<velox::RowVector>(pool_.get(), type_, nullptr, rb->num_rows(), vecs, 0);
    parquetWriter_->write(rowVec);
  }
}

} // namespace gluten

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

#include "arrow/c/bridge.h"
#include "compute/Backend.h"
#include "compute/VeloxBackend.h"
#include "config/GlutenConfig.h"
#include "memory/MemoryAllocator.h"
#include "memory/VeloxMemoryPool.h"
#include "utils/ArrowTypeUtils.h"
#include "velox/core/QueryConfig.h"
#include "velox/core/QueryCtx.h"
#include "velox/dwio/common/Options.h"
#include "velox/vector/arrow/Bridge.h"

using namespace facebook;
using namespace facebook::velox::dwio::common;

namespace gluten {

void VeloxParquetDatasource::init(const std::unordered_map<std::string, std::string>& sparkConfs) {
//   auto backend = std::dynamic_pointer_cast<gluten::VeloxBackend>(gluten::createBackend());

//   auto veloxPool = asAggregateVeloxMemoryPool(gluten::defaultMemoryAllocator().get());
//   pool_ = veloxPool->addAggregateChild("velox_parquet_write");

//   if (strncmp(filePath_.c_str(), "file:", 5) == 0) {
//     sink_ = std::make_unique<velox::dwio::common::LocalFileSink>(filePath_.substr(5));
//   } else if (strncmp(filePath_.c_str(), "hdfs:", 5) == 0) {
// #ifdef ENABLE_HDFS
//     sink_ = std::make_unique<velox::HdfsFileSink>(filePath_);
// #else
//     throw std::runtime_error(
//         "The write path is hdfs path but the HDFS haven't been enabled when writing parquet data in velox backend!");
// #endif

//   } else {
//     throw std::runtime_error(
//         "The file path is not local or hdfs when writing data with parquet format in velox backend!");
//   }

//   ArrowSchema cSchema{};
//   arrow::Status status = arrow::ExportSchema(*(schema_.get()), &cSchema);
//   if (!status.ok()) {
//     throw std::runtime_error("Failed to export arrow cSchema.");
//   }

//   type_ = velox::importFromArrow(cSchema);

//   if (sparkConfs.find(kParquetBlockSize) != sparkConfs.end()) {
//     maxRowGroupBytes_ = static_cast<int64_t>(stoi(sparkConfs.find(kParquetBlockSize)->second));
//   }
//   if (sparkConfs.find(kParquetBlockRows) != sparkConfs.end()) {
//     maxRowGroupRows_ = static_cast<int64_t>(stoi(sparkConfs.find(kParquetBlockRows)->second));
//   }
//   auto compressionCodec = CompressionKind::CompressionKind_SNAPPY;
//   if (sparkConfs.find(kParquetCompressionCodec) != sparkConfs.end()) {
//     auto compressionCodecStr = sparkConfs.find(kParquetCompressionCodec)->second;
//     // spark support none, uncompressed, snappy, gzip, lzo, brotli, lz4, zstd.
//     if (boost::iequals(compressionCodecStr, "snappy")) {
//       compressionCodec = CompressionKind::CompressionKind_SNAPPY;
//     } else if (boost::iequals(compressionCodecStr, "gzip")) {
//       compressionCodec = CompressionKind::CompressionKind_GZIP;
//     } else if (boost::iequals(compressionCodecStr, "lzo")) {
//       compressionCodec = CompressionKind::CompressionKind_LZO;
//     } else if (boost::iequals(compressionCodecStr, "brotli")) {
//       // please make sure `brotli` is enabled when compiling
//       throw GlutenException("Gluten+velox does not support write parquet using brotli.");
//     } else if (boost::iequals(compressionCodecStr, "lz4")) {
//       compressionCodec = CompressionKind::CompressionKind_LZ4;
//     } else if (boost::iequals(compressionCodecStr, "zstd")) {
//       compressionCodec = CompressionKind::CompressionKind_ZSTD;
//     } else if (boost::iequals(compressionCodecStr, "uncompressed")) {
//       compressionCodec = CompressionKind::CompressionKind_NONE;
//     } else if (boost::iequals(compressionCodecStr, "none")) {
//       compressionCodec = CompressionKind::CompressionKind_NONE;
//     }
//   }

//   velox::parquet::WriterOptions writeOption;
//   writeOption.maxRowGroupLength = maxRowGroupRows_;
//   writeOption.bytesInRowGroup = maxRowGroupBytes_;
//   writeOption.compression = compressionCodec;
//   // Setting the ratio to 2 here refers to the grow strategy in the reserve() method of MemoryPool on the arrow side.
//   std::unordered_map<std::string, std::string> configData({{velox::core::QueryConfig::kDataBufferGrowRatio, "2"}});
//   auto queryCtx = std::make_shared<velox::core::QueryCtx>(nullptr, configData);

//   parquetWriter_ = std::make_unique<velox::parquet::Writer>(std::move(sink_), writeOption, pool_, schema_);
}

void VeloxParquetDatasource::inspectSchema(struct ArrowSchema* out) {
  // velox::dwio::common::ReaderOptions readerOptions(pool_.get());
  // auto format = velox::dwio::common::FileFormat::PARQUET;
  // readerOptions.setFileFormat(format);

  // // Creates a file system: local, hdfs or s3.
  // auto fs = velox::filesystems::getFileSystem(filePath_, nullptr);
  // std::shared_ptr<velox::ReadFile> readFile{fs->openFileForRead(filePath_)};

  // std::unique_ptr<velox::dwio::common::Reader> reader =
  //     velox::dwio::common::getReaderFactory(readerOptions.getFileFormat())
  //         ->createReader(
  //             std::make_unique<velox::dwio::common::BufferedInput>(
  //                 std::make_shared<velox::dwio::common::ReadFileInputStream>(readFile), *pool_.get()),
  //             readerOptions);
  // toArrowSchema(reader->rowType(), out);
}

void VeloxParquetDatasource::close() {
  // if (parquetWriter_) {
  //   parquetWriter_->close();
  // }
}

void VeloxParquetDatasource::write(const std::shared_ptr<ColumnarBatch>& cb) {
  // auto veloxBatch = std::dynamic_pointer_cast<VeloxColumnarBatch>(cb);
  // VELOX_DCHECK(veloxBatch != nullptr, "Write batch should be VeloxColumnarBatch");
  // parquetWriter_->write(veloxBatch->getFlattenedRowVector());
}

} // namespace gluten

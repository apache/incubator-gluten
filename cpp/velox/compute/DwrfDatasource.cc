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

#include "DwrfDatasource.h"

#include <arrow/array/array_base.h>
#include <arrow/buffer.h>
#include <arrow/type_traits.h>

#include <cstdlib>
#include <cstring>
#include <string>

#include "ArrowTypeUtils.h"
#include "include/arrow/c/bridge.h"
#include "velox/dwio/common/Options.h"
#include "velox/row/UnsafeRowDynamicSerializer.h"
#include "velox/row/UnsafeRowSerializer.h"
#include "velox/vector/arrow/Bridge.h"

using namespace facebook;

namespace gluten {

void DwrfDatasource::Init(const std::unordered_map<std::string, std::string>& sparkConfs) {
  // Construct the file path and writer
  std::string local_path = "";
  if (strncmp(file_path_.c_str(), "file:", 5) == 0) {
    local_path = file_path_.substr(5);
  } else {
    throw std::runtime_error("The path is not local file path when Write data with DWRF format!");
  }

  auto pos = local_path.find("_temporary", 0);
  std::string dir_path = local_path.substr(0, pos - 1);

  auto last_pos = file_path_.find_last_of("/");
  std::string file_name = file_path_.substr(last_pos + 1, (file_path_.length() - last_pos - 6));
  final_path_ = dir_path + "/" + file_name;
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

  writer_ = std::make_unique<velox::dwrf::Writer>(options, std::move(sink), *pool_);
}

std::shared_ptr<arrow::Schema> DwrfDatasource::InspectSchema() {
  velox::dwio::common::ReaderOptions reader_options{pool_};
  auto format = velox::dwio::common::FileFormat::DWRF; // DWRF
  reader_options.setFileFormat(format);

  // Creates a file system: local, hdfs or s3.
  auto fs = velox::filesystems::getFileSystem(file_path_, nullptr);
  std::shared_ptr<velox::ReadFile> readFile{std::move(fs->openFileForRead(file_path_))};

  std::unique_ptr<velox::dwio::common::Reader> reader =
      velox::dwio::common::getReaderFactory(reader_options.getFileFormat())
          ->createReader(
              std::make_unique<velox::dwio::common::BufferedInput>(
                  std::make_shared<velox::dwio::common::ReadFileInputStream>(readFile), *pool_),
              reader_options);
  return toArrowSchema(reader->rowType());
}

void DwrfDatasource::Close() {
  if (writer_ != nullptr) {
    writer_->close();
  }
}

void DwrfDatasource::Write(const std::shared_ptr<arrow::RecordBatch>& rb) {
  auto num_cols = rb->num_columns();
  num_rbs_ += 1;

  std::vector<velox::VectorPtr> vecs;

  for (int col_idx = 0; col_idx < num_cols; col_idx++) {
    auto array = rb->column(col_idx);
    ArrowArray c_array{};
    ArrowSchema c_schema{};
    arrow::Status status = arrow::ExportArray(*array, &c_array, &c_schema);
    if (!status.ok()) {
      throw std::runtime_error("Failed to export from Arrow record batch");
    }

    velox::VectorPtr vec = velox::importFromArrowAsOwner(c_schema, c_array, pool_);
    vecs.push_back(vec);
  }

  auto row_vec = std::make_shared<velox::RowVector>(pool_, type_, nullptr, rb->num_rows(), vecs, 0);
  writer_->write(row_vec);
}

} // namespace gluten

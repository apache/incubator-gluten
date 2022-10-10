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
#include <regex>
#include <string>

#include "ArrowTypeUtils.h"
#include "arrow/c/Bridge.h"
#include "arrow/c/bridge.h"
#include "velox/dwio/common/Options.h"
#include "velox/row/UnsafeRowDynamicSerializer.h"
#include "velox/row/UnsafeRowSerializer.h"

using namespace facebook::velox;
using namespace facebook::velox::dwio::common;

namespace velox {
namespace compute {

void DwrfDatasource::Init(
    const std::unordered_map<std::string, std::string>& sparkConfs) {
  // Construct the file path and writer
  std::string local_path = "";
  if (strncmp(file_path_.c_str(), "file:", 5) == 0) {
    local_path = file_path_.substr(5);
  } else {
    throw std::runtime_error(
        "The path is not local file path when Write data with DWRF format!");
  }

  auto pos = local_path.find("_temporary", 0);
  std::string dir_path = local_path.substr(0, pos - 1);

  auto last_pos = file_path_.find_last_of("/");
  std::string file_name =
      file_path_.substr(last_pos + 1, (file_path_.length() - last_pos - 6));
  final_path_ = dir_path + "/" + file_name;
  std::string command = "touch " + final_path_;
  system(command.c_str());

  ArrowSchema c_schema{};
  arrow::Status status = arrow::ExportSchema(*(schema_.get()), &c_schema);
  if (!status.ok()) {
    throw std::runtime_error("Failed to export from Arrow record batch");
  }

  type_ = importFromArrow(c_schema);
  auto sink =
      std::make_unique<facebook::velox::dwio::common::FileSink>(final_path_);
  auto config = std::make_shared<facebook::velox::dwrf::Config>();

  for (auto iter = sparkConfs.begin(); iter != sparkConfs.end(); iter++) {
    auto key = iter->first;
    if (strcmp(key.c_str(), "hive.exec.orc.stripe.size") == 0) {
      config->set(
          facebook::velox::dwrf::Config::STRIPE_SIZE,
          static_cast<uint64_t>(stoi(iter->second)));
    } else if (strcmp(key.c_str(), "hive.exec.orc.row.index.stride") == 0) {
      config->set(
          facebook::velox::dwrf::Config::ROW_INDEX_STRIDE,
          static_cast<uint32_t>(stoi(iter->second)));
    } else if (strcmp(key.c_str(), "hive.exec.orc.compress") == 0) {
      // Currently velox only support ZLIB and ZSTD and the default is ZSTD.
      if (strcasecmp(iter->second.c_str(), "ZLIB") == 0) {
        config->set(
            facebook::velox::dwrf::Config::COMPRESSION,
            facebook::velox::dwio::common::CompressionKind::
                CompressionKind_ZLIB);
      } else {
        config->set(
            facebook::velox::dwrf::Config::COMPRESSION,
            facebook::velox::dwio::common::CompressionKind::
                CompressionKind_ZSTD);
      }
    }
  }
  const int64_t writerMemoryCap = std::numeric_limits<int64_t>::max();

  facebook::velox::dwrf::WriterOptions options;
  options.config = config;
  options.schema = type_;
  options.memoryBudget = writerMemoryCap;
  options.flushPolicyFactory = nullptr;
  options.layoutPlannerFactory = nullptr;

  writer_ = std::make_unique<facebook::velox::dwrf::Writer>(
      options, std::move(sink), *pool_);
}

std::shared_ptr<arrow::Schema> DwrfDatasource::InspectSchema() {
  dwio::common::ReaderOptions reader_options;
  auto format = dwio::common::FileFormat::DWRF; // DWRF
  reader_options.setFileFormat(format);

  if (strncmp(file_path_.c_str(), "file:", 5) == 0) {
    std::unique_ptr<dwio::common::Reader> reader =
        dwio::common::getReaderFactory(reader_options.getFileFormat())
            ->createReader(
                std::make_unique<dwio::common::FileInputStream>(
                    file_path_.substr(5)),
                reader_options);
    return toArrowSchema(reader->rowType());
  } else if (strncmp(file_path_.c_str(), "hdfs:", 5) == 0) {
    struct hdfsBuilder* builder = hdfsNewBuilder();
    // read hdfs client conf from hdfs-client.xml from LIBHDFS3_CONF
    hdfsBuilderSetNameNode(builder, "default");
    hdfsFS hdfs = hdfsBuilderConnect(builder);
    hdfsFreeBuilder(builder);
    std::regex hdfsPrefixExp("hdfs://(\\w+)/");
    std::string hdfsFilePath =
        regex_replace(file_path_.c_str(), hdfsPrefixExp, "/");
    HdfsReadFile readFile(hdfs, hdfsFilePath);
    std::unique_ptr<dwio::common::Reader> reader =
        dwio::common::getReaderFactory(reader_options.getFileFormat())
            ->createReader(
                std::make_unique<dwio::common::ReadFileInputStream>(&readFile),
                reader_options);
    return toArrowSchema(reader->rowType());
  } else {
    throw std::runtime_error(
        "The path is not local file path when inspect shcema with DWRF format!");
  }
}

void DwrfDatasource::Close() {
  if (writer_ != nullptr) {
    writer_->close();
  }
}

void DwrfDatasource::Write(const std::shared_ptr<arrow::RecordBatch>& rb) {
  auto num_cols = rb->num_columns();
  num_rbs_ += 1;

  std::vector<VectorPtr> vecs;

  for (int col_idx = 0; col_idx < num_cols; col_idx++) {
    auto array = rb->column(col_idx);
    ArrowArray c_array{};
    ArrowSchema c_schema{};
    arrow::Status status = arrow::ExportArray(*array, &c_array, &c_schema);
    if (!status.ok()) {
      throw std::runtime_error("Failed to export from Arrow record batch");
    }

    VectorPtr vec = importFromArrowAsOwner(c_schema, c_array, pool_);
    vecs.push_back(vec);
  }

  auto row_vec = std::make_shared<RowVector>(
      pool_, type_, nullptr, rb->num_rows(), vecs, 0);
  writer_->write(row_vec);
}

} // namespace compute
} // namespace velox

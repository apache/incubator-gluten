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

#include "memory/ColumnarBatch.h"

#include <arrow/ipc/message.h>
#include <arrow/ipc/options.h>

#include "utils/compression.h"

namespace gluten {

struct ReaderOptions {
  arrow::ipc::IpcReadOptions ipc_read_options = arrow::ipc::IpcReadOptions::Defaults();
  arrow::Compression::type compression_type = arrow::Compression::type::LZ4_FRAME;
  CodecBackend codec_backend = CodecBackend::NONE;
  CompressionMode compression_mode = CompressionMode::BUFFER;

  static ReaderOptions defaults();
};

class Reader {
 public:
  Reader(
      std::shared_ptr<arrow::io::InputStream> in,
      std::shared_ptr<arrow::Schema> schema,
      ReaderOptions options,
      std::shared_ptr<arrow::MemoryPool> pool);

  virtual ~Reader() = default;

  virtual arrow::Result<std::shared_ptr<ColumnarBatch>> next();
  arrow::Status close();
  int64_t getDecompressTime();

 protected:
  std::shared_ptr<arrow::MemoryPool> pool_;
  int64_t decompressTime_ = 0;
  ReaderOptions options_;

 private:
  std::shared_ptr<arrow::io::InputStream> in_;
  std::shared_ptr<arrow::Schema> writeSchema_;
  std::unique_ptr<arrow::ipc::Message> firstMessage_;
  bool firstMessageConsumed_ = false;
};

} // namespace gluten

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

#include <arrow/ipc/options.h>
#include "shuffle/Partitioning.h"
#include "utils/Compression.h"

namespace gluten {

static constexpr int32_t kDefaultShuffleWriterBufferSize = 4096;
static constexpr int32_t kDefaultNumSubDirs = 64;
static constexpr int32_t kDefaultCompressionThreshold = 100;
static constexpr int32_t kDefaultBufferAlignment = 64;
static constexpr double kDefaultBufferReallocThreshold = 0.25;
static constexpr bool kEnableBufferedWrite = true;
static constexpr bool kWriteEos = true;

enum PartitionWriterType { kLocal, kCeleborn };

struct ShuffleReaderOptions {
  arrow::ipc::IpcReadOptions ipc_read_options = arrow::ipc::IpcReadOptions::Defaults();
  arrow::Compression::type compression_type = arrow::Compression::type::LZ4_FRAME;
  CodecBackend codec_backend = CodecBackend::NONE;

  static ShuffleReaderOptions defaults();
};

struct ShuffleWriterOptions {
  int32_t buffer_size = kDefaultShuffleWriterBufferSize;
  int32_t push_buffer_max_size = kDefaultShuffleWriterBufferSize;
  int32_t num_sub_dirs = kDefaultNumSubDirs;
  int32_t compression_threshold = kDefaultCompressionThreshold;
  double buffer_realloc_threshold = kDefaultBufferReallocThreshold;
  arrow::Compression::type compression_type = arrow::Compression::LZ4_FRAME;
  CodecBackend codec_backend = CodecBackend::NONE;
  CompressionMode compression_mode = CompressionMode::BUFFER;
  bool buffered_write = kEnableBufferedWrite;
  bool write_eos = kWriteEos;

  PartitionWriterType partition_writer_type = PartitionWriterType::kLocal;
  Partitioning partitioning = Partitioning::kRoundRobin;

  int64_t thread_id = -1;
  int64_t task_attempt_id = -1;

  arrow::ipc::IpcWriteOptions ipc_write_options = arrow::ipc::IpcWriteOptions::Defaults();

  std::string data_file{};
  std::string local_dirs{};
  arrow::MemoryPool* memory_pool{};

  static ShuffleWriterOptions defaults();
};

} // namespace gluten

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

#include <arrow/extension_type.h>
#include <arrow/ipc/options.h>
#include <arrow/type.h>
#include <arrow/util/logging.h>

#include <deque>

#include "jni/JniCommon.h"

#include "memory/ArrowMemoryPool.h"

namespace gluten {

static constexpr int32_t kDefaultShuffleWriterBufferSize = 4096;
static constexpr int32_t kDefaultNumSubDirs = 64;
static constexpr int32_t kDefaultBatchCompressThreshold = 256;

// This 0xFFFFFFFF value is the first 4 bytes of a valid IPC message
static constexpr int32_t kIpcContinuationToken = -1;

const unsigned ONES[] = {1, 1, 1, 1, 1, 1, 1, 1};

struct ReaderOptions {
  arrow::ipc::IpcReadOptions ipc_read_options = arrow::ipc::IpcReadOptions::Defaults();

  static ReaderOptions Defaults();
};

struct SplitOptions {
  int64_t offheap_per_task = 0;
  int32_t buffer_size = kDefaultShuffleWriterBufferSize;
  int32_t push_buffer_max_size = kDefaultShuffleWriterBufferSize;
  int32_t num_sub_dirs = kDefaultNumSubDirs;
  int32_t batch_compress_threshold = kDefaultBatchCompressThreshold;
  arrow::Compression::type compression_type = arrow::Compression::UNCOMPRESSED;

  bool prefer_evict = true;
  bool write_schema = true;
  bool buffered_write = false;

  std::string data_file;
  std::string partition_writer_type = "local";

  int64_t thread_id = -1;
  int64_t task_attempt_id = -1;

  std::shared_ptr<arrow::MemoryPool> memory_pool = GetDefaultWrappedArrowMemoryPool();

  std::shared_ptr<CelebornClient> celeborn_client;

  arrow::ipc::IpcWriteOptions ipc_write_options = arrow::ipc::IpcWriteOptions::Defaults();

  std::string partitioning_name;

  static SplitOptions Defaults();
};

namespace Type {
/// \brief Data type enumeration for shuffle writer
///
/// This enumeration maps the types of arrow::Type::type with same length
/// to identical type

enum typeId : int {
  SHUFFLE_1BYTE,
  SHUFFLE_2BYTE,
  SHUFFLE_4BYTE,
  SHUFFLE_8BYTE,
  SHUFFLE_DECIMAL128,
  SHUFFLE_BIT,
  SHUFFLE_BINARY,
  SHUFFLE_LARGE_BINARY,
  SHUFFLE_LIST,
  SHUFFLE_LARGE_LIST,
  SHUFFLE_NULL,
  NUM_TYPES,
  SHUFFLE_NOT_IMPLEMENTED
};

static const typeId all[] = {
    SHUFFLE_1BYTE,
    SHUFFLE_2BYTE,
    SHUFFLE_4BYTE,
    SHUFFLE_8BYTE,
    SHUFFLE_DECIMAL128,
    SHUFFLE_BIT,
    SHUFFLE_BINARY,
    SHUFFLE_LARGE_BINARY,
    SHUFFLE_NULL,
};

} // namespace Type

} // namespace gluten

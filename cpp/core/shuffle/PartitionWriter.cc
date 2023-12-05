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

#include "shuffle/PartitionWriter.h"
#include "shuffle/Utils.h"

namespace gluten {

arrow::Result<std::unique_ptr<arrow::ipc::IpcPayload>> gluten::ShuffleWriter::PartitionWriter::createPayloadFromBuffers(
    uint32_t numRows,
    std::vector<std::shared_ptr<arrow::Buffer>> buffers) {
  std::shared_ptr<arrow::RecordBatch> recordBatch;
  if (options_->compression_type != arrow::Compression::UNCOMPRESSED) {
    ARROW_ASSIGN_OR_RAISE(
        recordBatch,
        makeCompressedRecordBatch(
            numRows,
            std::move(buffers),
            options_->write_schema,
            options_->ipc_write_options.memory_pool,
            options_->codec.get(),
            options_->compression_threshold,
            options_->compression_mode,
            compressTime_));
  } else {
    ARROW_ASSIGN_OR_RAISE(
        recordBatch,
        makeUncompressedRecordBatch(
            numRows, std::move(buffers), options_->write_schema, options_->ipc_write_options.memory_pool));
  }

  auto payload = std::make_unique<arrow::ipc::IpcPayload>();
  RETURN_NOT_OK(arrow::ipc::GetRecordBatchPayload(*recordBatch, options_->ipc_write_options, payload.get()));
  return payload;
}

} // namespace gluten
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

#include "reader.h"
#include "arrow/ipc/reader.h"
#include "arrow/record_batch.h"

#include <utility>

namespace gluten {

ReaderOptions ReaderOptions::defaults() {
  return {};
}

Reader::Reader(
    std::shared_ptr<arrow::io::InputStream> in,
    std::shared_ptr<arrow::Schema> schema,
    ReaderOptions options,
    std::shared_ptr<arrow::MemoryPool> pool)
    : pool_(pool), in_(std::move(in)), options_(std::move(options)), schema_(std::move(schema)) {
  GLUTEN_ASSIGN_OR_THROW(firstMessage_, arrow::ipc::ReadMessage(in_.get()))
  if (firstMessage_ == nullptr) {
    throw GlutenException("Failed to read message from shuffle.");
  }
  if (firstMessage_->type() == arrow::ipc::MessageType::SCHEMA) {
    GLUTEN_ASSIGN_OR_THROW(schema_, arrow::ipc::ReadSchema(*firstMessage_, nullptr))
    firstMessageConsumed_ = true;
  }
}

arrow::Result<std::shared_ptr<ColumnarBatch>> Reader::next() {
  std::shared_ptr<arrow::RecordBatch> arrowBatch;
  std::unique_ptr<arrow::ipc::Message> messageToRead;
  if (!firstMessageConsumed_) {
    messageToRead = std::move(firstMessage_);
    firstMessageConsumed_ = true;
  } else {
    GLUTEN_ASSIGN_OR_THROW(messageToRead, arrow::ipc::ReadMessage(in_.get()))
  }
  if (messageToRead == nullptr) {
    return nullptr;
  }
  GLUTEN_ASSIGN_OR_THROW(
      arrowBatch, arrow::ipc::ReadRecordBatch(*messageToRead, schema_, nullptr, options_.ipc_read_options))
  std::shared_ptr<ColumnarBatch> glutenBatch = std::make_shared<ArrowColumnarBatch>(arrowBatch);
  return glutenBatch;
}

arrow::Status Reader::close() {
  return arrow::Status::OK();
}

} // namespace gluten

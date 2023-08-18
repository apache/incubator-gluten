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

#include "VeloxColumnarBatchSerde.h"

#include "VeloxColumnarBatchSerializer.h"
#include "memory/ArrowMemoryPool.h"
#include "memory/VeloxColumnarBatch.h"
#include "velox/common/memory/Memory.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/arrow/Bridge.h"

using namespace facebook::velox;

namespace gluten {

namespace {
std::unique_ptr<ByteStream> toByteStream(uint8_t* data, int32_t size) {
  auto byteStream = std::make_unique<ByteStream>();
  ByteRange byteRange{data, size, 0};
  byteStream->resetInput({byteRange});
  return byteStream;
}
} // namespace

VeloxColumnarBatchSerde::VeloxColumnarBatchSerde(
    std::shared_ptr<facebook::velox::memory::MemoryPool> veloxPool,
    std::shared_ptr<arrow::MemoryPool> arrowPool)
    : ColumnarBatchSerde(), arrowPool_(std::move(arrowPool)), veloxPool_(std::move(veloxPool)) {}

std::shared_ptr<ColumnarBatchSerializer> VeloxColumnarBatchSerde::createSerializer() {
  return std::make_shared<VeloxColumnarBatchSerializer>(veloxPool_, arrowPool_);
}

void VeloxColumnarBatchSerde::initDeserializer(struct ArrowSchema* cSchema) {
  rowType_ = asRowType(importFromArrow(*cSchema));
  ArrowSchemaRelease(cSchema); // otherwise the c schema leaks memory
  serde_ = std::make_unique<serializer::presto::PrestoVectorSerde>();
}

std::shared_ptr<ColumnarBatch> VeloxColumnarBatchSerde::deserialize(uint8_t* data, int32_t size) {
  RowVectorPtr result;
  auto byteStream = toByteStream(data, size);
  serde_->deserialize(byteStream.get(), veloxPool_.get(), rowType_, &result, /* serdeOptions */ nullptr);
  return std::make_shared<VeloxColumnarBatch>(result);
}
} // namespace gluten

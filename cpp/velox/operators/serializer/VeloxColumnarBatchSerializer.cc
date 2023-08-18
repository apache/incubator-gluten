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

#include "VeloxColumnarBatchSerializer.h"

#include "memory/ArrowMemory.h"
#include "memory/VeloxColumnarBatch.h"
#include "velox/common/memory/Memory.h"
#include "velox/vector/FlatVector.h"

#include <iostream>

using namespace facebook::velox;

namespace gluten {

VeloxColumnarBatchSerializer::VeloxColumnarBatchSerializer(
    std::shared_ptr<facebook::velox::memory::MemoryPool> veloxPool,
    std::shared_ptr<arrow::MemoryPool> arrowPool)
    : ColumnarBatchSerializer(), veloxPool_(std::move(veloxPool)), arrowPool_(std::move(arrowPool)) {
  serde_ = std::make_unique<serializer::presto::PrestoVectorSerde>();
}

std::shared_ptr<arrow::Buffer> VeloxColumnarBatchSerializer::serializeColumnarBatches(
    const std::vector<std::shared_ptr<ColumnarBatch>>& batches) {
  VELOX_DCHECK(batches.size() != 0, "Should serialize at least 1 vector");
  auto firstRowVector = std::dynamic_pointer_cast<VeloxColumnarBatch>(batches[0])->getRowVector();
  auto numRows = firstRowVector->size();
  auto arena = std::make_unique<StreamArena>(veloxPool_.get());
  auto rowType = asRowType(firstRowVector->type());
  auto serializer = serde_->createSerializer(rowType, numRows, arena.get(), /* serdeOptions */ nullptr);
  for (auto& batch : batches) {
    auto rowVector = std::dynamic_pointer_cast<VeloxColumnarBatch>(batch)->getRowVector();
    numRows = rowVector->size();
    std::vector<IndexRange> rows(numRows);
    for (int i = 0; i < numRows; i++) {
      rows[i] = IndexRange{i, 1};
    }
    serializer->append(rowVector, folly::Range(rows.data(), numRows));
  }

  std::shared_ptr<arrow::Buffer> valueBuffer;
  GLUTEN_ASSIGN_OR_THROW(
      valueBuffer, arrow::AllocateResizableBuffer(serializer->maxSerializedSize(), arrowPool_.get()));
  auto output = std::make_shared<arrow::io::FixedSizeBufferWriter>(valueBuffer);
  serializer::presto::PrestoOutputStreamListener listener;
  ArrowFixedSizeBufferOutputStream out(output, &listener);
  serializer->flush(&out);
  GLUTEN_THROW_NOT_OK(output->Close());
  return valueBuffer;
}

} // namespace gluten

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

#include "VeloxRowToColumnarConverter.h"
#include "memory/VeloxColumnarBatch.h"
#include "velox/row/UnsafeRowDeserializers.h"
#include "velox/vector/arrow/Bridge.h"

using namespace facebook::velox;
namespace gluten {
VeloxRowToColumnarConverter::VeloxRowToColumnarConverter(
    struct ArrowSchema* cSchema,
    std::shared_ptr<memory::MemoryPool> memoryPool)
    : RowToColumnarConverter(), pool_(memoryPool) {
  rowType_ = importFromArrow(*cSchema); // otherwise the c schema leaks memory
  ArrowSchemaRelease(cSchema);
}

std::shared_ptr<ColumnarBatch>
VeloxRowToColumnarConverter::convert(int64_t numRows, int64_t* rowLength, uint8_t* memoryAddress) {
  std::vector<std::optional<std::string_view>> data;
  int64_t offset = 0;
  for (auto i = 0; i < numRows; i++) {
    data.emplace_back(std::string_view(reinterpret_cast<const char*>(memoryAddress + offset), rowLength[i]));
    offset += rowLength[i];
  }
  auto vp = row::UnsafeRowDeserializer::deserialize(data, rowType_, pool_.get());
  return std::make_shared<VeloxColumnarBatch>(std::dynamic_pointer_cast<RowVector>(vp));
}
} // namespace gluten

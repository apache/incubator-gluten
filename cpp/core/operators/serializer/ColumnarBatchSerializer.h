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

#include <arrow/c/abi.h>

#include "memory/ColumnarBatch.h"

namespace gluten {

class ColumnarBatchSerializer {
 public:
  ColumnarBatchSerializer(arrow::MemoryPool* arrowPool) : arrowPool_(arrowPool) {}

  virtual ~ColumnarBatchSerializer() = default;

  virtual std::shared_ptr<arrow::Buffer> serializeColumnarBatches(
      const std::vector<std::shared_ptr<ColumnarBatch>>& batches) = 0;

  virtual std::shared_ptr<ColumnarBatch> deserialize(uint8_t* data, int32_t size) = 0;

 protected:
  arrow::MemoryPool* arrowPool_;
};

} // namespace gluten

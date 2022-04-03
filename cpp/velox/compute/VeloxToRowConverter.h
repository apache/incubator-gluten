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

#include <arrow/memory_pool.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>

#include "operators/c2r/columnar_to_row_base.h"
#include "velox/vector/ComplexVector.h"

using namespace facebook::velox;

namespace velox {
namespace compute {

class VeloxToRowConverter : public gazellejni::columnartorow::ColumnarToRowConverterBase {
 public:
  VeloxToRowConverter(const std::shared_ptr<arrow::RecordBatch>& rb,
                      arrow::MemoryPool* memory_pool)
      : ColumnarToRowConverterBase(rb, memory_pool) {}

  arrow::Status Init() override;

  arrow::Status Write() override;

 private:
  std::vector<VectorPtr> vecs_;
  std::shared_ptr<arrow::Schema> schema_;
  std::unique_ptr<memory::MemoryPool> velox_pool_{memory::getDefaultScopedMemoryPool()};

  void ResumeVeloxVector();
};

}  // namespace compute
}  // namespace velox

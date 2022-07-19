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

#include "operators/c2r/arrow_columnar_to_row_converter.h"
#include "operators/c2r/columnar_to_row_base.h"
#include "velox/vector/ComplexVector.h"

using namespace facebook::velox;

namespace velox {
namespace compute {

class VeloxToRowConverter
    : public gluten::columnartorow::ColumnarToRowConverterBase {
 public:
  VeloxToRowConverter(
      const std::shared_ptr<arrow::RecordBatch>& rb,
      std::shared_ptr<arrow::MemoryPool> arrow_pool,
      std::shared_ptr<memory::MemoryPool> velox_pool)
      : ColumnarToRowConverterBase(rb, arrow_pool), velox_pool_(velox_pool) {}

  arrow::Status Init() override;

  arrow::Status Write() override;

 private:
  void ResumeVeloxVector();

  std::shared_ptr<memory::MemoryPool> velox_pool_;
  std::vector<VectorPtr> vecs_;
  std::shared_ptr<arrow::Schema> schema_;
};

} // namespace compute
} // namespace velox

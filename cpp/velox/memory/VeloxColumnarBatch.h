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

#include "memory/ColumnarBatch.h"
#include "memory/VeloxMemoryPool.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/arrow/Bridge.h"

namespace gluten {

class VeloxColumnarBatch : public ColumnarBatch {
 public:
  VeloxColumnarBatch(facebook::velox::RowVectorPtr rowVector)
      : ColumnarBatch(rowVector->childrenSize(), rowVector->size()), rowVector_(rowVector) {}

  std::string GetType() const override {
    return "velox";
  }

  std::shared_ptr<ArrowSchema> exportArrowSchema() override;
  std::shared_ptr<ArrowArray> exportArrowArray() override;

  void saveToFile(std::shared_ptr<ArrowWriter> writer) override;

  facebook::velox::RowVectorPtr getRowVector() const;
  facebook::velox::RowVectorPtr getFlattenedRowVector();

 private:
  void EnsureFlattened();

  facebook::velox::RowVectorPtr rowVector_ = nullptr;
  facebook::velox::RowVectorPtr flattened_ = nullptr;
};

} // namespace gluten

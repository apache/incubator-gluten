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
#include <arrow/type.h>

#include "velox/vector/ComplexVector.h"

namespace gazellejni {
namespace columnartorow {

class VeloxToRowConverter(const RowVectorPtr& rv) : rv_(rv) {}

public:
VeloxToRowConverter(const std::shared_ptr<arrow::Schema>& schema);

void Init();
void Write();

char* GetBufferAddress() { return buffer_address_; }
const std::vector<int64_t>& GetOffsets() { return offsets_; }
const std::vector<int64_t>& GetLengths() { return lengths_; }

private:
RowVectorPtr rv_;
char* buffer_address_;
arrow::MemoryPool* memory_pool_ = arrow::default_memory_pool();
std::vector<int64_t> offsets_;
std::vector<int64_t> lengths_;
std::shared_ptr<arrow::Schema> schema_;
int64_t nullBitsetWidthInBytes_;
int64_t num_cols_;
int64_t num_rows_;
}  // namespace columnartorow

}  // namespace gazellejni
}  // namespace gazellejni

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

#include <arrow/type.h>

namespace gazellejni {
namespace columnartorow {

int64_t CalculateBitSetWidthInBytes(int32_t numFields);

int64_t RoundNumberOfBytesToNearestWord(int64_t numBytes);

int64_t CalculatedFixeSizePerRow(std::shared_ptr<arrow::Schema> schema, int64_t num_cols);

int64_t GetFieldOffset(int64_t nullBitsetWidthInBytes, int32_t index);

}  // namespace columnartorow
}  // namespace gazellejni

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

#include "compute/VeloxColumnarToRowConverter.h"
#include "jni/JniErrors.h"
#include "memory/ArrowMemoryPool.h"
#include "memory/VeloxMemoryPool.h"
#include "velox/type/Timestamp.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

#include <gtest/gtest.h>

using namespace facebook;
using namespace facebook::velox;

namespace gluten {
class VeloxColumnarToRowTest : public ::testing::Test, public test::VectorTestBase {};

TEST_F(VeloxColumnarToRowTest, timestamp) {
  // only RowVector can convert to RecordBatch
  std::vector<Timestamp> timeValues = {
      Timestamp{0, 0}, Timestamp{12, 0}, Timestamp{0, 17'123'456}, Timestamp{1, 17'123'456}, Timestamp{-1, 17'123'456}};
  auto row = makeRowVector({
      makeFlatVector<Timestamp>(timeValues),
  });
  auto arrowPool = GetDefaultWrappedArrowMemoryPool();
  auto veloxPool = AsWrappedVeloxMemoryPool(DefaultMemoryAllocator().get());
  auto converter = std::make_shared<VeloxColumnarToRowConverter>(row, arrowPool, veloxPool);

  JniAssertOkOrThrow(
      converter->Init(),
      "Native convert columnar to row: Init "
      "ColumnarToRowConverter failed");
  JniAssertOkOrThrow(converter->Write(), "Native convert columnar to row: ColumnarToRowConverter write failed");
}
} // namespace gluten

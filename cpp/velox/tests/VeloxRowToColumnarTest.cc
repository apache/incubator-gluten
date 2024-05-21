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

#include "memory/ArrowMemoryPool.h"
#include "memory/VeloxColumnarBatch.h"
#include "operators/serializer//VeloxColumnarToRowConverter.h"
#include "operators/serializer//VeloxRowToColumnarConverter.h"
#include "utils/VeloxArrowUtils.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook;
using namespace facebook::velox;

namespace gluten {
class VeloxRowToColumnarTest : public ::testing::Test, public test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  void testRowVectorEqual(velox::RowVectorPtr vector) {
    auto columnarToRowConverter = std::make_shared<VeloxColumnarToRowConverter>(pool_);

    auto columnarBatch = std::make_shared<VeloxColumnarBatch>(vector);
    columnarToRowConverter->convert(columnarBatch);

    int64_t numRows = vector->size();
    uint8_t* address = columnarToRowConverter->getBufferAddress();
    auto lengthVec = columnarToRowConverter->getLengths();

    long lengthArr[lengthVec.size()];
    for (int i = 0; i < lengthVec.size(); i++) {
      lengthArr[i] = lengthVec[i];
    }

    ArrowSchema cSchema;
    toArrowSchema(vector->type(), pool(), &cSchema);
    auto rowToColumnarConverter = std::make_shared<VeloxRowToColumnarConverter>(&cSchema, pool_);

    auto cb = rowToColumnarConverter->convert(numRows, lengthArr, address);
    auto vp = std::dynamic_pointer_cast<VeloxColumnarBatch>(cb)->getRowVector();
    velox::test::assertEqualVectors(vector, vp);
  }
};

TEST_F(VeloxRowToColumnarTest, allTypes) {
  auto vector = makeRowVector({
      makeNullableFlatVector<int8_t>({1, 2, 3, std::nullopt, 4, std::nullopt, 5, 6, std::nullopt, 7}),
      makeNullableFlatVector<int8_t>({1, -1, std::nullopt, std::nullopt, -2, 2, std::nullopt, std::nullopt, 3, -3}),
      makeNullableFlatVector<int32_t>({1, 2, 3, 4, std::nullopt, 5, 6, 7, 8, std::nullopt}),
      makeNullableFlatVector<int64_t>(
          {std::nullopt,
           std::nullopt,
           std::nullopt,
           std::nullopt,
           std::nullopt,
           std::nullopt,
           std::nullopt,
           std::nullopt,
           std::nullopt,
           std::nullopt}),
      makeNullableFlatVector<float>(
          {-0.1234567,
           std::nullopt,
           0.1234567,
           std::nullopt,
           -0.142857,
           std::nullopt,
           0.142857,
           0.285714,
           0.428617,
           std::nullopt}),
      makeNullableFlatVector<bool>(
          {std::nullopt, true, false, std::nullopt, true, true, false, true, std::nullopt, std::nullopt}),
      makeFlatVector<velox::StringView>(
          {"alice0", "bob1", "alice2", "bob3", "Alice4", "Bob5", "AlicE6", "boB7", "ALICE8", "BOB9"}),
      makeNullableFlatVector<velox::StringView>(
          {"alice", "bob", std::nullopt, std::nullopt, "Alice", "Bob", std::nullopt, "alicE", std::nullopt, "boB"}),
  });
  testRowVectorEqual(vector);
}
} // namespace gluten

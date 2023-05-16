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

#include "compute/Backend.h"

#include <gtest/gtest.h>
#include "TestUtils.h"

namespace gluten {

class DummyBackend final : public Backend {
 public:
  std::shared_ptr<ResultIterator> getResultIterator(
      MemoryAllocator* allocator,
      const std::string& spillDir,
      const std::vector<std::shared_ptr<ResultIterator>>& inputs,
      const std::unordered_map<std::string, std::string>& sessionConf) override {
    auto resIter = std::make_unique<DummyResultIterator>();
    return std::make_shared<ResultIterator>(std::move(resIter));
  }

 private:
  class DummyResultIterator : public ColumnarBatchIterator {
   public:
    std::shared_ptr<ColumnarBatch> next() override {
      if (!hasNext_) {
        return nullptr;
      }
      hasNext_ = false;

      auto fArrInt32 = arrow::field("f_int32", arrow::int32());
      auto rbSchema = arrow::schema({fArrInt32});
      const std::vector<std::string> inputDataArr = {R"([1, 2,3])"};
      std::shared_ptr<arrow::RecordBatch> inputBatchArr;
      makeInputBatch(inputDataArr, rbSchema, &inputBatchArr);
      return std::make_shared<ArrowColumnarBatch>(inputBatchArr);
    }

   private:
    bool hasNext_ = true;
  };
};

TEST(TestExecBackend, CreateBackend) {
  setBackendFactory([] { return std::make_shared<DummyBackend>(); });
  auto backendP = createBackend();
  auto& backend = *backendP.get();
  ASSERT_EQ(typeid(backend), typeid(DummyBackend));
}

TEST(TestExecBackend, GetResultIterator) {
  auto backend = std::make_shared<DummyBackend>();
  auto iter = backend->getResultIterator(defaultMemoryAllocator().get(), "/tmp/test-spill", {}, {});
  ASSERT_TRUE(iter->hasNext());
  auto next = iter->next();
  ASSERT_NE(next, nullptr);
  ASSERT_FALSE(iter->hasNext());
  next = iter->next();
  ASSERT_EQ(next, nullptr);
}

} // namespace gluten

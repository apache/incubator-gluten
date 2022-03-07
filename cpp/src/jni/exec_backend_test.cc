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

#include "exec_backend.h"

#include <gtest/gtest.h>

namespace gazellejni {

class DummyBackend : public ExecBackendBase {
 public:
  std::shared_ptr<RecordBatchResultIterator> GetResultIterator() override {
    auto res_iter = std::make_shared<ResultIterator>();
    return std::make_shared<RecordBatchResultIterator>(std::move(res_iter));
  }
  std::shared_ptr<RecordBatchResultIterator> GetResultIterator(
      std::vector<std::shared_ptr<RecordBatchResultIterator>> inputs) {
    return GetResultIterator();
  }

 private:
  class ResultIterator {
   public:
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> Next() {
      if (!has_next_) {
        return nullptr;
      }
      has_next_ = false;

      std::unique_ptr<arrow::ArrayBuilder> tmp;
      std::unique_ptr<arrow::DoubleBuilder> builder;
      std::shared_ptr<arrow::Array> array;
      RETURN_NOT_OK(
          arrow::MakeBuilder(arrow::default_memory_pool(), arrow::float64(), &tmp));
      builder.reset(
          arrow::internal::checked_cast<arrow::DoubleBuilder*>(tmp.release()));

      RETURN_NOT_OK(builder->Append(1000));
      RETURN_NOT_OK(builder->Finish(&array));
      std::vector<std::shared_ptr<arrow::Field>> ret_types = {
          arrow::field("res", arrow::float64())};
      return arrow::RecordBatch::Make(arrow::schema(ret_types), 1, {array});
    }

   private:
    bool has_next_ = true;
  };
};

TEST(TestExecBackend, CreateBackend) {
  SetBackendFactory([] { return std::make_shared<DummyBackend>(); });
  auto backend = CreateBackend();
  ASSERT_EQ(typeid(*backend), typeid(DummyBackend));
}

TEST(TestExecBackend, GetResultIterator) {
  auto backend = std::make_shared<DummyBackend>();
  auto iter = backend->GetResultIterator();
  ASSERT_TRUE(iter->HasNext());
  auto next = iter->Next();
  ASSERT_NE(next, nullptr);
  ASSERT_FALSE(iter->HasNext());
  next = iter->Next();
  ASSERT_EQ(next, nullptr);
}

}  // namespace gazellejni

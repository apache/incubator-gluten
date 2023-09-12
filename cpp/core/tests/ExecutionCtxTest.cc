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

#include "compute/ExecutionCtx.h"

#include <gtest/gtest.h>

namespace gluten {

class DummyExecutionCtx final : public ExecutionCtx {
 public:
  std::shared_ptr<ResultIterator> getResultIterator(
      MemoryManager* memoryManager,
      const std::string& spillDir,
      const std::vector<std::shared_ptr<ResultIterator>>& inputs,
      const std::unordered_map<std::string, std::string>& sessionConf) override {
    auto resIter = std::make_unique<DummyResultIterator>();
    return std::make_shared<ResultIterator>(std::move(resIter));
  }
  MemoryManager* createMemoryManager(
      const std::string& name,
      std::shared_ptr<MemoryAllocator> ptr,
      std::unique_ptr<AllocationListener> uniquePtr) override {
    return nullptr;
  }
  std::shared_ptr<ColumnarToRowConverter> getColumnar2RowConverter(MemoryManager* memoryManager) override {
    return std::shared_ptr<ColumnarToRowConverter>();
  }
  std::shared_ptr<RowToColumnarConverter> getRowToColumnarConverter(
      MemoryManager* memoryManager,
      struct ArrowSchema* cSchema) override {
    return std::shared_ptr<RowToColumnarConverter>();
  }
  std::shared_ptr<ShuffleWriter> createShuffleWriter(
      int numPartitions,
      std::shared_ptr<ShuffleWriter::PartitionWriterCreator> partitionWriterCreator,
      const ShuffleWriterOptions& options,
      MemoryManager* memoryManager) override {
    return std::shared_ptr<ShuffleWriter>();
  }
  std::shared_ptr<Metrics> getMetrics(ColumnarBatchIterator* rawIter, int64_t exportNanos) override {
    return std::shared_ptr<Metrics>();
  }
  std::shared_ptr<Datasource> getDatasource(
      const std::string& filePath,
      MemoryManager* memoryManager,
      std::shared_ptr<arrow::Schema> schema) override {
    return std::shared_ptr<Datasource>();
  }
  std::shared_ptr<ShuffleReader> createShuffleReader(
      std::shared_ptr<arrow::Schema> schema,
      ReaderOptions options,
      std::shared_ptr<arrow::MemoryPool> pool,
      MemoryManager* memoryManager) override {
    return std::shared_ptr<ShuffleReader>();
  }
  std::shared_ptr<ColumnarBatchSerializer> getColumnarBatchSerializer(
      MemoryManager* memoryManager,
      std::shared_ptr<arrow::MemoryPool> arrowPool,
      struct ArrowSchema* cSchema) override {
    return std::shared_ptr<ColumnarBatchSerializer>();
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
      auto rb = arrow::RecordBatch::Make(rbSchema, 1, std::vector<std::shared_ptr<arrow::Array>>{});
      return std::make_shared<ArrowColumnarBatch>(rb);
    }

   private:
    bool hasNext_ = true;
  };
};

static std::shared_ptr<ExecutionCtx> DummyExecutionCtxFactory() {
  return std::make_shared<DummyExecutionCtx>();
}

TEST(TestExecutionCtx, CreateExecutionCtx) {
  setExecutionCtxFactory(DummyExecutionCtxFactory);
  auto executionCtxP = createExecutionCtx();
  auto& executionCtx = *executionCtxP.get();
  ASSERT_EQ(typeid(executionCtx), typeid(DummyExecutionCtx));
}

TEST(TestExecutionCtx, GetResultIterator) {
  auto executionCtx = std::make_shared<DummyExecutionCtx>();
  auto iter = executionCtx->getResultIterator(nullptr, "/tmp/test-spill", {}, {});
  ASSERT_TRUE(iter->hasNext());
  auto next = iter->next();
  ASSERT_NE(next, nullptr);
  ASSERT_FALSE(iter->hasNext());
  next = iter->next();
  ASSERT_EQ(next, nullptr);
}

} // namespace gluten

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
  ResourceHandle createResultIterator(
      MemoryManager* memoryManager,
      const std::string& spillDir,
      const std::vector<std::shared_ptr<ResultIterator>>& inputs,
      const std::unordered_map<std::string, std::string>& sessionConf) override {
    auto resIter = std::make_unique<DummyResultIterator>();
    auto iter = std::make_shared<ResultIterator>(std::move(resIter));
    return resultIteratorHolder_.insert(iter);
  }
  MemoryManager* createMemoryManager(
      const std::string& name,
      std::shared_ptr<MemoryAllocator> ptr,
      std::unique_ptr<AllocationListener> uniquePtr) override {
    return nullptr;
  }
  ResourceHandle addResultIterator(std::shared_ptr<ResultIterator> ptr) override {
    return kInvalidResourceHandle;
  }
  std::shared_ptr<ResultIterator> getResultIterator(ResourceHandle handle) override {
    return resultIteratorHolder_.lookup(handle);
  }
  void releaseResultIterator(ResourceHandle handle) override {}
  ResourceHandle addBatch(std::shared_ptr<ColumnarBatch> ptr) override {
    return kInvalidResourceHandle;
  }
  std::shared_ptr<ColumnarBatch> getBatch(ResourceHandle handle) override {
    return std::shared_ptr<ColumnarBatch>();
  }
  void releaseBatch(ResourceHandle handle) override {}
  ResourceHandle createColumnar2RowConverter(MemoryManager* memoryManager) override {
    return kInvalidResourceHandle;
  }
  std::shared_ptr<ColumnarToRowConverter> getColumnar2RowConverter(ResourceHandle handle) override {
    return std::shared_ptr<ColumnarToRowConverter>();
  }
  void releaseColumnar2RowConverter(ResourceHandle handle) override {}
  ResourceHandle createRow2ColumnarConverter(MemoryManager* memoryManager, struct ArrowSchema* cSchema) override {
    return kInvalidResourceHandle;
  }
  std::shared_ptr<RowToColumnarConverter> getRow2ColumnarConverter(ResourceHandle handle) override {
    return std::shared_ptr<RowToColumnarConverter>();
  }
  void releaseRow2ColumnarConverter(ResourceHandle handle) override {}
  ResourceHandle createShuffleWriter(
      int numPartitions,
      std::shared_ptr<ShuffleWriter::PartitionWriterCreator> partitionWriterCreator,
      const ShuffleWriterOptions& options,
      MemoryManager* memoryManager) override {
    return kInvalidResourceHandle;
  }
  std::shared_ptr<ShuffleWriter> getShuffleWriter(ResourceHandle handle) override {
    return std::shared_ptr<ShuffleWriter>();
  }
  void releaseShuffleWriter(ResourceHandle handle) override {}
  std::shared_ptr<Metrics> getMetrics(ColumnarBatchIterator* rawIter, int64_t exportNanos) override {
    return std::shared_ptr<Metrics>();
  }
  ResourceHandle createDatasource(
      const std::string& filePath,
      MemoryManager* memoryManager,
      std::shared_ptr<arrow::Schema> schema) override {
    return kInvalidResourceHandle;
  }
  std::shared_ptr<Datasource> getDatasource(ResourceHandle handle) override {
    return std::shared_ptr<Datasource>();
  }
  void releaseDatasource(ResourceHandle handle) override {}
  ResourceHandle createShuffleReader(
      std::shared_ptr<arrow::Schema> schema,
      ReaderOptions options,
      std::shared_ptr<arrow::MemoryPool> pool,
      MemoryManager* memoryManager) override {
    return kInvalidResourceHandle;
  }
  std::shared_ptr<ShuffleReader> getShuffleReader(ResourceHandle handle) override {
    return std::shared_ptr<ShuffleReader>();
  }
  void releaseShuffleReader(ResourceHandle handle) override {}
  ResourceHandle createColumnarBatchSerializer(
      MemoryManager* memoryManager,
      std::shared_ptr<arrow::MemoryPool> arrowPool,
      struct ArrowSchema* cSchema) override {
    return kInvalidResourceHandle;
  }
  std::unique_ptr<ColumnarBatchSerializer> createTempColumnarBatchSerializer(
      MemoryManager* memoryManager,
      std::shared_ptr<arrow::MemoryPool> arrowPool,
      struct ArrowSchema* cSchema) override {
    return std::unique_ptr<ColumnarBatchSerializer>();
  }
  std::shared_ptr<ColumnarBatchSerializer> getColumnarBatchSerializer(ResourceHandle handle) override {
    return std::shared_ptr<ColumnarBatchSerializer>();
  }
  void releaseColumnarBatchSerializer(ResourceHandle handle) override {}

 private:
  ConcurrentMap<std::shared_ptr<ResultIterator>> resultIteratorHolder_;

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

static ExecutionCtx* DummyExecutionCtxFactory() {
  return new DummyExecutionCtx();
}

TEST(TestExecutionCtx, CreateExecutionCtx) {
  setExecutionCtxFactory(DummyExecutionCtxFactory);
  auto executionCtx = createExecutionCtx();
  ASSERT_EQ(typeid(*executionCtx), typeid(DummyExecutionCtx));
  releaseExecutionCtx(executionCtx);
}

TEST(TestExecutionCtx, GetResultIterator) {
  auto executionCtx = std::make_shared<DummyExecutionCtx>();
  auto handle = executionCtx->createResultIterator(nullptr, "/tmp/test-spill", {}, {});
  auto iter = executionCtx->getResultIterator(handle);
  ASSERT_TRUE(iter->hasNext());
  auto next = iter->next();
  ASSERT_NE(next, nullptr);
  ASSERT_FALSE(iter->hasNext());
  next = iter->next();
  ASSERT_EQ(next, nullptr);
}

} // namespace gluten

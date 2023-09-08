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

#include "compute/ProtobufUtils.h"
#include "compute/ResultIterator.h"
#include "memory/ArrowMemoryPool.h"
#include "memory/ColumnarBatch.h"
#include "memory/MemoryManager.h"
#include "operators/c2r/ColumnarToRow.h"
#include "operators/r2c/RowToColumnar.h"
#include "operators/serializer/ColumnarBatchSerializer.h"
#include "operators/writer/Datasource.h"
#include "shuffle/ShuffleReader.h"
#include "shuffle/ShuffleWriter.h"
#include "substrait/plan.pb.h"
#include "utils/ConcurrentMap.h"

namespace gluten {

class ResultIterator;

struct SparkTaskInfo {
  int32_t stageId;
  int32_t partitionId;
  int64_t taskId;
};

/// ExecutionCtx is stateful and manager all kinds of native resources' lifecycle during execute a computation fragment.
class ExecutionCtx : public std::enable_shared_from_this<ExecutionCtx> {
 public:
  ExecutionCtx() = default;
  ExecutionCtx(const std::unordered_map<std::string, std::string>& confMap) : confMap_(confMap) {}
  virtual ~ExecutionCtx() = default;

  virtual ResourceHandle createResultIterator(
      MemoryManager* memoryManager,
      const std::string& spillDir,
      const std::vector<std::shared_ptr<ResultIterator>>& inputs,
      const std::unordered_map<std::string, std::string>& sessionConf) = 0;
  virtual ResourceHandle addResultIterator(std::shared_ptr<ResultIterator>) = 0;
  virtual std::shared_ptr<ResultIterator> getResultIterator(ResourceHandle) = 0;
  virtual void releaseResultIterator(ResourceHandle) = 0;

  virtual ResourceHandle addBatch(std::shared_ptr<ColumnarBatch>) = 0;
  virtual std::shared_ptr<ColumnarBatch> getBatch(ResourceHandle) = 0;
  virtual void releaseBatch(ResourceHandle) = 0;

  void parsePlan(const uint8_t* data, int32_t size) {
    parsePlan(data, size, {-1, -1, -1});
  }

  /// Parse and cache the plan.
  /// Return true if parsed successfully.
  void parsePlan(const uint8_t* data, int32_t size, SparkTaskInfo taskInfo) {
    taskInfo_ = taskInfo;
#ifdef GLUTEN_PRINT_DEBUG
    try {
      auto jsonPlan = substraitFromPbToJson("Plan", data, size);
      std::cout << std::string(50, '#') << " received substrait::Plan:" << std::endl;
      std::cout << "Task stageId: " << taskInfo_.stageId << ", partitionId: " << taskInfo_.partitionId
                << ", taskId: " << taskInfo_.taskId << "; " << jsonPlan << std::endl;
    } catch (const std::exception& e) {
      std::cerr << "Error converting Substrait plan to JSON: " << e.what() << std::endl;
    }
#endif
    GLUTEN_CHECK(parseProtobuf(data, size, &substraitPlan_) == true, "Parse substrait plan failed");
  }

  // Just for benchmark
  ::substrait::Plan& getPlan() {
    return substraitPlan_;
  }

  virtual MemoryManager* createMemoryManager(
      const std::string& name,
      std::shared_ptr<MemoryAllocator>,
      std::unique_ptr<AllocationListener>) = 0;

  /// This function is used to create certain converter from the format used by
  /// the backend to Spark unsafe row.
  virtual ResourceHandle createColumnar2RowConverter(MemoryManager* memoryManager) = 0;
  virtual std::shared_ptr<ColumnarToRowConverter> getColumnar2RowConverter(ResourceHandle) = 0;
  virtual void releaseColumnar2RowConverter(ResourceHandle) = 0;

  virtual ResourceHandle createRow2ColumnarConverter(MemoryManager* memoryManager, struct ArrowSchema* cSchema) = 0;
  virtual std::shared_ptr<RowToColumnarConverter> getRow2ColumnarConverter(ResourceHandle) = 0;
  virtual void releaseRow2ColumnarConverter(ResourceHandle) = 0;

  virtual ResourceHandle createShuffleWriter(
      int numPartitions,
      std::shared_ptr<ShuffleWriter::PartitionWriterCreator> partitionWriterCreator,
      const ShuffleWriterOptions& options,
      MemoryManager* memoryManager) = 0;
  virtual std::shared_ptr<ShuffleWriter> getShuffleWriter(ResourceHandle) = 0;
  virtual void releaseShuffleWriter(ResourceHandle) = 0;

  virtual std::shared_ptr<Metrics> getMetrics(ColumnarBatchIterator* rawIter, int64_t exportNanos) = 0;

  virtual ResourceHandle createDatasource(
      const std::string& filePath,
      MemoryManager* memoryManager,
      std::shared_ptr<arrow::Schema> schema) = 0;
  virtual std::shared_ptr<Datasource> getDatasource(ResourceHandle) = 0;
  virtual void releaseDatasource(ResourceHandle) = 0;

  virtual ResourceHandle createShuffleReader(
      std::shared_ptr<arrow::Schema> schema,
      ReaderOptions options,
      std::shared_ptr<arrow::MemoryPool> pool,
      MemoryManager* memoryManager) = 0;
  virtual std::shared_ptr<ShuffleReader> getShuffleReader(ResourceHandle) = 0;
  virtual void releaseShuffleReader(ResourceHandle) = 0;

  virtual ResourceHandle createColumnarBatchSerializer(
      MemoryManager* memoryManager,
      std::shared_ptr<arrow::MemoryPool> arrowPool,
      struct ArrowSchema* cSchema) = 0;
  // TODO: separate serializer and deserializer then remove this method.
  virtual std::unique_ptr<ColumnarBatchSerializer> createTempColumnarBatchSerializer(
      MemoryManager* memoryManager,
      std::shared_ptr<arrow::MemoryPool> arrowPool,
      struct ArrowSchema* cSchema) = 0;
  virtual std::shared_ptr<ColumnarBatchSerializer> getColumnarBatchSerializer(ResourceHandle) = 0;
  virtual void releaseColumnarBatchSerializer(ResourceHandle) = 0;

  std::unordered_map<std::string, std::string> getConfMap() {
    return confMap_;
  }

  SparkTaskInfo getSparkTaskInfo() {
    return taskInfo_;
  }

 protected:
  ::substrait::Plan substraitPlan_;
  SparkTaskInfo taskInfo_;
  // static conf map
  std::unordered_map<std::string, std::string> confMap_;
};

using ExecutionCtxFactoryWithConf = ExecutionCtx* (*)(const std::unordered_map<std::string, std::string>&);
using ExecutionCtxFactory = ExecutionCtx* (*)();

struct ExecutionCtxFactoryContext {
  std::mutex mutex;

  enum {
    kExecutionCtxFactoryInvalid,
    kExecutionCtxFactoryDefault,
    kExecutionCtxFactoryWithConf
  } type = kExecutionCtxFactoryInvalid;

  union {
    ExecutionCtxFactoryWithConf backendFactoryWithConf;
    ExecutionCtxFactory backendFactory;
  };

  std::unordered_map<std::string, std::string> sparkConf_;

  void set(ExecutionCtxFactoryWithConf factory, const std::unordered_map<std::string, std::string>& sparkConf) {
    std::lock_guard<std::mutex> lockGuard(mutex);

    if (type != kExecutionCtxFactoryInvalid) {
      assert(false);
      abort();
      return;
    }

    type = kExecutionCtxFactoryWithConf;
    backendFactoryWithConf = factory;
    this->sparkConf_.clear();
    for (auto& x : sparkConf) {
      this->sparkConf_[x.first] = x.second;
    }
  }

  void set(ExecutionCtxFactory factory) {
    std::lock_guard<std::mutex> lockGuard(mutex);
    if (type != kExecutionCtxFactoryInvalid) {
      assert(false);
      abort();
      return;
    }

    type = kExecutionCtxFactoryDefault;
    backendFactory = factory;
  }

  ExecutionCtx* create() {
    std::lock_guard<std::mutex> lockGuard(mutex);
    if (type == kExecutionCtxFactoryInvalid) {
      assert(false);
      abort();
      return nullptr;
    } else if (type == kExecutionCtxFactoryWithConf) {
      return backendFactoryWithConf(sparkConf_);
    } else {
      return backendFactory();
    }
  }
};

void setExecutionCtxFactory(
    ExecutionCtxFactoryWithConf factory,
    const std::unordered_map<std::string, std::string>& sparkConf);

void setExecutionCtxFactory(ExecutionCtxFactory factory);

ExecutionCtx* createExecutionCtx();

void releaseExecutionCtx(ExecutionCtx*);

} // namespace gluten

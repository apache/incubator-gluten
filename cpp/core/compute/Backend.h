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

namespace gluten {

class ResultIterator;

struct SparkTaskInfo {
  int32_t stageId;
  int32_t partitionId;
  int64_t taskId;
};

class Backend : public std::enable_shared_from_this<Backend> {
 public:
  Backend() {}
  Backend(const std::unordered_map<std::string, std::string>& confMap) : confMap_(confMap) {}
  virtual ~Backend() = default;

  virtual std::shared_ptr<ResultIterator> getResultIterator(
      MemoryManager* memoryManager,
      const std::string& spillDir,
      const std::vector<std::shared_ptr<ResultIterator>>& inputs,
      const std::unordered_map<std::string, std::string>& sessionConf) = 0;

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

  virtual MemoryManager*
  createMemoryManager(const std::string& name, std::shared_ptr<MemoryAllocator>, std::unique_ptr<AllocationListener>) {
    throw GlutenException("createMemoryManager not implemented.");
  }

  /// This function is used to create certain converter from the format used by
  /// the backend to Spark unsafe row.
  virtual std::shared_ptr<ColumnarToRowConverter> getColumnar2RowConverter(MemoryManager* memoryManager) {
    throw GlutenException("getColumnar2RowConverter not implemented.");
  }

  virtual std::shared_ptr<RowToColumnarConverter> getRowToColumnarConverter(
      MemoryManager* memoryManager,
      struct ArrowSchema* cSchema) {
    throw GlutenException("getRowToColumnarConverter not implemented.");
  }

  virtual std::shared_ptr<ShuffleWriter> createShuffleWriter(
      int numPartitions,
      std::shared_ptr<ShuffleWriter::PartitionWriterCreator> partitionWriterCreator,
      const ShuffleWriterOptions& options,
      MemoryManager* memoryManager) {
    throw GlutenException("createShuffleWriter not implemented.");
  }

  virtual std::shared_ptr<Metrics> getMetrics(ColumnarBatchIterator* rawIter, int64_t exportNanos) {
    return nullptr;
  }

  virtual std::shared_ptr<Datasource>
  getDatasource(const std::string& filePath, MemoryManager* memoryManager, std::shared_ptr<arrow::Schema> schema) {
    throw GlutenException("getDatasource not implemented.");
  }

  virtual std::shared_ptr<ShuffleReader> createShuffleReader(
      std::shared_ptr<arrow::Schema> schema,
      ReaderOptions options,
      std::shared_ptr<arrow::MemoryPool> pool,
      MemoryManager* memoryManager) {
    return std::make_shared<ShuffleReader>(schema, options, pool);
  }

  virtual std::shared_ptr<ColumnarBatchSerializer> getColumnarBatchSerializer(
      MemoryManager* memoryManager,
      std::shared_ptr<arrow::MemoryPool> arrowPool,
      struct ArrowSchema* cSchema) {
    throw GlutenException("getColumnarBatchSerializer not implemented.");
  }

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

using BackendFactoryWithConf = std::shared_ptr<Backend> (*)(const std::unordered_map<std::string, std::string>&);
using BackendFactory = std::shared_ptr<Backend> (*)();

struct BackendFactoryContext {
  std::mutex mutex;

  enum { kBackendFactoryInvalid, kBackendFactoryDefault, kBackendFactoryWithConf } type = kBackendFactoryInvalid;

  union {
    BackendFactoryWithConf backendFactoryWithConf;
    BackendFactory backendFactory;
  };

  std::unordered_map<std::string, std::string> sparkConfs;

  void set(BackendFactoryWithConf factory, const std::unordered_map<std::string, std::string>& sparkConfs = {}) {
    std::lock_guard<std::mutex> lockGuard(mutex);

    if (type != kBackendFactoryInvalid) {
      assert(false);
      abort();
      return;
    }

    type = kBackendFactoryWithConf;
    backendFactoryWithConf = factory;
    this->sparkConfs.clear();
    for (auto& x : sparkConfs) {
      this->sparkConfs[x.first] = x.second;
    }
  }

  void set(BackendFactory factory) {
    std::lock_guard<std::mutex> lockGuard(mutex);
    if (type != kBackendFactoryInvalid) {
      assert(false);
      abort();
      return;
    }

    type = kBackendFactoryDefault;
    backendFactory = factory;
  }

  std::shared_ptr<Backend> create() {
    std::lock_guard<std::mutex> lockGuard(mutex);
    if (type == kBackendFactoryInvalid) {
      assert(false);
      abort();
      return nullptr;
    } else if (type == kBackendFactoryWithConf) {
      return backendFactoryWithConf(sparkConfs);
    } else {
      return backendFactory();
    }
  }
};

void setBackendFactory(BackendFactoryWithConf factory, const std::unordered_map<std::string, std::string>& sparkConfs);

void setBackendFactory(BackendFactory factory);

std::shared_ptr<Backend> createBackend();

} // namespace gluten

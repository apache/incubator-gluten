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
#include "operators/c2r/ArrowColumnarToRowConverter.h"
#include "operators/r2c/RowToColumnar.h"
#include "operators/writer/Datasource.h"
#include "shuffle/ArrowShuffleWriter.h"
#include "shuffle/ShuffleWriter.h"
#include "substrait/plan.pb.h"

namespace gluten {

class ResultIterator;

class Backend : public std::enable_shared_from_this<Backend> {
 public:
  Backend() {}
  Backend(const std::unordered_map<std::string, std::string>& confMap) : confMap_(confMap) {}
  virtual ~Backend() = default;

  virtual std::shared_ptr<ResultIterator> getResultIterator(
      MemoryAllocator* allocator,
      const std::string& spillDir,
      const std::vector<std::shared_ptr<ResultIterator>>& inputs,
      const std::unordered_map<std::string, std::string>& sessionConf) = 0;

  bool parsePlan(const uint8_t* data, int32_t size) {
    return parsePlan(data, size, -1, -1, -1);
  }

  /// Parse and cache the plan.
  /// Return true if parsed successfully.
  bool parsePlan(const uint8_t* data, int32_t size, int32_t stageId, int32_t partitionId, int64_t taskId) {
#ifdef GLUTEN_PRINT_DEBUG
    auto buf = std::make_shared<arrow::Buffer>(data, size);
    auto maybePlanJson = substraitFromPbToJson("Plan", *buf);
    if (maybePlanJson.status().ok()) {
      std::cout << std::string(50, '#') << " received substrait::Plan:" << std::endl;
      std::cout << "Task stageId: " << stageId << ", partitionId: " << partitionId << ", taskId: " << taskId << "; "
                << maybePlanJson.ValueOrDie() << std::endl;
    } else {
      std::cout << "Error parsing substrait plan to json: " << maybePlanJson.status().ToString() << std::endl;
    }
#endif
    return parseProtobuf(data, size, &substraitPlan_);
  }

  // Just for benchmark
  ::substrait::Plan& getPlan() {
    return substraitPlan_;
  }

  /// This function is used to create certain converter from the format used by
  /// the backend to Spark unsafe row. By default, Arrow-to-Row converter is
  /// used.
  virtual arrow::Result<std::shared_ptr<ColumnarToRowConverter>> getColumnar2RowConverter(
      MemoryAllocator* allocator,
      std::shared_ptr<ColumnarBatch> cb) {
    auto memoryPool = asWrappedArrowMemoryPool(allocator);
    std::shared_ptr<ArrowSchema> cSchema = cb->exportArrowSchema();
    std::shared_ptr<ArrowArray> cArray = cb->exportArrowArray();
    ARROW_ASSIGN_OR_RAISE(
        std::shared_ptr<arrow::RecordBatch> rb, arrow::ImportRecordBatch(cArray.get(), cSchema.get()));
    ArrowSchemaRelease(cSchema.get());
    ArrowArrayRelease(cArray.get());
    return std::make_shared<ArrowColumnarToRowConverter>(rb, memoryPool);
  }

  virtual std::shared_ptr<RowToColumnarConverter> getRowToColumnarConverter(
      MemoryAllocator* allocator,
      struct ArrowSchema* cSchema) {
    return std::make_shared<gluten::RowToColumnarConverter>(cSchema);
  }

  virtual std::shared_ptr<ShuffleWriter>
  makeShuffleWriter(int numPartitions, const SplitOptions& options, const std::string& batchType) {
    GLUTEN_ASSIGN_OR_THROW(auto shuffle_writer, ArrowShuffleWriter::create(numPartitions, std::move(options)));
    return shuffle_writer;
  }

  virtual std::shared_ptr<Metrics> getMetrics(ColumnarBatchIterator* rawIter, int64_t exportNanos) {
    return nullptr;
  }

  virtual std::shared_ptr<Datasource>
  getDatasource(const std::string& filePath, const std::string& fileName, std::shared_ptr<arrow::Schema> schema) {
    return std::make_shared<Datasource>(filePath, fileName, schema);
  }

  std::unordered_map<std::string, std::string> getConfMap() {
    return confMap_;
  }

 protected:
  ::substrait::Plan substraitPlan_;
  // static conf map
  std::unordered_map<std::string, std::string> confMap_;
};

void setBackendFactory(std::function<std::shared_ptr<Backend>()> factory);

std::shared_ptr<Backend> createBackend();

} // namespace gluten

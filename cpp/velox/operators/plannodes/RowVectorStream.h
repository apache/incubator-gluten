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

#include "compute/ResultIterator.h"
#include "memory/VeloxColumnarBatch.h"
#include "velox/exec/Driver.h"
#include "velox/exec/Operator.h"
#include "velox/exec/Task.h"

namespace gluten {

class RowVectorStream {
 public:
  explicit RowVectorStream(
      facebook::velox::exec::DriverCtx* driverCtx,
      facebook::velox::memory::MemoryPool* pool,
      ResultIterator* iterator,
      const facebook::velox::RowTypePtr& outputType)
      : driverCtx_(driverCtx), pool_(pool), outputType_(outputType), iterator_(iterator) {}

  bool hasNext();

  // Convert arrow batch to row vector, construct the new Rowvector with new outputType.
  virtual facebook::velox::RowVectorPtr next();

 protected:
  // Get the next batch from iterator_.
  std::shared_ptr<ColumnarBatch> nextInternal();

  facebook::velox::exec::DriverCtx* driverCtx_;
  facebook::velox::memory::MemoryPool* pool_;
  const facebook::velox::RowTypePtr outputType_;
  ResultIterator* iterator_;

  bool finished_{false};
};

class ValueStreamNode final : public facebook::velox::core::PlanNode {
 public:
  ValueStreamNode(
      const facebook::velox::core::PlanNodeId& id,
      const facebook::velox::RowTypePtr& outputType,
      std::shared_ptr<ResultIterator> iterator)
      : facebook::velox::core::PlanNode(id), outputType_(outputType), iterator_(std::move(iterator)) {}

  const facebook::velox::RowTypePtr& outputType() const override {
    return outputType_;
  }

  const std::vector<facebook::velox::core::PlanNodePtr>& sources() const override {
    return kEmptySources_;
  };

  ResultIterator* iterator() const {
    return iterator_.get();
  }

  std::string_view name() const override {
    return "ValueStream";
  }

  folly::dynamic serialize() const override {
    VELOX_UNSUPPORTED("ValueStream plan node is not serializable");
  }

 private:
  void addDetails(std::stringstream& stream) const override{};

  const facebook::velox::RowTypePtr outputType_;
  std::shared_ptr<ResultIterator> iterator_;
  const std::vector<facebook::velox::core::PlanNodePtr> kEmptySources_;
};

class ValueStream : public facebook::velox::exec::SourceOperator {
 public:
  ValueStream(
      int32_t operatorId,
      facebook::velox::exec::DriverCtx* driverCtx,
      std::shared_ptr<const ValueStreamNode> valueStreamNode)
      : facebook::velox::exec::SourceOperator(
            driverCtx,
            valueStreamNode->outputType(),
            operatorId,
            valueStreamNode->id(),
            valueStreamNode->name().data()) {
    ResultIterator* itr = valueStreamNode->iterator();
    rvStream_ = std::make_unique<RowVectorStream>(driverCtx, pool(), itr, outputType_);
  }

  facebook::velox::RowVectorPtr getOutput() override {
    if (finished_) {
      return nullptr;
    }
    if (rvStream_->hasNext()) {
      return rvStream_->next();
    } else {
      finished_ = true;
      return nullptr;
    }
  }

  facebook::velox::exec::BlockingReason isBlocked(facebook::velox::ContinueFuture* /* unused */) override {
    return facebook::velox::exec::BlockingReason::kNotBlocked;
  }

  bool isFinished() override {
    return finished_;
  }

 private:
  bool finished_ = false;
  std::unique_ptr<RowVectorStream> rvStream_;
};

class RowVectorStreamOperatorTranslator : public facebook::velox::exec::Operator::PlanNodeTranslator {
  std::unique_ptr<facebook::velox::exec::Operator> toOperator(
      facebook::velox::exec::DriverCtx* ctx,
      int32_t id,
      const facebook::velox::core::PlanNodePtr& node) override {
    if (auto valueStreamNode = std::dynamic_pointer_cast<const ValueStreamNode>(node)) {
      return std::make_unique<ValueStream>(id, ctx, valueStreamNode);
    }
    return nullptr;
  }
};
} // namespace gluten

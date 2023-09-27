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

namespace gluten {
class RowVectorStream {
 public:
  explicit RowVectorStream(std::shared_ptr<ResultIterator> iterator, const facebook::velox::RowTypePtr& outputType)
      : iterator_(iterator), outputType_(outputType) {}

  bool hasNext() {
    return iterator_->hasNext();
  }

  // Convert arrow batch to rowvector and use new output columns
  facebook::velox::RowVectorPtr next() {
    auto vp = std::dynamic_pointer_cast<VeloxColumnarBatch>(iterator_->next())->getRowVector();
    VELOX_DCHECK(vp != nullptr);
    return std::make_shared<facebook::velox::RowVector>(
        vp->pool(), outputType_, facebook::velox::BufferPtr(0), vp->size(), std::move(vp->children()));
  }

 private:
  std::shared_ptr<ResultIterator> iterator_;
  const facebook::velox::RowTypePtr outputType_;
};

class ValueStreamNode : public facebook::velox::core::PlanNode {
 public:
  ValueStreamNode(
      const facebook::velox::core::PlanNodeId& id,
      const facebook::velox::RowTypePtr& outputType,
      std::shared_ptr<RowVectorStream> valueStream)
      : facebook::velox::core::PlanNode(id), outputType_(outputType), valueStream_(std::move(valueStream)) {
    VELOX_CHECK_NOT_NULL(valueStream_);
  }

  const facebook::velox::RowTypePtr& outputType() const override {
    return outputType_;
  }

  const std::vector<facebook::velox::core::PlanNodePtr>& sources() const override {
    return kEmptySources;
  };

  const std::shared_ptr<RowVectorStream>& rowVectorStream() const {
    return valueStream_;
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
  std::shared_ptr<RowVectorStream> valueStream_;
  const std::vector<facebook::velox::core::PlanNodePtr> kEmptySources;
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
            "ValueStream") {
    valueStream_ = valueStreamNode->rowVectorStream();
  }

  facebook::velox::RowVectorPtr getOutput() override {
    if (valueStream_->hasNext()) {
      return valueStream_->next();
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
  std::shared_ptr<RowVectorStream> valueStream_;
};

class RowVectorStreamOperatorTranslator : public facebook::velox::exec::Operator::PlanNodeTranslator {
  std::unique_ptr<facebook::velox::exec::Operator>
  toOperator(facebook::velox::exec::DriverCtx* ctx, int32_t id, const facebook::velox::core::PlanNodePtr& node) {
    if (auto valueStreamNode = std::dynamic_pointer_cast<const ValueStreamNode>(node)) {
      return std::make_unique<ValueStream>(id, ctx, valueStreamNode);
    }
    return nullptr;
  }
};
} // namespace gluten

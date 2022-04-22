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

#include "VeloxPlanConverter.h"

#include <arrow/c/bridge.h>
#include <arrow/type_fwd.h>
#include <arrow/util/iterator.h>

#include "ArrowTypeUtils.h"
#include "arrow/c/Bridge.h"
#include "velox/buffer/Buffer.h"
#include "velox/functions/prestosql/aggregates/AverageAggregate.h"
#include "velox/functions/prestosql/aggregates/CountAggregate.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::connector;
using namespace facebook::velox::dwio::common;

namespace velox {
namespace compute {

// The Init will be called per executor.
void VeloxInitializer::Init() {
  // Setup and register.
  filesystems::registerLocalFileSystem();
  std::unique_ptr<folly::IOThreadPoolExecutor> executor =
      std::make_unique<folly::IOThreadPoolExecutor>(1);
  // auto hiveConnectorFactory = std::make_shared<hive::HiveConnectorFactory>();
  // registerConnectorFactory(hiveConnectorFactory);
  auto hiveConnector = getConnectorFactory("hive")->newConnector(
      "hive-connector", nullptr, nullptr, executor.get());
  registerConnector(hiveConnector);
  dwrf::registerDwrfReaderFactory();
  // Register Velox functions
  functions::prestosql::registerAllScalarFunctions();
  aggregate::registerSumAggregate<aggregate::SumAggregate>("sum");
  aggregate::registerAverageAggregate("avg");
  aggregate::registerCountAggregate("count");
}

void VeloxPlanConverter::setInputPlanNode(const ::substrait::AggregateRel& sagg) {
  if (sagg.has_input()) {
    setInputPlanNode(sagg.input());
  } else {
    throw std::runtime_error("Child expected");
  }
}

void VeloxPlanConverter::setInputPlanNode(const ::substrait::ProjectRel& sproject) {
  if (sproject.has_input()) {
    setInputPlanNode(sproject.input());
  } else {
    throw std::runtime_error("Child expected");
  }
}

void VeloxPlanConverter::setInputPlanNode(const ::substrait::FilterRel& sfilter) {
  if (sfilter.has_input()) {
    setInputPlanNode(sfilter.input());
  } else {
    throw std::runtime_error("Child expected");
  }
}

void VeloxPlanConverter::setInputPlanNode(const ::substrait::ReadRel& sread) {
  int32_t iterIdx = subVeloxPlanConverter_->iterAsInput(sread);
  if (iterIdx == -1) {
    return;
  }
  dsAsInput_ = false;
  if (arrowInputIters_.size() == 0) {
    throw std::runtime_error("Invalid input iterator.");
  }

  // Get the input schema of this iterator.
  uint64_t colNum = 0;
  std::vector<std::shared_ptr<facebook::velox::substrait::SubstraitParser::SubstraitType>>
      subTypeList;
  if (sread.has_base_schema()) {
    const auto& baseSchema = sread.base_schema();
    // Input names is not used. Instead, new input/output names will be created
    // because the Arrow Stream node in Velox does not support name change.
    colNum = baseSchema.names().size();
    subTypeList = subParser_->parseNamedStruct(baseSchema);
  }

  // Get the Arrow fields and output names for this plan node.
  std::vector<std::shared_ptr<arrow::Field>> arrowFields;
  arrowFields.reserve(colNum);
  std::vector<std::string> outNames;
  outNames.reserve(colNum);
  for (int idx = 0; idx < colNum; idx++) {
    auto colName = subParser_->makeNodeName(planNodeId_, idx);
    arrowFields.emplace_back(
        arrow::field(colName, toArrowTypeFromName(subTypeList[idx]->type)));
    outNames.emplace_back(colName);
  }

  // Create Arrow reader.
  std::shared_ptr<arrow::Schema> schema = arrow::schema(arrowFields);
  auto rbIter = std::move(arrowInputIters_[iterIdx]);
  // TODO: manage the iter well.
  auto maybeReader = arrow::RecordBatchReader::Make(
      std::move(*(rbIter->ToArrowRecordBatchIterator())), schema);
  if (!maybeReader.status().ok()) {
    throw std::runtime_error("Reader is not created.");
  }
  auto reader = maybeReader.ValueOrDie();

  // Create ArrowArrayStream.
  struct ArrowArrayStream veloxArrayStream;
  arrow::ExportRecordBatchReader(reader, &veloxArrayStream);
  arrowStreamIter_ = std::make_shared<ArrowArrayStream>(veloxArrayStream);

  // Create Velox ArrowStream node.
  std::vector<TypePtr> veloxTypeList;
  for (auto subType : subTypeList) {
    veloxTypeList.push_back(facebook::velox::substrait::toVeloxType(subType->type));
  }
  auto outputType = ROW(std::move(outNames), std::move(veloxTypeList));
  auto arrowStreamNode = std::make_shared<core::ArrowStreamNode>(
      nextPlanNodeId(), outputType, arrowStreamIter_);
  subVeloxPlanConverter_->insertInputNode(iterIdx, arrowStreamNode, planNodeId_);
}

void VeloxPlanConverter::setInputPlanNode(const ::substrait::Rel& srel) {
  if (srel.has_aggregate()) {
    setInputPlanNode(srel.aggregate());
  } else if (srel.has_project()) {
    setInputPlanNode(srel.project());
  } else if (srel.has_filter()) {
    setInputPlanNode(srel.filter());
  } else if (srel.has_read()) {
    setInputPlanNode(srel.read());
  } else {
    throw std::runtime_error("Rel is not supported.");
  }
}

void VeloxPlanConverter::setInputPlanNode(const ::substrait::RelRoot& sroot) {
  auto& snames = sroot.names();
  int name_idx = 0;
  for (auto& sname : snames) {
    if (name_idx == 0 && sname == "fake_arrow_output") {
      fakeArrowOutput_ = true;
    }
    name_idx += 1;
  }
  if (sroot.has_input()) {
    setInputPlanNode(sroot.input());
  } else {
    throw std::runtime_error("Input is expected in RelRoot.");
  }
}

std::shared_ptr<const core::PlanNode> VeloxPlanConverter::getVeloxPlanNode(
    const ::substrait::Plan& splan) {
  // In fact, only one RelRoot is expected here.
  for (auto& srel : splan.relations()) {
    if (srel.has_root()) {
      setInputPlanNode(srel.root());
    }
    if (srel.has_rel()) {
      setInputPlanNode(srel.rel());
    }
  }
  return subVeloxPlanConverter_->toVeloxPlan(splan);
}

std::string VeloxPlanConverter::nextPlanNodeId() {
  auto id = fmt::format("{}", planNodeId_);
  planNodeId_++;
  return id;
}

std::shared_ptr<gluten::RecordBatchResultIterator>
VeloxPlanConverter::GetResultIterator() {
  std::shared_ptr<gluten::RecordBatchResultIterator> resIter;
  const std::shared_ptr<const core::PlanNode> planNode = getVeloxPlanNode(plan_);
  auto wholestageIter = std::make_shared<WholeStageResIterFirstStage>(
      planNode, subVeloxPlanConverter_->getPartitionIndex(),
      subVeloxPlanConverter_->getPaths(), subVeloxPlanConverter_->getStarts(),
      subVeloxPlanConverter_->getLengths(), fakeArrowOutput_);
  return std::make_shared<gluten::RecordBatchResultIterator>(std::move(wholestageIter));
}

std::shared_ptr<gluten::RecordBatchResultIterator> VeloxPlanConverter::GetResultIterator(
    std::vector<std::shared_ptr<gluten::RecordBatchResultIterator>> inputs) {
  std::shared_ptr<gluten::RecordBatchResultIterator> resIter;
  arrowInputIters_ = std::move(inputs);
  const std::shared_ptr<const core::PlanNode> planNode = getVeloxPlanNode(plan_);
  auto wholestageIter =
      std::make_shared<WholeStageResIterMiddleStage>(planNode, fakeArrowOutput_);
  return std::make_shared<gluten::RecordBatchResultIterator>(std::move(wholestageIter));
}

class VeloxPlanConverter::WholeStageResIter {
 public:
  virtual ~WholeStageResIter() = default;

  arrow::Status CopyBuffer(const uint8_t* from, uint8_t* to, int64_t copy_bytes) {
    // ARROW_ASSIGN_OR_RAISE(*out, AllocateBuffer(size * length, memory_pool_));
    // uint8_t* buffer_data = (*out)->mutable_data();
    std::memcpy(to, from, copy_bytes);
    // double val = *(double*)buffer_data;
    // std::cout << "buffler val: " << val << std::endl;
    return arrow::Status::OK();
  }

  /* This method converts Velox RowVector into Arrow RecordBatch based on Velox's
     Arrow conversion implementation, in which memcopy is not needed for fixed-width data
     types, but is conducted in String conversion. The output batch will be the input of
     Columnar Shuffle.
  */
  void toRealArrowBatch(const RowVectorPtr& rv, uint64_t numRows,
                        const RowTypePtr& outTypes,
                        std::shared_ptr<arrow::RecordBatch>* out) {
    uint32_t colNum = outTypes->size();
    std::vector<std::shared_ptr<arrow::Array>> outArrays;
    outArrays.reserve(colNum);
    std::vector<std::shared_ptr<arrow::Field>> retTypes;
    retTypes.reserve(colNum);
    for (uint32_t idx = 0; idx < colNum; idx++) {
      arrow::ArrayData outData;
      outData.length = numRows;
      auto vec = rv->childAt(idx);
      // FIXME: need to release this.
      ArrowArray arrowArray;
      exportToArrow(vec, arrowArray, veloxPool_.get());
      outData.buffers.resize(arrowArray.n_buffers);
      outData.null_count = arrowArray.null_count;
      // Validity buffer
      std::shared_ptr<arrow::Buffer> valBuffer = nullptr;
      if (arrowArray.null_count > 0) {
        arrowArray.buffers[0];
        // FIXME: set BitMap
      }
      outData.buffers[0] = valBuffer;
      auto colType = outTypes->childAt(idx);
      auto colArrowType = toArrowType(colType);
      // TODO: use the names in RelRoot.
      auto colName = "res_" + std::to_string(idx);
      retTypes.emplace_back(arrow::field(colName, colArrowType));
      outData.type = colArrowType;
      if (colType->isPrimitiveType() && !facebook::velox::substrait::isString(colType)) {
        auto dataBuffer = std::make_shared<arrow::Buffer>(
            static_cast<const uint8_t*>(arrowArray.buffers[1]),
            facebook::velox::substrait::bytesOfType(colType) * numRows);
        outData.buffers[1] = dataBuffer;
      } else if (facebook::velox::substrait::isString(colType)) {
        auto offsets = static_cast<const int32_t*>(arrowArray.buffers[1]);
        int32_t stringDataSize = offsets[numRows];
        auto valueBuffer = std::make_shared<arrow::Buffer>(
            static_cast<const uint8_t*>(arrowArray.buffers[2]), stringDataSize);
        auto offsetBuffer = std::make_shared<arrow::Buffer>(
            static_cast<const uint8_t*>(arrowArray.buffers[1]),
            sizeof(int32_t) * (numRows + 1));
        outData.buffers[1] = offsetBuffer;
        outData.buffers[2] = valueBuffer;
      }
      std::shared_ptr<arrow::Array> outArray =
          MakeArray(std::make_shared<arrow::ArrayData>(std::move(outData)));
      outArrays.emplace_back(outArray);
      // int ref_count = vec->mutableValues(0)->refCount();
    }
    // auto typed_array = std::dynamic_pointer_cast<arrow::StringArray>(out_arrays[0]);
    // for (int i = 0; i < typed_array->length(); i++) {
    //   std::cout << "array val: " << typed_array->GetString(i) << std::endl;
    // }
    *out = arrow::RecordBatch::Make(arrow::schema(retTypes), numRows, outArrays);
  }

  /* This method converts Velox RowVector into Faked Arrow RecordBatch. Velox's impl is
    used for fixed-width data types. For String conversion, a faked array is constructed.
    The output batch will be converted into Unsafe Row in Velox-to-Row converter.
  */
  void toFakedArrowBatch(const RowVectorPtr& rv, uint64_t numRows,
                         const RowTypePtr& outTypes,
                         std::shared_ptr<arrow::RecordBatch>* out) {
    uint32_t colNum = outTypes->size();
    std::vector<std::shared_ptr<arrow::Array>> outArrays;
    outArrays.reserve(colNum);
    std::vector<std::shared_ptr<arrow::Field>> retTypes;
    retTypes.reserve(colNum);
    for (uint32_t idx = 0; idx < colNum; idx++) {
      arrow::ArrayData outData;
      outData.length = numRows;
      auto vec = rv->childAt(idx);
      auto colType = outTypes->childAt(idx);
      auto colArrowType = toArrowType(colType);
      // TODO: use the names in RelRoot.
      auto colName = "res_" + std::to_string(idx);
      retTypes.emplace_back(arrow::field(colName, colArrowType));
      if (colType->isPrimitiveType() && !facebook::velox::substrait::isString(colType)) {
        outData.type = colArrowType;
        // FIXME: need to release this.
        ArrowArray arrowArray;
        exportToArrow(vec, arrowArray, veloxPool_.get());
        outData.buffers.resize(arrowArray.n_buffers);
        outData.null_count = arrowArray.null_count;
        auto dataBuffer = std::make_shared<arrow::Buffer>(
            static_cast<const uint8_t*>(arrowArray.buffers[1]),
            facebook::velox::substrait::bytesOfType(colType) * numRows);
        // Validity buffer
        std::shared_ptr<arrow::Buffer> valBuffer = nullptr;
        if (arrowArray.null_count > 0) {
          arrowArray.buffers[0];
          // FIXME: set BitMap
        }
        outData.buffers[0] = valBuffer;
        outData.buffers[1] = dataBuffer;
      } else if (facebook::velox::substrait::isString(colType)) {
        // Will construct a faked String Array.
        outData.buffers.resize(3);
        outData.null_count = 0;
        outData.type = arrow::utf8();
        auto strValues = vec->asFlatVector<StringView>()->rawValues();
        auto valBuffer = std::make_shared<arrow::Buffer>(
            reinterpret_cast<const uint8_t*>(strValues), 8 * numRows);
        outData.buffers[0] = nullptr;
        outData.buffers[1] = valBuffer;
        outData.buffers[2] = valBuffer;
      }
      std::shared_ptr<arrow::Array> outArray =
          MakeArray(std::make_shared<arrow::ArrayData>(std::move(outData)));
      outArrays.emplace_back(outArray);
      // int ref_count = vec->mutableValues(0)->refCount();
    }
    // auto typed_array = std::dynamic_pointer_cast<arrow::StringArray>(out_arrays[0]);
    // for (int i = 0; i < typed_array->length(); i++) {
    //   std::cout << "array val: " << typed_array->GetString(i) << std::endl;
    // }
    *out = arrow::RecordBatch::Make(arrow::schema(retTypes), numRows, outArrays);
  }

  arrow::MemoryPool* memoryPool_ = arrow::default_memory_pool();
  std::unique_ptr<memory::MemoryPool> veloxPool_{memory::getDefaultScopedMemoryPool()};
  std::shared_ptr<const core::PlanNode> planNode_;
  test::CursorParameters params_;
  std::unique_ptr<test::TaskCursor> cursor_;
  std::function<void(exec::Task*)> addSplits_;
  bool fakeArrowOutput_;
  bool mayHasNext_ = true;
  // FIXME: use the setted one
  uint64_t batchSize_ = 10000;
};

class VeloxPlanConverter::WholeStageResIterFirstStage : public WholeStageResIter {
 public:
  WholeStageResIterFirstStage(const std::shared_ptr<const core::PlanNode>& planNode,
                              const u_int32_t& index,
                              const std::vector<std::string>& paths,
                              const std::vector<u_int64_t>& starts,
                              const std::vector<u_int64_t>& lengths,
                              const bool& fakeArrowOutput)
      : index_(index), paths_(paths), starts_(starts), lengths_(lengths) {
    planNode_ = planNode;
    fakeArrowOutput_ = fakeArrowOutput;
    std::vector<std::shared_ptr<ConnectorSplit>> connectorSplits;
    for (int idx = 0; idx < paths.size(); idx++) {
      auto path = paths[idx];
      auto start = starts[idx];
      auto length = lengths[idx];
      auto split = std::make_shared<hive::HiveConnectorSplit>(
          "hive-connector", path, FileFormat::ORC, start, length);
      connectorSplits.push_back(split);
    }
    splits_.reserve(connectorSplits.size());
    for (const auto& connectorSplit : connectorSplits) {
      splits_.emplace_back(exec::Split(folly::copy(connectorSplit), -1));
    }
    params_.planNode = planNode;
    cursor_ = std::make_unique<test::TaskCursor>(params_);
    addSplits_ = [&](Task* task) {
      if (noMoreSplits_) {
        return;
      }
      for (auto& split : splits_) {
        task->addSplit("0", std::move(split));
      }
      task->noMoreSplits("0");
      noMoreSplits_ = true;
    };
  }

  arrow::Result<std::shared_ptr<arrow::RecordBatch>> Next() {
    std::shared_ptr<arrow::RecordBatch> out = nullptr;
    if (!mayHasNext_) {
      return out;
    }
    addSplits_(cursor_->task().get());
    if (cursor_->moveNext()) {
      RowVectorPtr result = cursor_->current();
      uint64_t numRows = result->size();
      if (numRows == 0) {
        return out;
      }
      auto outTypes = planNode_->outputType();
      if (fakeArrowOutput_) {
        toFakedArrowBatch(result, numRows, outTypes, &out);
      } else {
        toRealArrowBatch(result, numRows, outTypes, &out);
      }
      return out;
    }
    mayHasNext_ = false;
    return out;
  }

 private:
  u_int32_t index_;
  std::vector<std::string> paths_;
  std::vector<u_int64_t> starts_;
  std::vector<u_int64_t> lengths_;
  std::vector<exec::Split> splits_;
  bool noMoreSplits_ = false;
};

class VeloxPlanConverter::WholeStageResIterMiddleStage : public WholeStageResIter {
 public:
  WholeStageResIterMiddleStage(const std::shared_ptr<const core::PlanNode>& planNode,
                               const bool& fakeArrowOutput) {
    planNode_ = planNode;
    fakeArrowOutput_ = fakeArrowOutput;
    params_.planNode = planNode;
    cursor_ = std::make_unique<test::TaskCursor>(params_);
    addSplits_ = [&](Task* task) {
      if (noMoreSplits_) {
        return;
      }
      task->noMoreSplits("0");
      noMoreSplits_ = true;
    };
  }

  arrow::Result<std::shared_ptr<arrow::RecordBatch>> Next() {
    std::shared_ptr<arrow::RecordBatch> out = nullptr;
    if (!mayHasNext_) {
      return out;
    }
    addSplits_(cursor_->task().get());
    if (cursor_->moveNext()) {
      RowVectorPtr result = cursor_->current();
      uint64_t numRows = result->size();
      if (numRows == 0) {
        return out;
      }
      auto outTypes = planNode_->outputType();
      if (fakeArrowOutput_) {
        toFakedArrowBatch(result, numRows, outTypes, &out);
      } else {
        toRealArrowBatch(result, numRows, outTypes, &out);
      }
      // arrow::PrettyPrint(*out->get(), 2, &std::cout);
      return out;
    }
    mayHasNext_ = false;
    return out;
  }

 private:
  bool noMoreSplits_ = false;
};

}  // namespace compute
}  // namespace velox

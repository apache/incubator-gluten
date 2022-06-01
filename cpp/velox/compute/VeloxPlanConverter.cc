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
#include "arrow/c/bridge.h"
#include "velox/buffer/Buffer.h"
#include "velox/functions/prestosql/aggregates/AverageAggregate.h"
#include "velox/functions/prestosql/aggregates/CountAggregate.h"
#include "velox/functions/prestosql/aggregates/MinMaxAggregates.h"
#include "velox/functions/sparksql/Register.h"

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
  functions::sparksql::registerFunctions("");
  aggregate::registerSumAggregate<aggregate::SumAggregate>("sum");
  aggregate::registerAverageAggregate("avg");
  aggregate::registerCountAggregate("count");
  aggregate::registerMinMaxAggregate<aggregate::MinAggregate,
                                     aggregate::NonNumericMinAggregate>("min");
  aggregate::registerMinMaxAggregate<aggregate::MaxAggregate,
                                     aggregate::NonNumericMaxAggregate>("max");
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

void VeloxPlanConverter::setInputPlanNode(const ::substrait::JoinRel& sjoin) {
  if (sjoin.has_left()) {
    setInputPlanNode(sjoin.left());
  } else {
    throw std::runtime_error("Left child expected");
  }

  if (sjoin.has_right()) {
    setInputPlanNode(sjoin.right());
  } else {
    throw std::runtime_error("Right child expected");
  }
}

void VeloxPlanConverter::setInputPlanNode(const ::substrait::ReadRel& sread) {
  int32_t iterIdx = subVeloxPlanConverter_->streamIsInput(sread);
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
  } else if (srel.has_join()) {
    setInputPlanNode(srel.join());
  } else {
    throw std::runtime_error("Rel is not supported: " + srel.DebugString());
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
  auto planNode = subVeloxPlanConverter_->toVeloxPlan(splan);
#ifdef DEBUG
  std::cout << "Plan Node: " << std::endl << planNode->toString(true, true) << std::endl;
#endif
  return planNode;
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
  // Move the velox pool and the iterator will manage it.
  auto wholestageIter = std::make_shared<WholeStageResIterFirstStage>(
      std::move(veloxPool_), planNode, subVeloxPlanConverter_->getPartitionIndex(),
      subVeloxPlanConverter_->getPaths(), subVeloxPlanConverter_->getStarts(),
      subVeloxPlanConverter_->getLengths(), fakeArrowOutput_);
  return std::make_shared<gluten::RecordBatchResultIterator>(std::move(wholestageIter));
}

std::shared_ptr<gluten::RecordBatchResultIterator> VeloxPlanConverter::GetResultIterator(
    std::vector<std::shared_ptr<gluten::RecordBatchResultIterator>> inputs) {
  std::shared_ptr<gluten::RecordBatchResultIterator> resIter;
  arrowInputIters_ = std::move(inputs);
  const std::shared_ptr<const core::PlanNode> planNode = getVeloxPlanNode(plan_);
  // Move the velox pool and the iterator will manage it.
  auto wholestageIter = std::make_shared<WholeStageResIterMiddleStage>(
      std::move(veloxPool_), planNode, fakeArrowOutput_);
  return std::make_shared<gluten::RecordBatchResultIterator>(std::move(wholestageIter));
}

class VeloxPlanConverter::WholeStageResIter {
 public:
  virtual ~WholeStageResIter() = default;

  /// This method converts Velox RowVector into Arrow RecordBatch based on Velox's
  /// Arrow conversion implementation, in which memcopy is not needed for fixed-width data
  /// types, but is conducted in String conversion. The output batch will be the input of
  /// Columnar Shuffle.
  void toRealArrowBatch(const RowVectorPtr& rv, uint64_t numRows,
                        const RowTypePtr& outTypes,
                        std::shared_ptr<arrow::RecordBatch>* out) {
    ArrowArray cArray{};
    ArrowSchema cSchema{};
    exportToArrow(rv, cArray, veloxPool_.get());
    exportToArrow(outTypes, cSchema);
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> batch =
        arrow::ImportRecordBatch(&cArray, &cSchema);

    if (!batch.status().ok()) {
      throw std::runtime_error("Failed to import to Arrow record batch");
    }
    *out = batch.ValueOrDie();
  }

  /// This method converts Velox RowVector into Faked Arrow RecordBatch. Velox's impl is
  /// used for fixed-width data types. For String conversion, a faked array is
  /// constructed. The output batch will be converted into Unsafe Row in Velox-to-Row
  /// converter.
  void toFakedArrowBatch(const RowVectorPtr& rv, uint64_t numRows,
                         const RowTypePtr& outTypes,
                         std::shared_ptr<arrow::RecordBatch>* out) {
    // not to make fake batches as of now
    toRealArrowBatch(rv, numRows, outTypes, out);
  }

  arrow::Result<std::shared_ptr<arrow::RecordBatch>> Next() {
    std::shared_ptr<arrow::RecordBatch> out = nullptr;
    if (!mayHaveNext_) {
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
    mayHaveNext_ = false;
    return out;
  }

  std::unique_ptr<memory::MemoryPool> veloxPool_;
  std::shared_ptr<const core::PlanNode> planNode_;
  test::CursorParameters params_;
  std::unique_ptr<test::TaskCursor> cursor_;
  std::function<void(exec::Task*)> addSplits_;
  bool fakeArrowOutput_;
  bool mayHaveNext_ = true;
  // TODO: use the setted one.
  uint64_t batchSize_ = 10000;
};

class VeloxPlanConverter::WholeStageResIterFirstStage : public WholeStageResIter {
 public:
  WholeStageResIterFirstStage(std::unique_ptr<memory::MemoryPool> veloxPool,
                              const std::shared_ptr<const core::PlanNode>& planNode,
                              const u_int32_t index,
                              const std::vector<std::string>& paths,
                              const std::vector<u_int64_t>& starts,
                              const std::vector<u_int64_t>& lengths,
                              const bool fakeArrowOutput)
      : index_(index), paths_(paths), starts_(starts), lengths_(lengths) {
    veloxPool_ = std::move(veloxPool);
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
  WholeStageResIterMiddleStage(std::unique_ptr<memory::MemoryPool> veloxPool,
                               const std::shared_ptr<const core::PlanNode>& planNode,
                               const bool fakeArrowOutput) {
    veloxPool_ = std::move(veloxPool);
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

 private:
  bool noMoreSplits_ = false;
};

}  // namespace compute
}  // namespace velox

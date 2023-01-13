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
#include <filesystem>

#include "VeloxBackend.h"

#include <folly/executors/IOThreadPoolExecutor.h>

#include "ArrowTypeUtils.h"
#include "RegistrationAllFunctions.h"
#include "VeloxBridge.h"
#include "compute/Backend.h"
#include "compute/ResultIterator.h"
#include "include/arrow/c/bridge.h"
#include "velox/common/file/FileSystems.h"
#ifdef VELOX_ENABLE_HDFS
#include "velox/connectors/hive/storage_adapters/hdfs/HdfsFileSystem.h"
#endif
#ifdef VELOX_ENABLE_S3
#include "velox/connectors/hive/storage_adapters/s3fs/S3FileSystem.h"
#endif
#include "velox/common/memory/MmapAllocator.h"
#include "velox/dwio/dwrf/reader/DwrfReader.h"
#include "velox/dwio/dwrf/writer/Writer.h"
#include "velox/dwio/parquet/RegisterParquetReader.h"
#include "velox/exec/Operator.h"
#include "velox/vector/arrow/Bridge.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::connector;
using namespace facebook::velox::dwio::common;
using namespace facebook::velox::parquet;

namespace gluten {

// The Init will be called per executor.
void VeloxInitializer::Init(std::unordered_map<std::string, std::string> conf) {
  // Setup and register.
  filesystems::registerLocalFileSystem();

  // TODO(yuan): move to seperate func initcache()
  auto key = conf.find(kVeloxCacheEnabled);
  if (key != conf.end() && boost::algorithm::to_lower_copy(conf[kVeloxCacheEnabled]) == "true") {
    uint64_t cacheSize = std::stol(kVeloxCacheSizeDefault);
    int32_t cacheShards = std::stoi(kVeloxCacheShardsDefault);
    int32_t ioTHreads = std::stoi(kVeloxCacheIOThreadsDefault);
    std::string cachePathPrefix = kVeloxCachePathDefault;
    for (auto& [k, v] : conf) {
      if (k == kVeloxCacheSize)
        cacheSize = std::stol(v);
      if (k == kVeloxCacheShards)
        cacheShards = std::stoi(v);
      if (k == kVeloxCachePath)
        cachePathPrefix = v;
      if (k == kVeloxCacheIOThreads)
        ioTHreads = std::stoi(v);
    }
    std::string cachePath = cachePathPrefix + "/cache." + genUuid();
    cacheExecutor_ = std::make_unique<folly::IOThreadPoolExecutor>(cacheShards);
    ioExecutor_ = std::make_unique<folly::IOThreadPoolExecutor>(ioTHreads);
    auto ssd = std::make_unique<cache::SsdCache>(cachePath, cacheSize, cacheShards, cacheExecutor_.get());

    std::error_code ec;
    const std::filesystem::space_info si = std::filesystem::space(cachePathPrefix, ec);
    if (si.available < cacheSize) {
      VELOX_FAIL(
          "not enough space for cache in " + cachePath + " cacha size: " + std::to_string(cacheSize) +
          "free space: " + std::to_string(si.available));
    }

    memory::MmapAllocator::Options options;
    // TODO(yuan): should try to parse the offheap memory size here:
    uint64_t memoryBytes = 200L << 30;
    options.capacity = memoryBytes;

    auto allocator = std::make_shared<memory::MmapAllocator>(options);
    mappedMemory_ = std::make_shared<cache::AsyncDataCache>(allocator, memoryBytes, std::move(ssd));

    // register as default instance, will be used in parquet reader
    memory::MemoryAllocator::setDefaultInstance(mappedMemory_.get());
    VELOX_CHECK_NOT_NULL(dynamic_cast<cache::AsyncDataCache*>(mappedMemory_.get()));
    LOG(INFO) << "STARTUP: Using AsyncDataCache"
              << ", cache size: " << cacheSize << ", cache shards: " << cacheShards << ", IO threads: " << ioTHreads
              << ", cache path: " << cachePath;
  }

  std::unordered_map<std::string, std::string> configurationValues;

#ifdef VELOX_ENABLE_HDFS
  filesystems::registerHdfsFileSystem();
  // TODO(yuan): should read hdfs client conf from hdfs-client.xml from
  // LIBHDFS3_CONF
  std::string hdfsUri = conf["spark.hadoop.fs.defaultFS"];
  const char* envHdfsUri = std::getenv("VELOX_HDFS");
  if (envHdfsUri != nullptr) {
    hdfsUri = std::string(envHdfsUri);
  }
  auto hdfsHostWithPort = hdfsUri.substr(hdfsUri.find(":") + 3);
  auto hdfsPort = hdfsHostWithPort.substr(hdfsHostWithPort.find(":") + 1);
  auto hdfsHost = hdfsHostWithPort.substr(0, hdfsHostWithPort.find(":"));
  std::unordered_map<std::string, std::string> hdfsConfig({{"hive.hdfs.host", hdfsHost}, {"hive.hdfs.port", hdfsPort}});
  configurationValues.merge(hdfsConfig);
#endif

#ifdef VELOX_ENABLE_S3
  filesystems::registerS3FileSystem();

  std::string awsAccessKey = conf["spark.hadoop.fs.s3a.access.key"];
  std::string awsSecretKey = conf["spark.hadoop.fs.s3a.secret.key"];
  std::string awsEndpoint = conf["spark.hadoop.fs.s3a.endpoint"];
  std::string sslEnabled = conf["spark.hadoop.fs.s3a.connection.ssl.enabled"];
  std::string pathStyleAccess = conf["spark.hadoop.fs.s3a.path.style.access"];
  std::string useInstanceCredentials = conf["spark.hadoop.fs.s3a.use.instance.credentials"];

  const char* envAwsAccessKey = std::getenv("AWS_ACCESS_KEY_ID");
  if (envAwsAccessKey != nullptr) {
    awsAccessKey = std::string(envAwsAccessKey);
  }
  const char* envAwsSecretKey = std::getenv("AWS_SECRET_ACCESS_KEY");
  if (envAwsSecretKey != nullptr) {
    awsSecretKey = std::string(envAwsSecretKey);
  }
  const char* envAwsEndpoint = std::getenv("AWS_ENDPOINT");
  if (envAwsEndpoint != nullptr) {
    awsEndpoint = std::string(envAwsEndpoint);
  }

  std::unordered_map<std::string, std::string> S3Config({});
  if (useInstanceCredentials == "true") {
    S3Config.insert({
        {"hive.s3.use-instance-credentials", useInstanceCredentials},
    });
  } else {
    S3Config.insert({
        {"hive.s3.aws-access-key", awsAccessKey},
        {"hive.s3.aws-secret-key", awsSecretKey},
        {"hive.s3.endpoint", awsEndpoint},
        {"hive.s3.ssl.enabled", sslEnabled},
        {"hive.s3.path-style-access", pathStyleAccess},
    });
  }
  configurationValues.merge(S3Config);
#endif

  auto properties = std::make_shared<const core::MemConfig>(configurationValues);
  auto hiveConnector = getConnectorFactory(connector::hive::HiveConnectorFactory::kHiveConnectorName)
                           ->newConnector(kHiveConnectorId, properties, ioExecutor_.get());

  registerConnector(hiveConnector);
  facebook::velox::parquet::registerParquetReaderFactory(ParquetReaderType::NATIVE);
  dwrf::registerDwrfReaderFactory();
  // Register Velox functions
  registerAllFunctions();
}

void VeloxBackend::setInputPlanNode(const ::substrait::FetchRel& fetchRel) {
  if (fetchRel.has_input()) {
    setInputPlanNode(fetchRel.input());
  } else {
    throw std::runtime_error("Child expected");
  }
}

void VeloxBackend::setInputPlanNode(const ::substrait::ExpandRel& sexpand) {
  if (sexpand.has_input()) {
    setInputPlanNode(sexpand.input());
  } else {
    throw std::runtime_error("Child expected");
  }
}

void VeloxBackend::setInputPlanNode(const ::substrait::SortRel& ssort) {
  if (ssort.has_input()) {
    setInputPlanNode(ssort.input());
  } else {
    throw std::runtime_error("Child expected");
  }
}

void VeloxBackend::setInputPlanNode(const ::substrait::WindowRel& swindow) {
  if (swindow.has_input()) {
    setInputPlanNode(swindow.input());
  } else {
    throw std::runtime_error("Child expected");
  }
}

void VeloxBackend::setInputPlanNode(const ::substrait::AggregateRel& sagg) {
  if (sagg.has_input()) {
    setInputPlanNode(sagg.input());
  } else {
    throw std::runtime_error("Child expected");
  }
}

void VeloxBackend::setInputPlanNode(const ::substrait::ProjectRel& sproject) {
  if (sproject.has_input()) {
    setInputPlanNode(sproject.input());
  } else {
    throw std::runtime_error("Child expected");
  }
}

void VeloxBackend::setInputPlanNode(const ::substrait::FilterRel& sfilter) {
  if (sfilter.has_input()) {
    setInputPlanNode(sfilter.input());
  } else {
    throw std::runtime_error("Child expected");
  }
}

void VeloxBackend::setInputPlanNode(const ::substrait::JoinRel& sjoin) {
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

void VeloxBackend::setInputPlanNode(const ::substrait::ReadRel& sread) {
  int32_t iterIdx = subVeloxPlanConverter_->streamIsInput(sread);
  if (iterIdx == -1) {
    return;
  }
  if (arrowInputIters_.size() == 0) {
    throw std::runtime_error("Invalid input iterator.");
  }

  // Get the input schema of this iterator.
  uint64_t colNum = 0;
  std::vector<std::shared_ptr<facebook::velox::substrait::SubstraitParser::SubstraitType>> subTypeList;
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
    arrowFields.emplace_back(arrow::field(colName, toArrowTypeFromName(subTypeList[idx]->type)));
    outNames.emplace_back(colName);
  }

  // Create Arrow reader.
  std::shared_ptr<arrow::Schema> schema = arrow::schema(arrowFields);
  auto arrayIter = std::move(arrowInputIters_[iterIdx]);
  // Create ArrowArrayStream.
  struct ArrowArrayStream veloxArrayStream;
  GLUTEN_THROW_NOT_OK(ExportArrowArray(schema, arrayIter->ToArrowArrayIterator(), &veloxArrayStream));
  auto arrowStream = std::make_shared<ArrowArrayStream>(veloxArrayStream);

  // Create Velox ArrowStream node.
  std::vector<TypePtr> veloxTypeList;
  for (auto subType : subTypeList) {
    veloxTypeList.push_back(facebook::velox::substrait::toVeloxType(subType->type));
  }
  auto outputType = ROW(std::move(outNames), std::move(veloxTypeList));
  auto arrowStreamNode = std::make_shared<core::ArrowStreamNode>(
      nextPlanNodeId(), outputType, arrowStream, GetDefaultWrappedVeloxMemoryPool());
  subVeloxPlanConverter_->insertInputNode(iterIdx, arrowStreamNode, planNodeId_);
}

void VeloxBackend::setInputPlanNode(const ::substrait::Rel& srel) {
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
  } else if (srel.has_sort()) {
    setInputPlanNode(srel.sort());
  } else if (srel.has_expand()) {
    setInputPlanNode(srel.expand());
  } else if (srel.has_fetch()) {
    setInputPlanNode(srel.fetch());
  } else if (srel.has_window()) {
    setInputPlanNode(srel.window());
  } else {
    throw std::runtime_error("Rel is not supported: " + srel.DebugString());
  }
}

void VeloxBackend::setInputPlanNode(const ::substrait::RelRoot& sroot) {
  // Output names can be got from RelRoot, but are not used currently.
  if (sroot.has_input()) {
    setInputPlanNode(sroot.input());
  } else {
    throw std::runtime_error("Input is expected in RelRoot.");
  }
}

std::shared_ptr<const core::PlanNode> VeloxBackend::getVeloxPlanNode(const ::substrait::Plan& splan) {
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
#ifdef GLUTEN_PRINT_DEBUG
  std::cout << "Plan Node: " << std::endl << planNode->toString(true, true) << std::endl;
#endif
  return planNode;
}

std::string VeloxBackend::nextPlanNodeId() {
  auto id = fmt::format("{}", planNodeId_);
  planNodeId_++;
  return id;
}

void VeloxBackend::getInfoAndIds(
    std::unordered_map<core::PlanNodeId, std::shared_ptr<facebook::velox::substrait::SplitInfo>> splitInfoMap,
    std::unordered_set<core::PlanNodeId> leafPlanNodeIds,
    std::vector<std::shared_ptr<facebook::velox::substrait::SplitInfo>>& scanInfos,
    std::vector<core::PlanNodeId>& scanIds,
    std::vector<core::PlanNodeId>& streamIds) {
  if (splitInfoMap.size() == 0) {
    throw std::runtime_error("At least one data source info is required. Can be scan or stream info.");
  }
  for (const auto& leafPlanNodeId : leafPlanNodeIds) {
    if (splitInfoMap.find(leafPlanNodeId) == splitInfoMap.end()) {
      throw std::runtime_error("Could not find leafPlanNodeId.");
    }
    auto splitInfo = splitInfoMap[leafPlanNodeId];
    if (splitInfo->isStream) {
      streamIds.emplace_back(leafPlanNodeId);
    } else {
      scanInfos.emplace_back(splitInfo);
      scanIds.emplace_back(leafPlanNodeId);
    }
  }
}

std::shared_ptr<ResultIterator> VeloxBackend::GetResultIterator(
    MemoryAllocator* allocator,
    std::vector<std::shared_ptr<ResultIterator>> inputs) {
  if (inputs.size() > 0) {
    arrowInputIters_ = std::move(inputs);
  }
  planNode_ = getVeloxPlanNode(plan_);

  // Scan node can be required.
  std::vector<std::shared_ptr<facebook::velox::substrait::SplitInfo>> scanInfos;
  std::vector<core::PlanNodeId> scanIds;
  std::vector<core::PlanNodeId> streamIds;

  // Separate the scan ids and stream ids, and get the scan infos.
  getInfoAndIds(subVeloxPlanConverter_->splitInfos(), planNode_->leafPlanNodeIds(), scanInfos, scanIds, streamIds);

  auto veloxPool = AsWrappedVeloxMemoryPool(allocator);
  if (scanInfos.size() == 0) {
    // Source node is not required.
    auto wholestageIter =
        std::make_unique<WholeStageResultIteratorMiddleStage>(veloxPool, planNode_, streamIds, confMap_);
    return std::make_shared<ResultIterator>(std::move(wholestageIter), shared_from_this());
  } else {
    auto wholestageIter = std::make_unique<WholeStageResultIteratorFirstStage>(
        veloxPool, planNode_, scanIds, scanInfos, streamIds, confMap_);
    return std::make_shared<ResultIterator>(std::move(wholestageIter), shared_from_this());
  }
}

std::shared_ptr<ResultIterator> VeloxBackend::GetResultIterator(
    MemoryAllocator* allocator,
    const std::vector<std::shared_ptr<facebook::velox::substrait::SplitInfo>>& setScanInfos) {
  planNode_ = getVeloxPlanNode(plan_);

  // In test, use setScanInfos to replace the one got from Substrait.
  std::vector<std::shared_ptr<facebook::velox::substrait::SplitInfo>> scanInfos;
  std::vector<core::PlanNodeId> scanIds;
  std::vector<core::PlanNodeId> streamIds;

  // Separate the scan ids and stream ids, and get the scan infos.
  getInfoAndIds(subVeloxPlanConverter_->splitInfos(), planNode_->leafPlanNodeIds(), scanInfos, scanIds, streamIds);

  auto veloxPool = AsWrappedVeloxMemoryPool(allocator);

  auto wholestageIter = std::make_unique<WholeStageResultIteratorFirstStage>(
      veloxPool, planNode_, scanIds, setScanInfos, streamIds, confMap_);
  return std::make_shared<ResultIterator>(std::move(wholestageIter), shared_from_this());
}

arrow::Result<std::shared_ptr<ColumnarToRowConverter>> VeloxBackend::getColumnar2RowConverter(
    MemoryAllocator* allocator,
    std::shared_ptr<ColumnarBatch> cb) {
  auto veloxBatch = std::dynamic_pointer_cast<VeloxColumnarBatch>(cb);
  if (veloxBatch != nullptr) {
    auto arrowPool = AsWrappedArrowMemoryPool(allocator);
    auto veloxPool = AsWrappedVeloxMemoryPool(allocator);
    return std::make_shared<VeloxColumnarToRowConverter>(veloxBatch->getFlattenedRowVector(), arrowPool, veloxPool);
  } else {
    return Backend::getColumnar2RowConverter(allocator, cb);
  }
}

std::shared_ptr<arrow::Schema> VeloxBackend::GetOutputSchema() {
  if (output_schema_ == nullptr) {
    cacheOutputSchema(planNode_);
  }
  return output_schema_;
}

void VeloxBackend::cacheOutputSchema(const std::shared_ptr<const core::PlanNode>& planNode) {
  ArrowSchema arrowSchema{};
  exportToArrow(BaseVector::create(planNode->outputType(), 0, GetDefaultWrappedVeloxMemoryPool()), arrowSchema);
  GLUTEN_ASSIGN_OR_THROW(output_schema_, arrow::ImportSchema(&arrowSchema));
}

} // namespace gluten

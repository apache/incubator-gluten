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

#include "operators/functions/RegistrationAllFunctions.h"
#include "operators/plannodes/RowVectorStream.h"
#include "utils/ConfigExtractor.h"

#ifdef GLUTEN_ENABLE_QAT
#include "utils/qat/QatCodec.h"
#endif
#ifdef GLUTEN_ENABLE_IAA
#include "utils/qpl/qpl_codec.h"
#endif
#include "compute/VeloxRuntime.h"
#include "config/VeloxConfig.h"
#include "jni/JniFileSystem.h"
#include "operators/functions/SparkTokenizer.h"
#include "udf/UdfLoader.h"
#include "utils/exception.h"
#include "velox/common/caching/SsdCache.h"
#include "velox/common/file/FileSystems.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/HiveDataSource.h"
#include "velox/serializers/PrestoSerializer.h"

DECLARE_bool(velox_exception_user_stacktrace_enabled);
DECLARE_int32(velox_memory_num_shared_leaf_pools);
DECLARE_bool(velox_memory_use_hugepages);
DECLARE_int32(cache_prefetch_min_pct);

DECLARE_int32(gluten_velox_aysnc_timeout_on_task_stopping);
DEFINE_int32(gluten_velox_aysnc_timeout_on_task_stopping, 30000, "Aysnc timout when task is being stopped");

using namespace facebook;

namespace gluten {

namespace {
gluten::Runtime* veloxRuntimeFactory(const std::unordered_map<std::string, std::string>& sessionConf) {
  return new gluten::VeloxRuntime(sessionConf);
}
} // namespace

void VeloxBackend::init(const std::unordered_map<std::string, std::string>& conf) {
  backendConf_ = std::make_shared<facebook::velox::core::MemConfigMutable>(conf);

  // Register Velox runtime factory
  gluten::Runtime::registerFactory(gluten::kVeloxRuntimeKind, veloxRuntimeFactory);

  if (backendConf_->get<bool>(kDebugModeEnabled, false)) {
    LOG(INFO) << "VeloxBackend config:" << printConfig(backendConf_->valuesCopy());
  }

  // Init glog and log level.
  if (!backendConf_->get<bool>(kDebugModeEnabled, false)) {
    FLAGS_v = backendConf_->get<uint32_t>(kGlogVerboseLevel, kGlogVerboseLevelDefault);
    FLAGS_minloglevel = backendConf_->get<uint32_t>(kGlogSeverityLevel, kGlogSeverityLevelDefault);
  } else {
    if (backendConf_->isValueExists(kGlogVerboseLevel)) {
      FLAGS_v = backendConf_->get<uint32_t>(kGlogVerboseLevel, kGlogVerboseLevelDefault);
    } else {
      FLAGS_v = kGlogVerboseLevelMaximum;
    }
  }
  FLAGS_logtostderr = true;
  google::InitGoogleLogging("gluten");

  // Avoid creating too many shared leaf pools.
  FLAGS_velox_memory_num_shared_leaf_pools = 0;

  // Set velox_exception_user_stacktrace_enabled.
  FLAGS_velox_exception_user_stacktrace_enabled =
      backendConf_->get<bool>(kEnableUserExceptionStacktrace, kEnableUserExceptionStacktraceDefault);

  // Set velox_exception_system_stacktrace_enabled.
  FLAGS_velox_exception_system_stacktrace_enabled =
      backendConf_->get<bool>(kEnableSystemExceptionStacktrace, kEnableSystemExceptionStacktraceDefault);

  // Set velox_memory_use_hugepages.
  FLAGS_velox_memory_use_hugepages = backendConf_->get<bool>(kMemoryUseHugePages, kMemoryUseHugePagesDefault);

  // Async timeout.
  FLAGS_gluten_velox_aysnc_timeout_on_task_stopping =
      backendConf_->get<int32_t>(kVeloxAsyncTimeoutOnTaskStopping, kVeloxAsyncTimeoutOnTaskStoppingDefault);

  // Set backtrace_allocation
  gluten::backtrace_allocation = backendConf_->get<bool>(kBacktraceAllocation, false);

  // Setup and register.
  velox::filesystems::registerLocalFileSystem();
  initJolFilesystem();
  initCache();
  initConnector();

  // Register Velox functions
  registerAllFunctions();
  if (!facebook::velox::isRegisteredVectorSerde()) {
    // serde, for spill
    facebook::velox::serializer::presto::PrestoVectorSerde::registerVectorSerde();
  }
  velox::exec::Operator::registerOperator(std::make_unique<RowVectorStreamOperatorTranslator>());

  initUdf();
  registerSparkTokenizer();

  // initialize the global memory manager for current process
  facebook::velox::memory::MemoryManager::initialize({});
}

facebook::velox::cache::AsyncDataCache* VeloxBackend::getAsyncDataCache() const {
  return asyncDataCache_.get();
}

// JNI-or-local filesystem, for spilling-to-heap if we have extra JVM heap spaces
void VeloxBackend::initJolFilesystem() {
  int64_t maxSpillFileSize = backendConf_->get<int64_t>(kMaxSpillFileSize, kMaxSpillFileSizeDefault);

  // FIXME It's known that if spill compression is disabled, the actual spill file size may
  //   in crease beyond this limit a little (maximum 64 rows which is by default
  //   one compression page)
  gluten::registerJolFileSystem(maxSpillFileSize);
}

void VeloxBackend::initCache() {
  if (backendConf_->get<bool>(kVeloxCacheEnabled, false)) {
    FLAGS_ssd_odirect = true;

    FLAGS_ssd_odirect = backendConf_->get<bool>(kVeloxSsdODirectEnabled, false);

    uint64_t memCacheSize = backendConf_->get<uint64_t>(kVeloxMemCacheSize, kVeloxMemCacheSizeDefault);
    uint64_t ssdCacheSize = backendConf_->get<uint64_t>(kVeloxSsdCacheSize, kVeloxSsdCacheSizeDefault);
    int32_t ssdCacheShards = backendConf_->get<int32_t>(kVeloxSsdCacheShards, kVeloxSsdCacheShardsDefault);
    int32_t ssdCacheIOThreads = backendConf_->get<int32_t>(kVeloxSsdCacheIOThreads, kVeloxSsdCacheIOThreadsDefault);
    std::string ssdCachePathPrefix = backendConf_->get<std::string>(kVeloxSsdCachePath, kVeloxSsdCachePathDefault);

    cachePathPrefix_ = ssdCachePathPrefix;
    cacheFilePrefix_ = getCacheFilePrefix();
    std::string ssdCachePath = ssdCachePathPrefix + "/" + cacheFilePrefix_;
    ssdCacheExecutor_ = std::make_unique<folly::IOThreadPoolExecutor>(ssdCacheIOThreads);
    auto ssd =
        std::make_unique<velox::cache::SsdCache>(ssdCachePath, ssdCacheSize, ssdCacheShards, ssdCacheExecutor_.get());

    std::error_code ec;
    const std::filesystem::space_info si = std::filesystem::space(ssdCachePathPrefix, ec);
    if (si.available < ssdCacheSize) {
      VELOX_FAIL(
          "not enough space for ssd cache in " + ssdCachePath + " cache size: " + std::to_string(ssdCacheSize) +
          "free space: " + std::to_string(si.available))
    }

    velox::memory::MmapAllocator::Options options;
    options.capacity = memCacheSize;
    cacheAllocator_ = std::make_shared<velox::memory::MmapAllocator>(options);
    if (ssdCacheSize == 0) {
      LOG(INFO) << "AsyncDataCache will do memory caching only as ssd cache size is 0";
      // TODO: this is not tracked by Spark.
      asyncDataCache_ = velox::cache::AsyncDataCache::create(cacheAllocator_.get());
    } else {
      // TODO: this is not tracked by Spark.
      asyncDataCache_ = velox::cache::AsyncDataCache::create(cacheAllocator_.get(), std::move(ssd));
    }

    VELOX_CHECK_NOT_NULL(dynamic_cast<velox::cache::AsyncDataCache*>(asyncDataCache_.get()))
    LOG(INFO) << "STARTUP: Using AsyncDataCache memory cache size: " << memCacheSize
              << ", ssdCache prefix: " << ssdCachePath << ", ssdCache size: " << ssdCacheSize
              << ", ssdCache shards: " << ssdCacheShards << ", ssdCache IO threads: " << ssdCacheIOThreads;
  }
}

void VeloxBackend::initConnector() {
  // The configs below are used at process level.
  auto connectorConf = std::make_shared<facebook::velox::core::MemConfigMutable>(backendConf_->valuesCopy());

  auto hiveConf = getHiveConfig(backendConf_);
  for (auto& [k, v] : hiveConf->valuesCopy()) {
    connectorConf->setValue(k, v);
  }

#ifdef ENABLE_ABFS
  const auto& confValue = backendConf_->valuesCopy();
  for (auto& [k, v] : confValue) {
    if (k.find("fs.azure.account.key") == 0) {
      connectorConf->setValue(k, v);
    } else if (k.find("spark.hadoop.fs.azure.account.key") == 0) {
      constexpr int32_t accountKeyPrefixLength = 13;
      connectorConf->setValue(k.substr(accountKeyPrefixLength), v);
    }
  }
#endif

  connectorConf->setValue(
      velox::connector::hive::HiveConfig::kEnableFileHandleCache,
      backendConf_->get<bool>(kVeloxFileHandleCacheEnabled, kVeloxFileHandleCacheEnabledDefault) ? "true" : "false");

  connectorConf->setValue(
      velox::connector::hive::HiveConfig::kMaxCoalescedBytes,
      backendConf_->get<std::string>(kMaxCoalescedBytes, "67108864")); // 64M
  connectorConf->setValue(
      velox::connector::hive::HiveConfig::kMaxCoalescedDistanceBytes,
      backendConf_->get<std::string>(kMaxCoalescedDistanceBytes, "1048576")); // 1M
  connectorConf->setValue(
      velox::connector::hive::HiveConfig::kPrefetchRowGroups, backendConf_->get<std::string>(kPrefetchRowGroups, "1"));
  connectorConf->setValue(
      velox::connector::hive::HiveConfig::kLoadQuantum,
      backendConf_->get<std::string>(kLoadQuantum, "268435456")); // 256M
  connectorConf->setValue(
      velox::connector::hive::HiveConfig::kFooterEstimatedSize,
      backendConf_->get<std::string>(kDirectorySizeGuess, "32768")); // 32K
  connectorConf->setValue(
      velox::connector::hive::HiveConfig::kFilePreloadThreshold,
      backendConf_->get<std::string>(kFilePreloadThreshold, "1048576")); // 1M

  // set cache_prefetch_min_pct default as 0 to force all loads are prefetched in DirectBufferInput.
  FLAGS_cache_prefetch_min_pct = backendConf_->get<int>(kCachePrefetchMinPct, 0);

  auto ioThreads = backendConf_->get<int32_t>(kVeloxIOThreads, kVeloxIOThreadsDefault);
  if (ioThreads > 0) {
    ioExecutor_ = std::make_unique<folly::IOThreadPoolExecutor>(ioThreads);
  }
  velox::connector::registerConnector(std::make_shared<velox::connector::hive::HiveConnector>(
      kHiveConnectorId,
      std::make_shared<facebook::velox::core::MemConfig>(connectorConf->valuesCopy()),
      ioExecutor_.get()));
}

void VeloxBackend::initUdf() {
  auto got = backendConf_->get<std::string>(kVeloxUdfLibraryPaths, "");
  if (!got.empty()) {
    auto udfLoader = gluten::UdfLoader::getInstance();
    udfLoader->loadUdfLibraries(got);
    udfLoader->registerUdf();
  }
}

std::unique_ptr<VeloxBackend> VeloxBackend::instance_ = nullptr;

void VeloxBackend::create(const std::unordered_map<std::string, std::string>& conf) {
  instance_ = std::unique_ptr<VeloxBackend>(new gluten::VeloxBackend(conf));
}

VeloxBackend* VeloxBackend::get() {
  if (!instance_) {
    LOG(WARNING) << "VeloxBackend instance is null, please invoke VeloxBackend#create before use.";
    throw GlutenException("VeloxBackend instance is null.");
  }
  return instance_.get();
}

const std::shared_ptr<const facebook::velox::Config> VeloxBackend::getBackendConf() const {
  return backendConf_;
}

} // namespace gluten

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

#include "shuffle/VeloxShuffleReader.h"

#ifdef GLUTEN_ENABLE_QAT
#include "utils/qat/QatCodec.h"
#endif
#ifdef GLUTEN_ENABLE_IAA
#include "utils/qpl/qpl_codec.h"
#endif
#include "compute/VeloxRuntime.h"
#include "config/GlutenConfig.h"
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

namespace {

const std::string kEnableUserExceptionStacktrace =
    "spark.gluten.sql.columnar.backend.velox.enableUserExceptionStacktrace";
const bool kEnableUserExceptionStacktraceDefault = true;

const std::string kEnableSystemExceptionStacktrace =
    "spark.gluten.sql.columnar.backend.velox.enableSystemExceptionStacktrace";
const bool kEnableSystemExceptionStacktraceDefault = true;

const std::string kMemoryUseHugePages = "spark.gluten.sql.columnar.backend.velox.memoryUseHugePages";
const bool kMemoryUseHugePagesDefault = false;

const std::string kHiveConnectorId = "test-hive";
const std::string kVeloxCacheEnabled = "spark.gluten.sql.columnar.backend.velox.cacheEnabled";

// memory cache
const std::string kVeloxMemCacheSize = "spark.gluten.sql.columnar.backend.velox.memCacheSize";
const uint64_t kVeloxMemCacheSizeDefault = 1073741824; // 1G

// ssd cache
const std::string kVeloxSsdCacheSize = "spark.gluten.sql.columnar.backend.velox.ssdCacheSize";
const uint64_t kVeloxSsdCacheSizeDefault = 1073741824; // 1G
const std::string kVeloxSsdCachePath = "spark.gluten.sql.columnar.backend.velox.ssdCachePath";
const std::string kVeloxSsdCachePathDefault = "/tmp/";
const std::string kVeloxSsdCacheShards = "spark.gluten.sql.columnar.backend.velox.ssdCacheShards";
const uint32_t kVeloxSsdCacheShardsDefault = 1;
const std::string kVeloxSsdCacheIOThreads = "spark.gluten.sql.columnar.backend.velox.ssdCacheIOThreads";
const uint32_t kVeloxSsdCacheIOThreadsDefault = 1;
const std::string kVeloxSsdODirectEnabled = "spark.gluten.sql.columnar.backend.velox.ssdODirect";

// async
const std::string kVeloxIOThreads = "spark.gluten.sql.columnar.backend.velox.IOThreads";
const uint32_t kVeloxIOThreadsDefault = 0;
const std::string kVeloxAsyncTimeoutOnTaskStopping =
    "spark.gluten.sql.columnar.backend.velox.asyncTimeoutOnTaskStopping";
const int32_t kVeloxAsyncTimeoutOnTaskStoppingDefault = 30000; // 30s

// udf
const std::string kVeloxUdfLibraryPaths = "spark.gluten.sql.columnar.backend.velox.udfLibraryPaths";

// spill
const std::string kMaxSpillFileSize = "spark.gluten.sql.columnar.backend.velox.maxSpillFileSize";
const uint64_t kMaxSpillFileSizeDefault = 20L * 1024 * 1024;

// backtrace allocation
const std::string kBacktraceAllocation = "spark.gluten.backtrace.allocation";

// VeloxShuffleReader print flag.
const std::string kVeloxShuffleReaderPrintFlag = "spark.gluten.velox.shuffleReaderPrintFlag";

const std::string kVeloxFileHandleCacheEnabled = "spark.gluten.sql.columnar.backend.velox.fileHandleCacheEnabled";
const bool kVeloxFileHandleCacheEnabledDefault = false;

/* configs for file read in velox*/
const std::string kDirectorySizeGuess = "spark.gluten.sql.columnar.backend.velox.directorySizeGuess";
const std::string kFilePreloadThreshold = "spark.gluten.sql.columnar.backend.velox.filePreloadThreshold";
const std::string kPrefetchRowGroups = "spark.gluten.sql.columnar.backend.velox.prefetchRowGroups";
const std::string kLoadQuantum = "spark.gluten.sql.columnar.backend.velox.loadQuantum";
const std::string kMaxCoalescedDistanceBytes = "spark.gluten.sql.columnar.backend.velox.maxCoalescedDistanceBytes";
const std::string kMaxCoalescedBytes = "spark.gluten.sql.columnar.backend.velox.maxCoalescedBytes";
const std::string kCachePrefetchMinPct = "spark.gluten.sql.columnar.backend.velox.cachePrefetchMinPct";

} // namespace

namespace gluten {

namespace {
gluten::Runtime* veloxRuntimeFactory(const std::unordered_map<std::string, std::string>& sessionConf) {
  return new gluten::VeloxRuntime(sessionConf);
}
} // namespace

void VeloxBackend::init(const std::unordered_map<std::string, std::string>& conf) {
  backendConf_ = conf;

  // Register Velox runtime factory
  gluten::Runtime::registerFactory(gluten::kVeloxRuntimeKind, veloxRuntimeFactory);

  std::shared_ptr<const facebook::velox::Config> veloxcfg =
      std::make_shared<facebook::velox::core::MemConfigMutable>(conf);

  if (veloxcfg->get<bool>(kDebugModeEnabled, false)) {
    LOG(INFO) << "VeloxBackend config:" << printConfig(veloxcfg->valuesCopy());
  }

  // Init glog and log level.
  if (!veloxcfg->get<bool>(kDebugModeEnabled, false)) {
    FLAGS_v = veloxcfg->get<uint32_t>(kGlogVerboseLevel, kGlogVerboseLevelDefault);
    FLAGS_minloglevel = veloxcfg->get<uint32_t>(kGlogSeverityLevel, kGlogSeverityLevelDefault);
  } else {
    if (veloxcfg->isValueExists(kGlogVerboseLevel)) {
      FLAGS_v = veloxcfg->get<uint32_t>(kGlogVerboseLevel, kGlogVerboseLevelDefault);
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
      veloxcfg->get<bool>(kEnableUserExceptionStacktrace, kEnableUserExceptionStacktraceDefault);

  // Set velox_exception_system_stacktrace_enabled.
  FLAGS_velox_exception_system_stacktrace_enabled =
      veloxcfg->get<bool>(kEnableSystemExceptionStacktrace, kEnableSystemExceptionStacktraceDefault);

  // Set velox_memory_use_hugepages.
  FLAGS_velox_memory_use_hugepages = veloxcfg->get<bool>(kMemoryUseHugePages, kMemoryUseHugePagesDefault);

  // Async timeout.
  FLAGS_gluten_velox_aysnc_timeout_on_task_stopping =
      veloxcfg->get<int32_t>(kVeloxAsyncTimeoutOnTaskStopping, kVeloxAsyncTimeoutOnTaskStoppingDefault);

  // Set backtrace_allocation
  gluten::backtrace_allocation = veloxcfg->get<bool>(kBacktraceAllocation, false);

  // Setup and register.
  velox::filesystems::registerLocalFileSystem();
  initJolFilesystem(veloxcfg);
  initCache(veloxcfg);
  initConnector(veloxcfg);

  // Register Velox functions
  registerAllFunctions();
  if (!facebook::velox::isRegisteredVectorSerde()) {
    // serde, for spill
    facebook::velox::serializer::presto::PrestoVectorSerde::registerVectorSerde();
  }
  velox::exec::Operator::registerOperator(std::make_unique<RowVectorStreamOperatorTranslator>());

  initUdf(veloxcfg);
  registerSparkTokenizer();

  // initialize the global memory manager for current process
  facebook::velox::memory::MemoryManager::initialize({});
}

facebook::velox::cache::AsyncDataCache* VeloxBackend::getAsyncDataCache() const {
  return asyncDataCache_.get();
}

// JNI-or-local filesystem, for spilling-to-heap if we have extra JVM heap spaces
void VeloxBackend::initJolFilesystem(const std::shared_ptr<const facebook::velox::Config>& conf) {
  int64_t maxSpillFileSize = conf->get<int64_t>(kMaxSpillFileSize, kMaxSpillFileSizeDefault);

  // FIXME It's known that if spill compression is disabled, the actual spill file size may
  //   in crease beyond this limit a little (maximum 64 rows which is by default
  //   one compression page)
  gluten::registerJolFileSystem(maxSpillFileSize);
}

void VeloxBackend::initCache(const std::shared_ptr<const facebook::velox::Config>& conf) {
  bool veloxCacheEnabled = conf->get<bool>(kVeloxCacheEnabled, false);
  if (veloxCacheEnabled) {
    FLAGS_ssd_odirect = true;

    FLAGS_ssd_odirect = conf->get<bool>(kVeloxSsdODirectEnabled, false);

    uint64_t memCacheSize = conf->get<uint64_t>(kVeloxMemCacheSize, kVeloxMemCacheSizeDefault);
    uint64_t ssdCacheSize = conf->get<uint64_t>(kVeloxSsdCacheSize, kVeloxSsdCacheSizeDefault);
    int32_t ssdCacheShards = conf->get<int32_t>(kVeloxSsdCacheShards, kVeloxSsdCacheShardsDefault);
    int32_t ssdCacheIOThreads = conf->get<int32_t>(kVeloxSsdCacheIOThreads, kVeloxSsdCacheIOThreadsDefault);
    std::string ssdCachePathPrefix = conf->get<std::string>(kVeloxSsdCachePath, kVeloxSsdCachePathDefault);

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

void VeloxBackend::initConnector(const std::shared_ptr<const facebook::velox::Config>& conf) {
  int32_t ioThreads = conf->get<int32_t>(kVeloxIOThreads, kVeloxIOThreadsDefault);

  auto mutableConf = std::make_shared<facebook::velox::core::MemConfigMutable>(conf->valuesCopy());

  auto hiveConf = getHiveConfig(conf);
  for (auto& [k, v] : hiveConf->valuesCopy()) {
    mutableConf->setValue(k, v);
  }

#ifdef ENABLE_ABFS
  const auto& confValue = conf->valuesCopy();
  for (auto& [k, v] : confValue) {
    if (k.find("fs.azure.account.key") == 0) {
      mutableConf->setValue(k, v);
    } else if (k.find("spark.hadoop.fs.azure.account.key") == 0) {
      constexpr int32_t accountKeyPrefixLength = 13;
      mutableConf->setValue(k.substr(accountKeyPrefixLength), v);
    }
  }
#endif

  mutableConf->setValue(
      velox::connector::hive::HiveConfig::kEnableFileHandleCache,
      conf->get<bool>(kVeloxFileHandleCacheEnabled, kVeloxFileHandleCacheEnabledDefault) ? "true" : "false");

  mutableConf->setValue(
      velox::connector::hive::HiveConfig::kMaxCoalescedBytes,
      conf->get<std::string>(kMaxCoalescedBytes, "67108864")); // 64M
  mutableConf->setValue(
      velox::connector::hive::HiveConfig::kMaxCoalescedDistanceBytes,
      conf->get<std::string>(kMaxCoalescedDistanceBytes, "1048576")); // 1M
  mutableConf->setValue(
      velox::connector::hive::HiveConfig::kPrefetchRowGroups, conf->get<std::string>(kPrefetchRowGroups, "1"));
  mutableConf->setValue(
      velox::connector::hive::HiveConfig::kLoadQuantum, conf->get<std::string>(kLoadQuantum, "268435456")); // 256M
  mutableConf->setValue(
      velox::connector::hive::HiveConfig::kFooterEstimatedSize,
      conf->get<std::string>(kDirectorySizeGuess, "32768")); // 32K
  mutableConf->setValue(
      velox::connector::hive::HiveConfig::kFilePreloadThreshold,
      conf->get<std::string>(kFilePreloadThreshold, "1048576")); // 1M

  // set cache_prefetch_min_pct default as 0 to force all loads are prefetched in DirectBufferInput.
  FLAGS_cache_prefetch_min_pct = conf->get<int>(kCachePrefetchMinPct, 0);

  if (ioThreads > 0) {
    ioExecutor_ = std::make_unique<folly::IOThreadPoolExecutor>(ioThreads);
  }
  velox::connector::registerConnector(std::make_shared<velox::connector::hive::HiveConnector>(
      kHiveConnectorId,
      std::make_shared<facebook::velox::core::MemConfig>(mutableConf->valuesCopy()),
      ioExecutor_.get()));
}

void VeloxBackend::initUdf(const std::shared_ptr<const facebook::velox::Config>& conf) {
  auto got = conf->get<std::string>(kVeloxUdfLibraryPaths, "");
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

const std::unordered_map<std::string, std::string>& VeloxBackend::getBackendConf() const {
  return backendConf_;
}

} // namespace gluten

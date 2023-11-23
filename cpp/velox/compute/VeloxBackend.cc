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

#include "shuffle/VeloxShuffleReader.h"

#ifdef GLUTEN_ENABLE_QAT
#include "utils/qat/QatCodec.h"
#endif
#ifdef GLUTEN_ENABLE_IAA
#include "utils/qpl/qpl_codec.h"
#endif
#ifdef ENABLE_GCS
#include <fstream>
#endif
#include "config/GlutenConfig.h"
#include "jni/JniFileSystem.h"
#include "operators/functions/SparkTokenizer.h"
#include "udf/UdfLoader.h"
#include "utils/ConfigExtractor.h"
#include "utils/exception.h"
#include "velox/common/caching/SsdCache.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/MmapAllocator.h"
#include "velox/connectors/hive/HiveConfig.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/HiveDataSource.h"
#include "velox/dwio/dwrf/reader/DwrfReader.h"
#include "velox/dwio/parquet/RegisterParquetReader.h"
#include "velox/serializers/PrestoSerializer.h"

DECLARE_int32(split_preload_per_driver);
DECLARE_bool(velox_exception_user_stacktrace_enabled);
DECLARE_int32(velox_memory_num_shared_leaf_pools);
DECLARE_bool(velox_memory_use_hugepages);

using namespace facebook;

namespace {

const std::string kEnableUserExceptionStacktrace =
    "spark.gluten.sql.columnar.backend.velox.enableUserExceptionStacktrace";
const bool kEnableUserExceptionStacktraceDefault = true;

const std::string kGlogVerboseLevel = "spark.gluten.sql.columnar.backend.velox.glogVerboseLevel";
const uint32_t kGlogVerboseLevelDefault = 0;

const std::string kGlogSeverityLevel = "spark.gluten.sql.columnar.backend.velox.glogSeverityLevel";
const uint32_t kGlogSeverityLevelDefault = 1;

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

const std::string kVeloxIOThreads = "spark.gluten.sql.columnar.backend.velox.IOThreads";
const uint32_t kVeloxIOThreadsDefault = 0;

const std::string kVeloxSplitPreloadPerDriver = "spark.gluten.sql.columnar.backend.velox.SplitPreloadPerDriver";
const uint32_t kVeloxSplitPreloadPerDriverDefault = 2;

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

} // namespace

namespace gluten {

void VeloxBackend::init(const std::unordered_map<std::string, std::string>& conf) {
  // Init glog and log level.
  auto veloxmemcfg = std::make_shared<facebook::velox::core::MemConfigMutable>(conf);
  const facebook::velox::Config* veloxcfg = veloxmemcfg.get();

  if (veloxcfg->get<bool>(kDebugModeEnabled, false)) {
    LOG(INFO) << "VeloxBackend config:" << printConfig(veloxcfg->valuesCopy());
  }

  uint32_t vlogLevel = veloxcfg->get<uint32_t>(kGlogVerboseLevel, kGlogVerboseLevelDefault);
  uint32_t severityLogLevel = veloxcfg->get<uint32_t>(kGlogSeverityLevel, kGlogSeverityLevelDefault);
  FLAGS_v = vlogLevel;
  FLAGS_minloglevel = severityLogLevel;
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

  // Set backtrace_allocation
  gluten::backtrace_allocation = veloxcfg->get<bool>(kBacktraceAllocation, false);

  // Set veloxShuffleReaderPrintFlag
  gluten::veloxShuffleReaderPrintFlag = veloxcfg->get<bool>(kVeloxShuffleReaderPrintFlag, false);

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
}

facebook::velox::cache::AsyncDataCache* VeloxBackend::getAsyncDataCache() const {
  return asyncDataCache_.get();
}

// JNI-or-local filesystem, for spilling-to-heap if we have extra JVM heap spaces
void VeloxBackend::initJolFilesystem(const facebook::velox::Config* conf) {
  int64_t maxSpillFileSize = conf->get<int64_t>(kMaxSpillFileSize, kMaxSpillFileSizeDefault);

  // FIXME It's known that if spill compression is disabled, the actual spill file size may
  //   in crease beyond this limit a little (maximum 64 rows which is by default
  //   one compression page)
  gluten::registerJolFileSystem(maxSpillFileSize);
}

void VeloxBackend::initCache(const facebook::velox::Config* conf) {
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
    auto allocator = std::make_shared<velox::memory::MmapAllocator>(options);
    if (ssdCacheSize == 0) {
      LOG(INFO) << "AsyncDataCache will do memory caching only as ssd cache size is 0";
      // TODO: this is not tracked by Spark.
      asyncDataCache_ = velox::cache::AsyncDataCache::create(allocator.get());
    } else {
      // TODO: this is not tracked by Spark.
      asyncDataCache_ = velox::cache::AsyncDataCache::create(allocator.get(), std::move(ssd));
    }

    VELOX_CHECK_NOT_NULL(dynamic_cast<velox::cache::AsyncDataCache*>(asyncDataCache_.get()))
    LOG(INFO) << "STARTUP: Using AsyncDataCache memory cache size: " << memCacheSize
              << ", ssdCache prefix: " << ssdCachePath << ", ssdCache size: " << ssdCacheSize
              << ", ssdCache shards: " << ssdCacheShards << ", ssdCache IO threads: " << ssdCacheIOThreads;
  }
}

void VeloxBackend::initConnector(const facebook::velox::Config* conf) {
  int32_t ioThreads = conf->get<int32_t>(kVeloxIOThreads, kVeloxIOThreadsDefault);
  int32_t splitPreloadPerDriver = conf->get<int32_t>(kVeloxSplitPreloadPerDriver, kVeloxSplitPreloadPerDriverDefault);

  auto mutableConf = std::make_shared<facebook::velox::core::MemConfigMutable>(conf->valuesCopy());

#ifdef ENABLE_S3
  std::string awsAccessKey = conf->get<std::string>("spark.hadoop.fs.s3a.access.key", "");
  std::string awsSecretKey = conf->get<std::string>("spark.hadoop.fs.s3a.secret.key", "");
  std::string awsEndpoint = conf->get<std::string>("spark.hadoop.fs.s3a.endpoint", "");
  bool sslEnabled = conf->get<bool>("spark.hadoop.fs.s3a.connection.ssl.enabled", false);
  bool pathStyleAccess = conf->get<bool>("spark.hadoop.fs.s3a.path.style.access", false);
  bool useInstanceCredentials = conf->get<bool>("spark.hadoop.fs.s3a.use.instance.credentials", false);
  std::string iamRole = conf->get<std::string>("spark.hadoop.fs.s3a.iam.role", "");
  std::string iamRoleSessionName = conf->get<std::string>("spark.hadoop.fs.s3a.iam.role.session.name", "");

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

  std::unordered_map<std::string, std::string> s3Config({});
  if (useInstanceCredentials) {
    mutableConf->setValue("hive.s3.use-instance-credentials", "true");
  } else if (!iamRole.empty()) {
    mutableConf->setValue("hive.s3.iam-role", iamRole);
    if (!iamRoleSessionName.empty()) {
      mutableConf->setValue("hive.s3.iam-role-session-name", iamRoleSessionName);
    }
  } else {
    mutableConf->setValue("hive.s3.aws-access-key", awsAccessKey);
    mutableConf->setValue("hive.s3.aws-secret-key", awsSecretKey);
  }
  // Only need to set s3 endpoint when not use instance credentials.
  if (!useInstanceCredentials) {
    mutableConf->setValue("hive.s3.endpoint", awsEndpoint);
  }
  mutableConf->setValue("hive.s3.ssl.enabled", sslEnabled ? "true" : "false");
  mutableConf->setValue("hive.s3.path-style-access", pathStyleAccess ? "true" : "false");
#endif

#ifdef ENABLE_GCS
  // https://github.com/GoogleCloudDataproc/hadoop-connectors/blob/master/gcs/CONFIGURATION.md#api-client-configuration
  auto gsStorageRootUrl = conf->get("spark.hadoop.fs.gs.storage.root.url");
  if (gsStorageRootUrl.hasValue()) {
    std::string url = gsStorageRootUrl.value();
    std::string gcsScheme;
    std::string gcsEndpoint;

    const auto sep = std::string("://");
    const auto pos = url.find_first_of(sep);
    if (pos != std::string::npos) {
      gcsScheme = url.substr(0, pos);
      gcsEndpoint = url.substr(pos + sep.length());
    }

    if (!gcsEndpoint.empty() && !gcsScheme.empty()) {
      mutableConf->setValue("hive.gcs.scheme", gcsScheme);
      mutableConf->setValue("hive.gcs.endpoint", gcsEndpoint);
    }
  }

  // https://github.com/GoogleCloudDataproc/hadoop-connectors/blob/master/gcs/CONFIGURATION.md#authentication
  auto gsAuthType = conf->get("spark.hadoop.fs.gs.auth.type");
  if (gsAuthType.hasValue()) {
    std::string type = gsAuthType.value();
    if (type == "SERVICE_ACCOUNT_JSON_KEYFILE") {
      auto gsAuthServiceAccountJsonKeyfile = conf->get("spark.hadoop.fs.gs.auth.service.account.json.keyfile");
      if (gsAuthServiceAccountJsonKeyfile.hasValue()) {
        auto stream = std::ifstream(gsAuthServiceAccountJsonKeyfile.value());
        stream.exceptions(std::ios::badbit);
        std::string gsAuthServiceAccountJson = std::string(std::istreambuf_iterator<char>(stream.rdbuf()), {});
        mutableConf->setValue("hive.gcs.credentials", gsAuthServiceAccountJson);
      } else {
        LOG(WARNING) << "STARTUP: conf spark.hadoop.fs.gs.auth.type is set to SERVICE_ACCOUNT_JSON_KEYFILE, "
                        "however conf spark.hadoop.fs.gs.auth.service.account.json.keyfile is not set";
        throw GlutenException("Conf spark.hadoop.fs.gs.auth.service.account.json.keyfile is not set");
      }
    }
  }
#endif

  mutableConf->setValue(
      velox::connector::hive::HiveConfig::kEnableFileHandleCache,
      conf->get<bool>(kVeloxFileHandleCacheEnabled, kVeloxFileHandleCacheEnabledDefault) ? "true" : "false");

  if (ioThreads > 0) {
    if (splitPreloadPerDriver > 0) {
      // spark.gluten.sql.columnar.backend.velox.SplitPreloadPerDriver takes no effect if
      // spark.gluten.sql.columnar.backend.velox.IOThreads is set to 0
      LOG(INFO) << "STARTUP: Using split preloading, Split preload per driver: " << splitPreloadPerDriver
                << ", IO threads: " << ioThreads;
      FLAGS_split_preload_per_driver = splitPreloadPerDriver;
    }
    ioExecutor_ = std::make_unique<folly::IOThreadPoolExecutor>(ioThreads);
  }
  velox::connector::registerConnector(
      std::make_shared<velox::connector::hive::HiveConnector>(kHiveConnectorId, mutableConf, ioExecutor_.get()));
}

void VeloxBackend::initUdf(const facebook::velox::Config* conf) {
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

} // namespace gluten

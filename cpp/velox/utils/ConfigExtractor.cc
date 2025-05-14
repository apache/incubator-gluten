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

// This File includes common helper functions with Arrow dependency.

#include "ConfigExtractor.h"
#include <stdexcept>

#include "config/VeloxConfig.h"
#include "utils/Exception.h"
#include "velox/connectors/hive/HiveConfig.h"
#include "velox/connectors/hive/storage_adapters/s3fs/S3Config.h"

namespace gluten {

std::string getConfigValue(
    const std::unordered_map<std::string, std::string>& confMap,
    const std::string& key,
    const std::optional<std::string>& fallbackValue) {
  auto got = confMap.find(key);
  if (got == confMap.end()) {
    if (fallbackValue == std::nullopt) {
      throw std::runtime_error("No such config key: " + key);
    }
    return fallbackValue.value();
  }
  return got->second;
}

std::shared_ptr<facebook::velox::config::ConfigBase> getHiveConfig(
    std::shared_ptr<facebook::velox::config::ConfigBase> conf) {
  std::unordered_map<std::string, std::string> hiveConfMap;

#ifdef ENABLE_S3
  using namespace facebook::velox::filesystems;
  std::string_view kSparkHadoopS3Prefix = "spark.hadoop.fs.s3a.";
  std::string_view kSparkHadoopS3BucketPrefix = "spark.hadoop.fs.s3a.bucket.";

  // Log granularity of AWS C++ SDK
  const std::string kVeloxAwsSdkLogLevel = "spark.gluten.velox.awsSdkLogLevel";
  const std::string kVeloxAwsSdkLogLevelDefault = "FATAL";

  // Whether to use proxy from env for s3 c++ client
  const std::string kVeloxS3UseProxyFromEnv = "spark.gluten.velox.s3UseProxyFromEnv";
  const std::string kVeloxS3UseProxyFromEnvDefault = "false";

  // Payload signing policy
  const std::string kVeloxS3PayloadSigningPolicy = "spark.gluten.velox.s3PayloadSigningPolicy";
  const std::string kVeloxS3PayloadSigningPolicyDefault = "Never";

  // Log location of AWS C++ SDK
  const std::string kVeloxS3LogLocation = "spark.gluten.velox.s3LogLocation";

  const std::unordered_map<S3Config::Keys, std::pair<std::string, std::optional<std::string>>> sparkSuffixes = {
      {S3Config::Keys::kAccessKey, std::make_pair("access.key", std::nullopt)},
      {S3Config::Keys::kSecretKey, std::make_pair("secret.key", std::nullopt)},
      {S3Config::Keys::kEndpoint, std::make_pair("endpoint", std::nullopt)},
      {S3Config::Keys::kSSLEnabled, std::make_pair("connection.ssl.enabled", "false")},
      {S3Config::Keys::kPathStyleAccess, std::make_pair("path.style.access", "false")},
      {S3Config::Keys::kMaxAttempts, std::make_pair("retry.limit", std::nullopt)},
      {S3Config::Keys::kRetryMode, std::make_pair("retry.mode", "legacy")},
      {S3Config::Keys::kMaxConnections, std::make_pair("connection.maximum", "15")},
      {S3Config::Keys::kConnectTimeout, std::make_pair("connection.timeout", "200s")},
      {S3Config::Keys::kUseInstanceCredentials, std::make_pair("instance.credentials", "false")},
      {S3Config::Keys::kIamRole, std::make_pair("iam.role", std::nullopt)},
      {S3Config::Keys::kIamRoleSessionName, std::make_pair("iam.role.session.name", "gluten-session")},
      {S3Config::Keys::kEndpointRegion, std::make_pair("endpoint.region", std::nullopt)},
  };

  // get Velox S3 config key from Spark Suffix.
  auto getVeloxKey = [&](std::string_view suffix) {
    for (const auto& [key, value] : sparkSuffixes) {
      if (value.first == suffix) {
        return std::optional<S3Config::Keys>(key);
      }
    }
    return std::optional<S3Config::Keys>(std::nullopt);
  };

  auto sparkBaseConfigValue = [&](S3Config::Keys key) {
    std::stringstream ss;
    auto keyValue = sparkSuffixes.find(key)->second;
    ss << kSparkHadoopS3Prefix << keyValue.first;
    auto sparkKey = ss.str();
    if (conf->valueExists(sparkKey)) {
      return static_cast<std::optional<std::string>>(conf->get<std::string>(sparkKey));
    }
    // Return default value.
    return keyValue.second;
  };

  auto setConfigIfPresent = [&](S3Config::Keys key) {
    auto sparkConfig = sparkBaseConfigValue(key);
    if (sparkConfig.has_value()) {
      hiveConfMap[S3Config::baseConfigKey(key)] = sparkConfig.value();
    }
  };

  auto setFromEnvOrConfigIfPresent = [&](std::string_view envName, S3Config::Keys key) {
    const char* envValue = std::getenv(envName.data());
    if (envValue != nullptr) {
      hiveConfMap[S3Config::baseConfigKey(key)] = std::string(envValue);
    } else {
      setConfigIfPresent(key);
    }
  };

  setFromEnvOrConfigIfPresent("AWS_ENDPOINT", S3Config::Keys::kEndpoint);
  setFromEnvOrConfigIfPresent("AWS_MAX_ATTEMPTS", S3Config::Keys::kMaxAttempts);
  setFromEnvOrConfigIfPresent("AWS_RETRY_MODE", S3Config::Keys::kRetryMode);
  setFromEnvOrConfigIfPresent("AWS_ACCESS_KEY_ID", S3Config::Keys::kAccessKey);
  setFromEnvOrConfigIfPresent("AWS_SECRET_ACCESS_KEY", S3Config::Keys::kSecretKey);
  setConfigIfPresent(S3Config::Keys::kUseInstanceCredentials);
  setConfigIfPresent(S3Config::Keys::kIamRole);
  setConfigIfPresent(S3Config::Keys::kIamRoleSessionName);
  setConfigIfPresent(S3Config::Keys::kSSLEnabled);
  setConfigIfPresent(S3Config::Keys::kPathStyleAccess);
  setConfigIfPresent(S3Config::Keys::kMaxConnections);
  setConfigIfPresent(S3Config::Keys::kConnectTimeout);
  setConfigIfPresent(S3Config::Keys::kEndpointRegion);

  hiveConfMap[S3Config::kS3LogLevel] = conf->get<std::string>(kVeloxAwsSdkLogLevel, kVeloxAwsSdkLogLevelDefault);
  hiveConfMap[S3Config::baseConfigKey(S3Config::Keys::kUseProxyFromEnv)] =
      conf->get<std::string>(kVeloxS3UseProxyFromEnv, kVeloxS3UseProxyFromEnvDefault);
  hiveConfMap[S3Config::kS3PayloadSigningPolicy] =
      conf->get<std::string>(kVeloxS3PayloadSigningPolicy, kVeloxS3PayloadSigningPolicyDefault);
  auto logLocation = conf->get<std::string>(kVeloxS3LogLocation);
  if (logLocation.hasValue()) {
    hiveConfMap[S3Config::kS3LogLocation] = logLocation.value();
  };

  // Convert all Spark bucket configs to Velox bucket configs.
  for (const auto& [key, value] : conf->rawConfigs()) {
    if (key.find(kSparkHadoopS3BucketPrefix) == 0) {
      std::string_view skey = key;
      auto remaining = skey.substr(kSparkHadoopS3BucketPrefix.size());
      int dot = remaining.find(".");
      auto bucketName = remaining.substr(0, dot);
      auto suffix = remaining.substr(dot + 1);
      auto veloxKey = getVeloxKey(suffix);

      if (veloxKey.has_value()) {
        hiveConfMap[S3Config::bucketConfigKey(veloxKey.value(), bucketName)] = value;
      }
    }
  }
#endif

#ifdef ENABLE_GCS
  // https://github.com/GoogleCloudDataproc/hadoop-connectors/blob/master/gcs/CONFIGURATION.md#api-client-configuration
  auto gsStorageRootUrl = conf->get<std::string>("spark.hadoop.fs.gs.storage.root.url");
  if (gsStorageRootUrl.hasValue()) {
    std::string gcsEndpoint = gsStorageRootUrl.value();

    if (!gcsEndpoint.empty()) {
      hiveConfMap[facebook::velox::connector::hive::HiveConfig::kGcsEndpoint] = gcsEndpoint;
    }
  }

  // https://github.com/GoogleCloudDataproc/hadoop-connectors/blob/master/gcs/CONFIGURATION.md#http-transport-configuration
  // https://cloud.google.com/cpp/docs/reference/storage/latest/classgoogle_1_1cloud_1_1storage_1_1LimitedErrorCountRetryPolicy
  auto gsMaxRetryCount = conf->get<std::string>("spark.hadoop.fs.gs.http.max.retry");
  if (gsMaxRetryCount.hasValue()) {
    hiveConfMap[facebook::velox::connector::hive::HiveConfig::kGcsMaxRetryCount] = gsMaxRetryCount.value();
  }

  // https://cloud.google.com/cpp/docs/reference/storage/latest/classgoogle_1_1cloud_1_1storage_1_1LimitedTimeRetryPolicy
  auto gsMaxRetryTime = conf->get<std::string>("spark.hadoop.fs.gs.http.max.retry-time");
  if (gsMaxRetryTime.hasValue()) {
    hiveConfMap[facebook::velox::connector::hive::HiveConfig::kGcsMaxRetryTime] = gsMaxRetryTime.value();
  }

  // https://github.com/GoogleCloudDataproc/hadoop-connectors/blob/master/gcs/CONFIGURATION.md#authentication
  auto gsAuthType = conf->get<std::string>("spark.hadoop.fs.gs.auth.type");
  auto gsAuthServiceAccountJsonKeyfile = conf->get<std::string>("spark.hadoop.fs.gs.auth.service.account.json.keyfile");
  if (gsAuthType.hasValue() && gsAuthType.value() == "SERVICE_ACCOUNT_JSON_KEYFILE") {
    if (gsAuthServiceAccountJsonKeyfile.hasValue()) {
      hiveConfMap[facebook::velox::connector::hive::HiveConfig::kGcsCredentialsPath] =
          gsAuthServiceAccountJsonKeyfile.value();
    } else {
      LOG(WARNING) << "STARTUP: conf spark.hadoop.fs.gs.auth.type is set to SERVICE_ACCOUNT_JSON_KEYFILE, "
                      "however conf spark.hadoop.fs.gs.auth.service.account.json.keyfile is not set";
      throw GlutenException("Conf spark.hadoop.fs.gs.auth.service.account.json.keyfile is not set");
    }
  } else if (gsAuthServiceAccountJsonKeyfile.hasValue()) {
    LOG(WARNING) << "STARTUP: conf spark.hadoop.fs.gs.auth.service.account.json.keyfile is set, "
                    "but conf spark.hadoop.fs.gs.auth.type is not SERVICE_ACCOUNT_JSON_KEYFILE";
    throw GlutenException("Conf spark.hadoop.fs.gs.auth.type is missing or incorrect");
  }
#endif

#ifdef ENABLE_ABFS
  std::string_view kSparkHadoopPrefix = "spark.hadoop.";
  std::string_view kSparkHadoopAbfsPrefix = "spark.hadoop.fs.azure.";
  for (const auto& [key, value] : conf->rawConfigs()) {
    if (key.find(kSparkHadoopAbfsPrefix) == 0) {
      // Remove the SparkHadoopPrefix
      hiveConfMap[key.substr(kSparkHadoopPrefix.size())] = value;
    }
  }
#endif

  hiveConfMap[facebook::velox::connector::hive::HiveConfig::kEnableFileHandleCache] =
      conf->get<bool>(kVeloxFileHandleCacheEnabled, kVeloxFileHandleCacheEnabledDefault) ? "true" : "false";
  hiveConfMap[facebook::velox::connector::hive::HiveConfig::kMaxCoalescedBytes] =
      conf->get<std::string>(kMaxCoalescedBytes, "67108864"); // 64M
  hiveConfMap[facebook::velox::connector::hive::HiveConfig::kMaxCoalescedDistance] =
      conf->get<std::string>(kMaxCoalescedDistance, "512KB"); // 512KB
  hiveConfMap[facebook::velox::connector::hive::HiveConfig::kPrefetchRowGroups] =
      conf->get<std::string>(kPrefetchRowGroups, "1");
  hiveConfMap[facebook::velox::connector::hive::HiveConfig::kLoadQuantum] =
      conf->get<std::string>(kLoadQuantum, "268435456"); // 256M
  hiveConfMap[facebook::velox::connector::hive::HiveConfig::kFooterEstimatedSize] =
      conf->get<std::string>(kDirectorySizeGuess, "32768"); // 32K
  hiveConfMap[facebook::velox::connector::hive::HiveConfig::kFilePreloadThreshold] =
      conf->get<std::string>(kFilePreloadThreshold, "1048576"); // 1M

  // read as UTC
  hiveConfMap[facebook::velox::connector::hive::HiveConfig::kReadTimestampPartitionValueAsLocalTime] = "false";

  // Maps table field names to file field names using names, not indices.
  hiveConfMap[facebook::velox::connector::hive::HiveConfig::kParquetUseColumnNames] = "true";
  hiveConfMap[facebook::velox::connector::hive::HiveConfig::kOrcUseColumnNames] = "true";

  return std::make_shared<facebook::velox::config::ConfigBase>(std::move(hiveConfMap));
}

} // namespace gluten

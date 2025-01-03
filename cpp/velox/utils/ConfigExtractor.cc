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

#include "utils/Exception.h"
#include "velox/connectors/hive/HiveConfig.h"
#include "velox/connectors/hive/storage_adapters/s3fs/S3Config.h"

namespace {

const std::string kVeloxFileHandleCacheEnabled = "spark.gluten.sql.columnar.backend.velox.fileHandleCacheEnabled";
const bool kVeloxFileHandleCacheEnabledDefault = false;
} // namespace

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
  std::string_view kSparkHadoopPrefix = "spark.hadoop.fs.s3a.";
  std::string_view kSparkHadoopBucketPrefix = "spark.hadoop.fs.s3a.bucket.";

  // Log granularity of AWS C++ SDK
  const std::string kVeloxAwsSdkLogLevel = "spark.gluten.velox.awsSdkLogLevel";
  const std::string kVeloxAwsSdkLogLevelDefault = "FATAL";

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
    ss << kSparkHadoopPrefix << keyValue.first;
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

  hiveConfMap[facebook::velox::filesystems::S3Config::kS3LogLevel] =
      conf->get<std::string>(kVeloxAwsSdkLogLevel, kVeloxAwsSdkLogLevelDefault);
  ;

  // Convert all Spark bucket configs to Velox bucket configs.
  for (const auto& [key, value] : conf->rawConfigs()) {
    if (key.find(kSparkHadoopBucketPrefix) == 0) {
      std::string_view skey = key;
      auto remaining = skey.substr(kSparkHadoopBucketPrefix.size());
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
  if (gsAuthType.hasValue()) {
    std::string type = gsAuthType.value();
    if (type == "SERVICE_ACCOUNT_JSON_KEYFILE") {
      auto gsAuthServiceAccountJsonKeyfile =
          conf->get<std::string>("spark.hadoop.fs.gs.auth.service.account.json.keyfile");
      if (gsAuthServiceAccountJsonKeyfile.hasValue()) {
        hiveConfMap[facebook::velox::connector::hive::HiveConfig::kGcsCredentialsPath] =
            gsAuthServiceAccountJsonKeyfile.value();
      } else {
        LOG(WARNING) << "STARTUP: conf spark.hadoop.fs.gs.auth.type is set to SERVICE_ACCOUNT_JSON_KEYFILE, "
                        "however conf spark.hadoop.fs.gs.auth.service.account.json.keyfile is not set";
        throw GlutenException("Conf spark.hadoop.fs.gs.auth.service.account.json.keyfile is not set");
      }
    }
  }
#endif

  hiveConfMap[facebook::velox::connector::hive::HiveConfig::kEnableFileHandleCache] =
      conf->get<bool>(kVeloxFileHandleCacheEnabled, kVeloxFileHandleCacheEnabledDefault) ? "true" : "false";

  return std::make_shared<facebook::velox::config::ConfigBase>(std::move(hiveConfMap));
}

} // namespace gluten

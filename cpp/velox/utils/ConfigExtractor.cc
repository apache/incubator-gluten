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

namespace {

const std::string kVeloxFileHandleCacheEnabled = "spark.gluten.sql.columnar.backend.velox.fileHandleCacheEnabled";
const bool kVeloxFileHandleCacheEnabledDefault = false;

// Log granularity of AWS C++ SDK
const std::string kVeloxAwsSdkLogLevel = "spark.gluten.velox.awsSdkLogLevel";
const std::string kVeloxAwsSdkLogLevelDefault = "FATAL";
// Retry mode for AWS s3
const std::string kVeloxS3RetryMode = "spark.gluten.velox.fs.s3a.retry.mode";
const std::string kVeloxS3RetryModeDefault = "legacy";
// Connection timeout for AWS s3
const std::string kVeloxS3ConnectTimeout = "spark.gluten.velox.fs.s3a.connect.timeout";
// Using default fs.s3a.connection.timeout value in hadoop
const std::string kVeloxS3ConnectTimeoutDefault = "200s";
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
  std::string awsAccessKey = conf->get<std::string>("spark.hadoop.fs.s3a.access.key", "");
  std::string awsSecretKey = conf->get<std::string>("spark.hadoop.fs.s3a.secret.key", "");
  std::string awsEndpoint = conf->get<std::string>("spark.hadoop.fs.s3a.endpoint", "");
  bool sslEnabled = conf->get<bool>("spark.hadoop.fs.s3a.connection.ssl.enabled", false);
  bool pathStyleAccess = conf->get<bool>("spark.hadoop.fs.s3a.path.style.access", false);
  bool useInstanceCredentials = conf->get<bool>("spark.hadoop.fs.s3a.use.instance.credentials", false);
  std::string iamRole = conf->get<std::string>("spark.hadoop.fs.s3a.iam.role", "");
  std::string iamRoleSessionName = conf->get<std::string>("spark.hadoop.fs.s3a.iam.role.session.name", "");
  std::string retryMaxAttempts = conf->get<std::string>("spark.hadoop.fs.s3a.retry.limit", "20");
  std::string retryMode = conf->get<std::string>(kVeloxS3RetryMode, kVeloxS3RetryModeDefault);
  std::string maxConnections = conf->get<std::string>("spark.hadoop.fs.s3a.connection.maximum", "15");
  std::string connectTimeout = conf->get<std::string>(kVeloxS3ConnectTimeout, kVeloxS3ConnectTimeoutDefault);

  std::string awsSdkLogLevel = conf->get<std::string>(kVeloxAwsSdkLogLevel, kVeloxAwsSdkLogLevelDefault);

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
  const char* envRetryMaxAttempts = std::getenv("AWS_MAX_ATTEMPTS");
  if (envRetryMaxAttempts != nullptr) {
    retryMaxAttempts = std::string(envRetryMaxAttempts);
  }
  const char* envRetryMode = std::getenv("AWS_RETRY_MODE");
  if (envRetryMode != nullptr) {
    retryMode = std::string(envRetryMode);
  }

  if (useInstanceCredentials) {
    hiveConfMap[facebook::velox::connector::hive::HiveConfig::kS3UseInstanceCredentials] = "true";
  } else if (!iamRole.empty()) {
    hiveConfMap[facebook::velox::connector::hive::HiveConfig::kS3IamRole] = iamRole;
    if (!iamRoleSessionName.empty()) {
      hiveConfMap[facebook::velox::connector::hive::HiveConfig::kS3IamRoleSessionName] = iamRoleSessionName;
    }
  } else {
    hiveConfMap[facebook::velox::connector::hive::HiveConfig::kS3AwsAccessKey] = awsAccessKey;
    hiveConfMap[facebook::velox::connector::hive::HiveConfig::kS3AwsSecretKey] = awsSecretKey;
  }
  // Only need to set s3 endpoint when not use instance credentials.
  if (!useInstanceCredentials) {
    hiveConfMap[facebook::velox::connector::hive::HiveConfig::kS3Endpoint] = awsEndpoint;
  }
  hiveConfMap[facebook::velox::connector::hive::HiveConfig::kS3SSLEnabled] = sslEnabled ? "true" : "false";
  hiveConfMap[facebook::velox::connector::hive::HiveConfig::kS3PathStyleAccess] = pathStyleAccess ? "true" : "false";
  hiveConfMap[facebook::velox::connector::hive::HiveConfig::kS3LogLevel] = awsSdkLogLevel;
  hiveConfMap[facebook::velox::connector::hive::HiveConfig::kS3MaxAttempts] = retryMaxAttempts;
  hiveConfMap[facebook::velox::connector::hive::HiveConfig::kS3RetryMode] = retryMode;
  hiveConfMap[facebook::velox::connector::hive::HiveConfig::kS3MaxConnections] = maxConnections;
  hiveConfMap[facebook::velox::connector::hive::HiveConfig::kS3ConnectTimeout] = connectTimeout;
#endif

#ifdef ENABLE_GCS
  // https://github.com/GoogleCloudDataproc/hadoop-connectors/blob/master/gcs/CONFIGURATION.md#api-client-configuration
  auto gsStorageRootUrl = conf->get<std::string>("spark.hadoop.fs.gs.storage.root.url");
  if (gsStorageRootUrl.hasValue()) {
    std::string gcsEndpoint = gsStorageRootUrl.value();

    if (!gcsEndpoint.empty()) {
      hiveConfMap[facebook::velox::connector::hive::HiveConfig::kGCSEndpoint] = gcsEndpoint;
    }
  }

  // https://github.com/GoogleCloudDataproc/hadoop-connectors/blob/master/gcs/CONFIGURATION.md#http-transport-configuration
  // https://cloud.google.com/cpp/docs/reference/storage/latest/classgoogle_1_1cloud_1_1storage_1_1LimitedErrorCountRetryPolicy
  auto gsMaxRetryCount = conf->get<std::string>("spark.hadoop.fs.gs.http.max.retry");
  if (gsMaxRetryCount.hasValue()) {
    hiveConfMap[facebook::velox::connector::hive::HiveConfig::kGCSMaxRetryCount] = gsMaxRetryCount.value();
  }

  // https://cloud.google.com/cpp/docs/reference/storage/latest/classgoogle_1_1cloud_1_1storage_1_1LimitedTimeRetryPolicy
  auto gsMaxRetryTime = conf->get<std::string>("spark.hadoop.fs.gs.http.max.retry-time");
  if (gsMaxRetryTime.hasValue()) {
    hiveConfMap[facebook::velox::connector::hive::HiveConfig::kGCSMaxRetryTime] = gsMaxRetryTime.value();
  }

  // https://github.com/GoogleCloudDataproc/hadoop-connectors/blob/master/gcs/CONFIGURATION.md#authentication
  auto gsAuthType = conf->get<std::string>("spark.hadoop.fs.gs.auth.type");
  if (gsAuthType.hasValue()) {
    std::string type = gsAuthType.value();
    if (type == "SERVICE_ACCOUNT_JSON_KEYFILE") {
      auto gsAuthServiceAccountJsonKeyfile =
          conf->get<std::string>("spark.hadoop.fs.gs.auth.service.account.json.keyfile");
      if (gsAuthServiceAccountJsonKeyfile.hasValue()) {
        hiveConfMap[facebook::velox::connector::hive::HiveConfig::kGCSCredentialsPath] =
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

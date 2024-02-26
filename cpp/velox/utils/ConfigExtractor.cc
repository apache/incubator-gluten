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
#ifdef ENABLE_GCS
#include <fstream>
#endif

#include "utils/exception.h"
#include "velox/connectors/hive/HiveConfig.h"

namespace {

const std::string kVeloxFileHandleCacheEnabled = "spark.gluten.sql.columnar.backend.velox.fileHandleCacheEnabled";
const bool kVeloxFileHandleCacheEnabledDefault = false;

// Log granularity of AWS C++ SDK
const std::string kVeloxAwsSdkLogLevel = "spark.gluten.velox.awsSdkLogLevel";
const std::string kVeloxAwsSdkLogLevelDefault = "FATAL";
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

std::shared_ptr<facebook::velox::core::MemConfigMutable> getHiveConfig(
    const std::shared_ptr<const facebook::velox::Config>& conf) {
  auto hiveConf = std::make_shared<facebook::velox::core::MemConfigMutable>();

#ifdef ENABLE_S3
  std::string awsAccessKey = conf->get<std::string>("spark.hadoop.fs.s3a.access.key", "");
  std::string awsSecretKey = conf->get<std::string>("spark.hadoop.fs.s3a.secret.key", "");
  std::string awsEndpoint = conf->get<std::string>("spark.hadoop.fs.s3a.endpoint", "");
  bool sslEnabled = conf->get<bool>("spark.hadoop.fs.s3a.connection.ssl.enabled", false);
  bool pathStyleAccess = conf->get<bool>("spark.hadoop.fs.s3a.path.style.access", false);
  bool useInstanceCredentials = conf->get<bool>("spark.hadoop.fs.s3a.use.instance.credentials", false);
  std::string iamRole = conf->get<std::string>("spark.hadoop.fs.s3a.iam.role", "");
  std::string iamRoleSessionName = conf->get<std::string>("spark.hadoop.fs.s3a.iam.role.session.name", "");

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

  if (useInstanceCredentials) {
    hiveConf->setValue(facebook::velox::connector::hive::HiveConfig::kS3UseInstanceCredentials, "true");
  } else if (!iamRole.empty()) {
    hiveConf->setValue(facebook::velox::connector::hive::HiveConfig::kS3IamRole, iamRole);
    if (!iamRoleSessionName.empty()) {
      hiveConf->setValue(facebook::velox::connector::hive::HiveConfig::kS3IamRoleSessionName, iamRoleSessionName);
    }
  } else {
    hiveConf->setValue(facebook::velox::connector::hive::HiveConfig::kS3AwsAccessKey, awsAccessKey);
    hiveConf->setValue(facebook::velox::connector::hive::HiveConfig::kS3AwsSecretKey, awsSecretKey);
  }
  // Only need to set s3 endpoint when not use instance credentials.
  if (!useInstanceCredentials) {
    hiveConf->setValue(facebook::velox::connector::hive::HiveConfig::kS3Endpoint, awsEndpoint);
  }
  hiveConf->setValue(facebook::velox::connector::hive::HiveConfig::kS3SSLEnabled, sslEnabled ? "true" : "false");
  hiveConf->setValue(
      facebook::velox::connector::hive::HiveConfig::kS3PathStyleAccess, pathStyleAccess ? "true" : "false");
  hiveConf->setValue(facebook::velox::connector::hive::HiveConfig::kS3LogLevel, awsSdkLogLevel);
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
      hiveConf->setValue(facebook::velox::connector::hive::HiveConfig::kGCSScheme, gcsScheme);
      hiveConf->setValue(facebook::velox::connector::hive::HiveConfig::kGCSEndpoint, gcsEndpoint);
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
        hiveConf->setValue(facebook::velox::connector::hive::HiveConfig::kGCSCredentials, gsAuthServiceAccountJson);
      } else {
        LOG(WARNING) << "STARTUP: conf spark.hadoop.fs.gs.auth.type is set to SERVICE_ACCOUNT_JSON_KEYFILE, "
                        "however conf spark.hadoop.fs.gs.auth.service.account.json.keyfile is not set";
        throw GlutenException("Conf spark.hadoop.fs.gs.auth.service.account.json.keyfile is not set");
      }
    }
  }
#endif

  hiveConf->setValue(
      facebook::velox::connector::hive::HiveConfig::kEnableFileHandleCache,
      conf->get<bool>(kVeloxFileHandleCacheEnabled, kVeloxFileHandleCacheEnabledDefault) ? "true" : "false");

  return hiveConf;
}

} // namespace gluten

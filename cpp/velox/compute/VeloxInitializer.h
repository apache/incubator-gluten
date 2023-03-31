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

#pragma once

#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <folly/executors/IOThreadPoolExecutor.h>

#include "VeloxColumnarToRowConverter.h"
#include "WholeStageResultIterator.h"

namespace gluten {

/// As a static instance in per executor, initialized at executor startup.
/// Should not put heavily work here.
class VeloxInitializer {
 public:
  explicit VeloxInitializer(std::unordered_map<std::string, std::string>& conf) {
    Init(conf);
  }

  ~VeloxInitializer() {
    if (dynamic_cast<facebook::velox::cache::AsyncDataCache*>(asyncDataCache_.get())) {
      LOG(INFO) << asyncDataCache_->toString();
    }
  }

  void Init(std::unordered_map<std::string, std::string>& conf);

  void InitCache(std::unordered_map<std::string, std::string>& conf);

  static facebook::velox::memory::MemoryAllocator* getAsyncDataCache();

  std::string genUuid() {
    return boost::lexical_cast<std::string>(boost::uuids::random_generator()());
  }

  // Instance of AsyncDataCache used for all large allocations.
  static std::shared_ptr<facebook::velox::memory::MemoryAllocator> asyncDataCache_;

  std::unique_ptr<folly::IOThreadPoolExecutor> ssdCacheExecutor_;
  std::unique_ptr<folly::IOThreadPoolExecutor> ioExecutor_;
  const std::string kVeloxCacheEnabled = "spark.gluten.sql.columnar.backend.velox.cacheEnabled";

  // memory cache
  const std::string kVeloxMemCacheSize = "spark.gluten.sql.columnar.backend.velox.memCacheSize";
  const std::string kVeloxMemCacheSizeDefault = "1073741824";

  // ssd cache
  const std::string kVeloxSsdCacheSize = "spark.gluten.sql.columnar.backend.velox.ssdCacheSize";
  const std::string kVeloxSsdCacheSizeDefault = "1073741824";
  const std::string kVeloxSsdCachePath = "spark.gluten.sql.columnar.backend.velox.ssdCachePath";
  const std::string kVeloxSsdCachePathDefault = "/tmp/";
  const std::string kVeloxSsdCacheShards = "spark.gluten.sql.columnar.backend.velox.ssdCacheShards";
  const std::string kVeloxSsdCacheShardsDefault = "1";
  const std::string kVeloxSsdCacheIOThreads = "spark.gluten.sql.columnar.backend.velox.ssdCacheIOThreads";
  const std::string kVeloxSsdCacheIOThreadsDefault = "1";
  const std::string kVeloxSsdODirectEnabled = "spark.gluten.sql.columnar.backend.velox.ssdODirect";

  const std::string kVeloxIOThreads = "spark.gluten.sql.columnar.backend.velox.IOThreads";
  const std::string kVeloxIOThreadsDefault = "1";
};

} // namespace gluten

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
#include <filesystem>

#include "velox/common/caching/AsyncDataCache.h"
#include "velox/common/memory/MemoryPool.h"
#include "velox/common/memory/MmapAllocator.h"
#include "velox/core/Config.h"

namespace gluten {
/// As a static instance in per executor, initialized at executor startup.
/// Should not put heavily work here.
class VeloxBackend {
 public:
  ~VeloxBackend() {
    if (dynamic_cast<facebook::velox::cache::AsyncDataCache*>(asyncDataCache_.get())) {
      LOG(INFO) << asyncDataCache_->toString();
      for (const auto& entry : std::filesystem::directory_iterator(cachePathPrefix_)) {
        if (entry.path().filename().string().find(cacheFilePrefix_) != std::string::npos) {
          LOG(INFO) << "Removing cache file " << entry.path().filename().string();
          std::filesystem::remove(cachePathPrefix_ + "/" + entry.path().filename().string());
        }
      }
      asyncDataCache_->shutdown();
    }
  }

  static void create(const std::unordered_map<std::string, std::string>& conf);

  static VeloxBackend* get();

  facebook::velox::cache::AsyncDataCache* getAsyncDataCache() const;

  const std::unordered_map<std::string, std::string>& getBackendConf() const;

 private:
  explicit VeloxBackend(const std::unordered_map<std::string, std::string>& conf) {
    init(conf);
  }

  void init(const std::unordered_map<std::string, std::string>& conf);
  void initCache(const std::shared_ptr<const facebook::velox::Config>& conf);
  void initConnector(const std::shared_ptr<const facebook::velox::Config>& conf);
  void initUdf(const std::shared_ptr<const facebook::velox::Config>& conf);

  void initJolFilesystem(const std::shared_ptr<const facebook::velox::Config>& conf);

  std::string getCacheFilePrefix() {
    return "cache." + boost::lexical_cast<std::string>(boost::uuids::random_generator()()) + ".";
  }

  static std::unique_ptr<VeloxBackend> instance_;

  // Instance of AsyncDataCache used for all large allocations.
  std::shared_ptr<facebook::velox::cache::AsyncDataCache> asyncDataCache_;

  std::unique_ptr<folly::IOThreadPoolExecutor> ssdCacheExecutor_;
  std::unique_ptr<folly::IOThreadPoolExecutor> ioExecutor_;
  std::shared_ptr<facebook::velox::memory::MmapAllocator> cacheAllocator_;

  std::string cachePathPrefix_;
  std::string cacheFilePrefix_;

  std::unordered_map<std::string, std::string> backendConf_{};
};

} // namespace gluten

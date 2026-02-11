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

#include <folly/Synchronized.h>

#include <chrono>
#include <condition_variable>
#include <map>
#include <mutex>
#include <queue>
#include <utility>

namespace gluten {

class GpuBufferColumnarBatch;

class CachedBufferQueue {
 public:
  CachedBufferQueue(int64_t capacity) : capacity_(capacity) {}

  void put(std::shared_ptr<GpuBufferColumnarBatch> batch);

  std::shared_ptr<GpuBufferColumnarBatch> get();

  void noMoreBatches();

  int64_t size() const;

  bool empty() const;

 private:
  int64_t capacity_;
  int64_t totalSize_{0};
  bool noMoreBatches_{false};

  std::queue<std::shared_ptr<GpuBufferColumnarBatch>> queue_;

  std::mutex m_;
  std::condition_variable notEmpty_;
  std::condition_variable notFull_;
};

} // namespace gluten

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

#include "utils/TestAllocationListener.h"
#include "velox/common/base/SuccinctPrinter.h"

#include <fmt/format.h>

namespace gluten {

void TestAllocationListener::allocationChanged(int64_t diff) {
  if (diff > 0 && usedBytes_ + diff >= limit_) {
    LOG(INFO) << fmt::format(
        "reach hard limit {} when need {}, current used {}.",
        facebook::velox::succinctBytes(limit_),
        facebook::velox::succinctBytes(diff),
        facebook::velox::succinctBytes(usedBytes_));
    auto neededBytes = usedBytes_ + diff - limit_;
    int64_t spilledBytes = 0;
    if (iterator_) {
      spilledBytes += iterator_->spillFixedSize(neededBytes);
    }
    if (spilledBytes < neededBytes && shuffleWriter_) {
      int64_t reclaimed = 0;
      GLUTEN_THROW_NOT_OK(shuffleWriter_->reclaimFixedSize(neededBytes - spilledBytes, &reclaimed));
      spilledBytes += reclaimed;
    }
    reclaimedBytes_ += spilledBytes;
    LOG(INFO) << fmt::format("spill finish, got {}.", facebook::velox::succinctBytes(spilledBytes));

    if (spilledBytes < neededBytes && throwIfOOM_) {
      throw GlutenException(fmt::format(
          "Failed to reclaim {} bytes. Actual bytes reclaimed: {}",
          facebook::velox::succinctBytes(neededBytes),
          facebook::velox::succinctBytes(spilledBytes)));
    }
  }

  usedBytes_ += diff;
}

int64_t TestAllocationListener::currentBytes() {
  return usedBytes_;
}

int64_t TestAllocationListener::reclaimedBytes() const {
  return reclaimedBytes_;
}

void TestAllocationListener::reset() {
  usedBytes_ = 0;
  reclaimedBytes_ = 0;
  limit_ = std::numeric_limits<uint64_t>::max();
  iterator_ = nullptr;
  shuffleWriter_ = nullptr;
  throwIfOOM_ = false;
}
} // namespace gluten

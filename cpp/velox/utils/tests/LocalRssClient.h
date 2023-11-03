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

#include <arrow/buffer.h>
#include <arrow/util/io_util.h>
#include <map>
#include "shuffle/rss/RssClient.h"
#include "utils/Common.h"
#include "utils/macros.h"

namespace gluten {

class LocalRssClient : public RssClient {
 public:
  LocalRssClient(std::string dataFile) : dataFile_(dataFile) {}

  int32_t pushPartitionData(int32_t partitionId, char* bytes, int64_t size) {
    auto idx = -1;
    auto maybeIdx = partitionIdx_.find(partitionId);
    auto returnSize = size;
    if (maybeIdx == partitionIdx_.end()) {
      idx = partitionIdx_.size();
      partitionIdx_[partitionId] = idx;
      auto buffer = arrow::AllocateResizableBuffer(0).ValueOrDie();
      partitionBuffers_.push_back(std::move(buffer));
      // Add EOS length.
      returnSize += sizeof(int32_t) * 2;
    } else {
      idx = maybeIdx->second;
    }

    auto& buffer = partitionBuffers_[idx];
    auto newSize = buffer->size() + size;
    if (buffer->capacity() < newSize) {
      GLUTEN_THROW_NOT_OK(buffer->Reserve(newSize));
    }
    memcpy(buffer->mutable_data() + buffer->size(), bytes, size);
    GLUTEN_THROW_NOT_OK(buffer->Resize(newSize));
    return returnSize;
  }

  void stop() {
    std::shared_ptr<arrow::io::FileOutputStream> fout;
    GLUTEN_ASSIGN_OR_THROW(fout, arrow::io::FileOutputStream::Open(dataFile_));

    int64_t bytes; // unused
    for (auto item : partitionIdx_) {
      auto idx = item.second;
      GLUTEN_THROW_NOT_OK(fout->Write(partitionBuffers_[idx]->data(), partitionBuffers_[idx]->size()));
      GLUTEN_THROW_NOT_OK(writeEos(fout.get(), &bytes));
      GLUTEN_THROW_NOT_OK(fout->Flush());
    }
    GLUTEN_THROW_NOT_OK(fout->Close());
    partitionBuffers_.clear();
    partitionIdx_.clear();
  }

 private:
  std::string dataFile_;
  std::vector<std::unique_ptr<arrow::ResizableBuffer>> partitionBuffers_;
  std::map<uint32_t, uint32_t> partitionIdx_;
};

} // namespace gluten

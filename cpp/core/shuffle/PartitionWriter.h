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

#include "shuffle/Options.h"
#include "shuffle/ShuffleWriter.h"

namespace gluten {

class EvictHandle {
 public:
  virtual ~EvictHandle() = default;

  virtual arrow::Status evict(uint32_t partitionId, std::unique_ptr<arrow::ipc::IpcPayload> payload) = 0;

  virtual arrow::Status finish() = 0;
};

class ShuffleWriter::PartitionWriter {
 public:
  PartitionWriter(ShuffleWriter* shuffleWriter) : shuffleWriter_(shuffleWriter) {}
  virtual ~PartitionWriter() = default;

  virtual arrow::Status init() = 0;

  virtual arrow::Status stop() = 0;

  /// Request next evict. The caller can use `requestNextEvict` to start a evict, and choose to call
  /// `getEvictHandle()->evict()` immediately, or to call it latter somewhere else.
  /// The caller can start new evict multiple times. Once it's called, the last `EvictHandle`
  /// will be finished automatically.
  /// \param flush Whether to flush the evicted data immediately. If it's false,
  /// the data can be cached first.
  virtual arrow::Status requestNextEvict(bool flush) = 0;

  /// Get the current managed EvictHandle. Returns nullptr if the current EvictHandle was finished,
  /// or requestNextEvict has not been called.
  /// \return
  virtual EvictHandle* getEvictHandle() = 0;

  virtual arrow::Status finishEvict() = 0;

  ShuffleWriter* shuffleWriter_;
};

} // namespace gluten

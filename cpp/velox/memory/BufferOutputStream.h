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

#include "velox/buffer/Buffer.h"
#include "velox/common/memory/ByteStream.h"

namespace gluten {
class BufferOutputStream : public facebook::velox::OutputStream {
 public:
  BufferOutputStream(
      facebook::velox::memory::MemoryPool* pool,
      int32_t initialSize = facebook::velox::memory::AllocationTraits::kPageSize,
      facebook::velox::OutputStreamListener* listener = nullptr);

  void write(const char* s, std::streamsize count);

  std::streampos tellp() const;

  void seekp(std::streampos pos);

  facebook::velox::BufferPtr getBuffer() const;

 private:
  facebook::velox::BufferPtr buffer_;
};
} // namespace gluten

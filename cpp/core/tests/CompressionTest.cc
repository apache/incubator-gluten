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

#include "compute/Backend.h"

#include <gtest/gtest.h>
#include "memory/ArrowMemoryPool.h"
#include "utils/compression.h"

namespace gluten {

class TestCompression : public ::testing::Test {
 protected:
  std::shared_ptr<arrow::MemoryPool> pool_ = defaultArrowMemoryPool();
};
TEST_F(TestCompression, compress) {
  std::string a = "this is test";
  auto buffer = std::make_shared<arrow::Buffer>(reinterpret_cast<uint8_t*>(a.data()), a.size());
  auto length = zstdMaxCompressedLength(a.size());
  GLUTEN_ASSIGN_OR_THROW(
      std::shared_ptr<arrow::ResizableBuffer> compressBuffer, arrow::AllocateResizableBuffer(length, pool_.get()));
  GLUTEN_THROW_NOT_OK(compressBuffer->Resize(0, false));
  zstdCompressBuffers({buffer}, compressBuffer.get());

  GLUTEN_ASSIGN_OR_THROW(
      std::shared_ptr<arrow::ResizableBuffer> uncompressed, arrow::AllocateResizableBuffer(a.size(), pool_.get()));
  GLUTEN_THROW_NOT_OK(uncompressed->Resize(0, false));
  zstdUncompress(compressBuffer.get(), uncompressed.get());
  ASSERT_TRUE(buffer->Equals(*uncompressed));
}
} // namespace gluten
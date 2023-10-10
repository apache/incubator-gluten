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

#include "shuffle/ShuffleReader.h"
#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"

namespace gluten {

class VeloxShuffleReader final : public ShuffleReader {
 public:
  VeloxShuffleReader(
      std::shared_ptr<arrow::Schema> schema,
      ReaderOptions options,
      arrow::MemoryPool* pool,
      std::shared_ptr<facebook::velox::memory::MemoryPool> veloxPool);

  std::shared_ptr<ResultIterator> readStream(std::shared_ptr<arrow::io::InputStream> in) override;

 private:
  facebook::velox::RowTypePtr rowType_;
  std::shared_ptr<facebook::velox::memory::MemoryPool> veloxPool_;
};

extern bool veloxShuffleReaderPrintFlag;

} // namespace gluten

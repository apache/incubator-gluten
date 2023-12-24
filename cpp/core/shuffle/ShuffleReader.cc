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

#include "ShuffleReader.h"
#include "arrow/ipc/reader.h"
#include "arrow/record_batch.h"
#include "utils/macros.h"

#include <utility>

#include "ShuffleSchema.h"

namespace gluten {

ShuffleReader::ShuffleReader(std::unique_ptr<DeserializerFactory> factory) : factory_(std::move(factory)) {}

std::shared_ptr<ResultIterator> ShuffleReader::readStream(std::shared_ptr<arrow::io::InputStream> in) {
  return std::make_shared<ResultIterator>(factory_->createDeserializer(in));
}

arrow::Status ShuffleReader::close() {
  return arrow::Status::OK();
}

arrow::MemoryPool* ShuffleReader::getPool() const {
  return factory_->getPool();
}

int64_t ShuffleReader::getDecompressTime() const {
  return factory_->getDecompressTime();
}

int64_t ShuffleReader::getIpcTime() const {
  return ipcTime_;
}

int64_t ShuffleReader::getDeserializeTime() const {
  return factory_->getDeserializeTime();
}

} // namespace gluten

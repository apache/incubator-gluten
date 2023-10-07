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

#include "ShuffleWriter.h"

#include <arrow/result.h>

#include "ShuffleSchema.h"
#include "utils/macros.h"

#include "PartitionWriterCreator.h"

namespace gluten {

#ifndef SPLIT_BUFFER_SIZE
// by default, allocate 8M block, 2M page size
#define SPLIT_BUFFER_SIZE 16 * 1024 * 1024
#endif

ShuffleWriterOptions ShuffleWriterOptions::defaults() {
  return {};
}

std::shared_ptr<arrow::Schema> ShuffleWriter::writeSchema() {
  if (writeSchema_ != nullptr) {
    return writeSchema_;
  }

  writeSchema_ = toWriteSchema(*schema_);
  return writeSchema_;
}

std::shared_ptr<arrow::Schema> ShuffleWriter::compressWriteSchema() {
  if (compressWriteSchema_ != nullptr) {
    return compressWriteSchema_;
  }

  compressWriteSchema_ = toCompressWriteSchema(*schema_);
  return compressWriteSchema_;
}

} // namespace gluten

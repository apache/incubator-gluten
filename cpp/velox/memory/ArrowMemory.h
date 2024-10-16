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

#include <arrow/io/memory.h>
#include "utils/Exception.h"
#include "velox/common/memory/ByteStream.h"

namespace gluten {

class ArrowFixedSizeBufferOutputStream : public facebook::velox::OutputStream {
 public:
  explicit ArrowFixedSizeBufferOutputStream(
      std::shared_ptr<arrow::io::FixedSizeBufferWriter> out,
      facebook::velox::OutputStreamListener* listener = nullptr)
      : OutputStream(listener), out_(out) {}

  void write(const char* s, std::streamsize count) override {
    GLUTEN_THROW_NOT_OK(out_->Write((void*)s, count));
    if (listener_) {
      listener_->onWrite(s, count);
    }
  }

  std::streampos tellp() const override {
    GLUTEN_ASSIGN_OR_THROW(auto pos, out_->Tell());
    return pos;
  }

  void seekp(std::streampos pos) override {
    GLUTEN_THROW_NOT_OK(out_->Seek(pos));
  }

 private:
  std::shared_ptr<arrow::io::FixedSizeBufferWriter> out_;
};

} // namespace gluten

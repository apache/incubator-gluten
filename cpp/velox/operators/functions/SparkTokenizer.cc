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

#include "operators/functions/SparkTokenizer.h"
#include "velox/type/Tokenizer.h"

namespace gluten {
namespace {

class SparkTokenizer : public facebook::velox::common::Tokenizer {
 public:
  explicit SparkTokenizer(const std::string& path) : path_(path) {
    state_ = State::kNotReady;
  }

  bool hasNext() override {
    if (state_ == State::kDone) {
      return false;
    } else if (state_ == State::kNotReady) {
      return true;
    }
    VELOX_FAIL("Illegal state.");
  }

  std::unique_ptr<facebook::velox::common::Subfield::PathElement> next() override {
    if (!hasNext()) {
      VELOX_USER_FAIL("No more tokens.");
    }
    state_ = State::kDone;
    return std::make_unique<facebook::velox::common::Subfield::NestedField>(path_);
  }

 private:
  const std::string path_;
  State state_;
};
} // namespace

void registerSparkTokenizer() {
  facebook::velox::common::Tokenizer::registerInstanceFactory(
      [](const std::string& p) { return std::make_unique<SparkTokenizer>(p); });
}

} // namespace gluten

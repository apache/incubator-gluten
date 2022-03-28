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

#include "compute/substrait_utils.h"

namespace gazellecpp {
namespace compute {

class ArrowSubstraitParser : public gazellejni::ExecBackendBase {
 public:
  ArrowSubstraitParser() {
    delegate_ = std::make_unique<gazellejni::compute::SubstraitParser>();
  }

  std::shared_ptr<gazellejni::RecordBatchResultIterator> GetResultIterator() override {
    return delegate_->GetResultIterator();
  }

  std::shared_ptr<gazellejni::RecordBatchResultIterator> GetResultIterator(
      std::vector<std::shared_ptr<gazellejni::RecordBatchResultIterator>> inputs)
      override {
    return delegate_->GetResultIterator(std::move(inputs));
  }

 private:
  std::unique_ptr<gazellejni::compute::SubstraitParser> delegate_;
};

}  // namespace compute
}  // namespace gazellecpp

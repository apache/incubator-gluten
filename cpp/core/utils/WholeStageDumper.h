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

#include "memory/ColumnarBatchIterator.h"

#include <memory>
#include <string>
#include <unordered_map>

namespace gluten {

class WholeStageDumper {
 public:
  virtual ~WholeStageDumper() = default;

  virtual void dumpConf(const std::unordered_map<std::string, std::string>& confMap) = 0;

  virtual void dumpPlan(const std::string& plan) = 0;

  virtual void dumpInputSplit(int32_t splitIndex, const std::string& splitInfo) = 0;

  virtual std::shared_ptr<ColumnarBatchIterator> dumpInputIterator(
      int32_t iteratorIndex,
      const std::shared_ptr<ColumnarBatchIterator>& inputIterator) = 0;
};

} // namespace gluten

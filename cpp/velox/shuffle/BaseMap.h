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

#include <velox/common/base/Exceptions.h>

#include <unordered_map>
#include <vector>

namespace gluten {
template <typename T>
class FlatMap;

class BaseMap {

public:
  template <typename T>
  const FlatMap<T>* asFlatMap() const {
    return dynamic_cast<const FlatMap<T>*>(this);
  }
  
};

using BaseMapPtr = std::shared_ptr<BaseMap>;

template <typename T>
class FlatMap : BaseMap {
public:
  std::unordered_map<T, int32_t> getValuesMap() {
    return values_;
  }
  std::unordered_map<T, int32_t> values_;
};

class RowVectorMap : BaseMap {
public:
  RowVectorMap(std::vector<BaseMapPtr> children): BaseMap(), childrenSize_(children.size()),
        children_(std::move(children)) {}

  /// Get the child vector at a given offset.
  BaseMapPtr& childAt(uint32_t index) {
    VELOX_CHECK_LT(
        index,
        childrenSize_,
        "Trying to access non-existing child in RowVectorMap");
    return children_[index];
  }

  uint32_t childrenSize() const {
    return childrenSize_;
  }

  std::vector<BaseMapPtr> children_;
  size_t childrenSize_;

};
}

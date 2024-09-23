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

#include "shuffle/Partitioning.h"
#include "utils/Exception.h"

namespace {
static const std::string kSinglePartitioningName = "single";
static const std::string kRoundRobinPartitioningName = "rr";
static const std::string kHashPartitioningName = "hash";
static const std::string kRangePartitioningName = "range";
static const std::string kRandomPartitioningName = "random";
} // namespace

namespace gluten {
Partitioning toPartitioning(std::string name) {
  if (name == kSinglePartitioningName) {
    return Partitioning::kSingle;
  }
  if (name == kRoundRobinPartitioningName) {
    return Partitioning::kRoundRobin;
  }
  if (name == kHashPartitioningName) {
    return Partitioning::kHash;
  }
  if (name == kRangePartitioningName) {
    return Partitioning::kRange;
  }
  if (name == kRandomPartitioningName) {
    return Partitioning::kRandom;
  }
  throw GlutenException("Invalid partition name: " + name);
}

} // namespace gluten

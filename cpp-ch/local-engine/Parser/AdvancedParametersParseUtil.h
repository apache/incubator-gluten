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
#include <unordered_map>
#include <base/types.h>

namespace local_engine
{
std::unordered_map<String, std::unordered_map<String, String>> convertToKVs(const String & advance);

struct JoinOptimizationInfo
{
    bool is_broadcast = false;
    bool is_smj = false;
    bool is_null_aware_anti_join = false;
    bool is_existence_join = false;
    bool is_any_join = false;
    Int64 left_table_rows = -1;
    Int64 left_table_bytes = -1;
    Int64 right_table_rows = -1;
    Int64 right_table_bytes = -1;
    Int64 partitions_num = -1;
    String storage_join_key;

    static JoinOptimizationInfo parse(const String & advance);
};

struct AggregateOptimizationInfo
{
    bool has_pre_partial_aggregate = false;
    bool has_required_child_distribution_expressions = false;
    static AggregateOptimizationInfo parse(const String & advance);
};

struct WindowGroupOptimizationInfo
{
    String window_function;
    bool is_aggregate_group_limit = false;
    static WindowGroupOptimizationInfo parse(const String & advnace);
};
}

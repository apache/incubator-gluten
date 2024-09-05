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

#include <Common/GlutenSettings.h>

namespace local_engine
{
#define MERGE_TREE_WRITE_RELATED_SETTINGS(M, ALIAS, UNIQ) \
    M(String, part_name_prefix, , "The part name prefix for writing data", UNIQ) \
    M(String, partition_dir, , "The parition directory for writing data", UNIQ) \
    M(String, bucket_dir, , "The bucket directory for writing data", UNIQ)

DECLARE_GLUTEN_SETTINGS(SparkMergeTreeWritePartitionSettings, MERGE_TREE_WRITE_RELATED_SETTINGS)

struct SparkMergeTreeWriteSettings
{
    SparkMergeTreeWritePartitionSettings partition_settings;
    bool merge_after_insert{true};
    bool insert_without_local_storage{false};
    size_t merge_min_size = 1024 * 1024 * 1024;
    size_t merge_limit_parts = 10;

    void load(const DB::ContextPtr & context);
};
}
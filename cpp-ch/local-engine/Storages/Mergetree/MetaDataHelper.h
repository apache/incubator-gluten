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

#include <Storages/StorageMergeTreeFactory.h>
#include <Common/MergeTreeTool.h>

namespace local_engine
{

void restoreMetaData(CustomStorageMergeTreePtr & storage, const MergeTreeTable & mergeTreeTable, const Context & context);

void saveFileStatus(
    const DB::MergeTreeData & storage,
    const DB::ContextPtr& context,
    const String & part_name,
    IDataPartStorage & data_part_storage);

std::vector<MergeTreeDataPartPtr> mergeParts(
    std::vector<DB::DataPartPtr> selected_parts,
    std::unordered_map<String, String> & partition_values,
    const String & new_part_uuid,
    CustomStorageMergeTreePtr storage,
    const String & partition_dir,
    const String & bucket_dir);

void extractPartitionValues(const String & partition_dir, std::unordered_map<String, String> & partition_values);
}

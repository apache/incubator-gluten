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

#include <Interpreters/TableJoin.h>
#include <Interpreters/TreeRewriter.h>

#include <Interpreters/MergeTreeTransaction.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <substrait/plan.pb.h>

namespace DB
{
class ReadBufferFromString;
}
namespace local_engine
{
class SparkStorageMergeTree;
using SparkStorageMergeTreePtr = std::shared_ptr<SparkStorageMergeTree>;
using namespace DB;

struct MergeTreePart
{
    std::string name;
    size_t begin;
    size_t end;
};

struct MergeTreeTableSettings
{
    std::string storage_policy{};
};

struct MergeTreeTable
{
    static constexpr std::string_view TUPLE = "tuple()";
    std::string database;
    std::string table;
    std::string snapshot_id;
    substrait::NamedStruct schema;
    std::string order_by_key;
    std::string low_card_key;
    std::string minmax_index_key;
    std::string bf_index_key;
    std::string set_index_key;
    std::string primary_key{};
    std::string relative_path;
    std::string absolute_path;
    MergeTreeTableSettings table_configs;

    bool sameTable(const MergeTreeTable & other) const;

    SparkStorageMergeTreePtr getStorage(ContextMutablePtr context) const;

    /// Create random table name and table path and use default storage policy.
    /// In insert case, mergetree data can be uploaded after merges in default storage(Local Disk).
    SparkStorageMergeTreePtr copyToDefaultPolicyStorage(const ContextMutablePtr & context) const;

    /// Use same table path and data path as the original table.
    SparkStorageMergeTreePtr copyToVirtualStorage(const ContextMutablePtr & context) const;

    std::shared_ptr<DB::StorageInMemoryMetadata> buildMetaData(const DB::Block & header, const ContextPtr & context) const;

    MergeTreeTable() = default;
    explicit MergeTreeTable(const substrait::WriteRel & write_rel);
};

struct MergeTreeTableInstance : MergeTreeTable
{
    std::vector<MergeTreePart> parts;
    std::unordered_set<std::string> getPartNames() const;
    RangesInDataParts extractRange(DataPartsVector parts_vector) const;

    SparkStorageMergeTreePtr restoreStorage(const ContextMutablePtr & context) const;

    explicit MergeTreeTableInstance(const google::protobuf::Any & any);
    explicit MergeTreeTableInstance(const substrait::ReadRel::ExtensionTable & extension_table);
    explicit MergeTreeTableInstance(const std::string & info);
};

std::unique_ptr<MergeTreeSettings> buildMergeTreeSettings(const MergeTreeTableSettings & config);

std::unique_ptr<SelectQueryInfo> buildQueryInfo(NamesAndTypesList & names_and_types_list);
}

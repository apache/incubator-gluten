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

#include <memory>
#include <optional>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Parser/RelParsers/RelParser.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <Storages/MergeTree/SparkMergeTreeMeta.h>
#include <Storages/Parquet/ParquetMeta.h>
#include <Storages/SubstraitSource/Delta/DeltaMeta.h>
#include <Storages/SubstraitSource/FormatFile.h>
#include <substrait/algebra.pb.h>
#include <Common/GlutenConfig.h>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}
}

namespace local_engine
{

using ReplaceDeltaNodeFunc = std::function<void(DB::ActionsDAG &, const MergeTreeTableInstance &, DB::ContextPtr)>;

void replaceInputFileNameNode(DB::ActionsDAG & actions_dag, const MergeTreeTableInstance & merge_tree_table, DB::ContextPtr context);
void replaceFilePathNode(DB::ActionsDAG & actions_dag, const MergeTreeTableInstance & merge_tree_table, DB::ContextPtr context);
void replaceFileNameNode(DB::ActionsDAG & actions_dag, const MergeTreeTableInstance & merge_tree_table, DB::ContextPtr context);
void replaceFileSizeNode(DB::ActionsDAG & actions_dag, const MergeTreeTableInstance & merge_tree_table, DB::ContextPtr context);
void replaceFileBlockStartNode(DB::ActionsDAG & actions_dag, const MergeTreeTableInstance & merge_tree_table, DB::ContextPtr context);
void replaceFileBlockLengthNode(DB::ActionsDAG & actions_dag, const MergeTreeTableInstance & merge_tree_table, DB::ContextPtr context);
void replaceFileModificationTimeNode(DB::ActionsDAG & actions_dag, const MergeTreeTableInstance & merge_tree_table, DB::ContextPtr context);
void replaceDeltaInternalRowDeletedNode(DB::ActionsDAG & actions_dag, const MergeTreeTableInstance & merge_tree_table, DB::ContextPtr context);
void replaceInputFileBlockStartNode(DB::ActionsDAG & actions_dag, const MergeTreeTableInstance & merge_tree_table, DB::ContextPtr context);
void replaceInputFileBlockLengthNode(DB::ActionsDAG & actions_dag, const MergeTreeTableInstance & merge_tree_table, DB::ContextPtr context);
void replaceTmpRowIndexNode(DB::ActionsDAG & actions_dag, const MergeTreeTableInstance & merge_tree_table, DB::ContextPtr context);

const std::unordered_map<String, std::tuple<std::optional<String>, DB::DataTypePtr, ReplaceDeltaNodeFunc>> & getDeltaMetaColumnMap();

class MergeTreeRelParser : public RelParser
{
public:
    inline static const std::string VIRTUAL_COLUMN_PART = "_part";

    explicit MergeTreeRelParser(ParserContextPtr parser_context_, const DB::ContextPtr & context_)
        : RelParser(parser_context_), context(context_)
    {
        spark_sql_config = SparkSQLConfig::loadFromContext(context);
    }

    ~MergeTreeRelParser() override = default;

    DB::QueryPlanPtr parse(DB::QueryPlanPtr query_plan, const substrait::Rel & rel, std::list<const substrait::Rel *> & rel_stack_) override
    {
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "MergeTreeRelParser can't call parse(), call parseReadRel instead.");
    }

    DB::QueryPlanPtr parseReadRel(
        DB::QueryPlanPtr query_plan, const substrait::ReadRel & read_rel, const substrait::ReadRel::ExtensionTable & extension_table);

    std::optional<const substrait::Rel *> getSingleInput(const substrait::Rel &) override
    {
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "MergeTreeRelParser can't call getSingleInput().");
    }

    String filterRangesOnDriver(const substrait::ReadRel & read_rel);

    struct Condition
    {
        explicit Condition(const substrait::Expression & node_) : node(node_) { }

        const substrait::Expression node;
        size_t columns_size = 0;
        DB::NameSet table_columns;
        Int64 min_position_in_primary_key = std::numeric_limits<Int64>::max() - 1;

        auto tuple() const { return std::make_tuple(-min_position_in_primary_key, columns_size, table_columns.size()); }

        bool operator<(const Condition & rhs) const { return tuple() < rhs.tuple(); }
    };
    using Conditions = std::list<Condition>;

    // visable for test
    void analyzeExpressions(Conditions & res, const substrait::Expression & rel, std::set<Int64> & pk_positions, const DB::Block & block);

public:
    std::unordered_map<std::string, UInt64> column_sizes;

private:
    DB::Block parseMergeTreeOutput(const substrait::ReadRel & rel, SparkStorageMergeTreePtr storage);
    DB::Block replaceDeltaNameIfNeeded(const DB::Block & output);
    void replaceNodeWithCaseSensitive(DB::Block & read_block, SparkStorageMergeTreePtr storage);
    void recoverDeltaNameIfNeeded(DB::QueryPlan & plan, const DB::Block & output, const MergeTreeTableInstance & merge_tree_table);
    void recoverNodeWithCaseSensitive(DB::QueryPlan & query_plan, const DB::Block & output);
    void parseToAction(DB::ActionsDAG & filter_action, const substrait::Expression & rel, std::string & filter_name) const;
    DB::PrewhereInfoPtr parsePreWhereInfo(const substrait::Expression & rel, const DB::Block & input);
    DB::ActionsDAG optimizePrewhereAction(const substrait::Expression & rel, std::string & filter_name, const DB::Block & block);
    String getCHFunctionName(const substrait::Expression_ScalarFunction & substrait_func) const;
    static void collectColumns(const substrait::Expression & rel, DB::NameSet & columns, const DB::Block & block);
    UInt64 getColumnsSize(const DB::NameSet & columns);

    DB::ContextPtr context;
    SparkSQLConfig spark_sql_config;
};

}

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

#include <Core/Block.h>
#include <Core/SortDescription.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Interpreters/Aggregator.h>
#include <Parser/CHColumnToSparkRow.h>
#include <Parser/RelMetric.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Storages/MergeTree/SparkStorageMergeTree.h>
#include <Storages/SourceFromJavaIter.h>
#include <base/types.h>
#include <substrait/plan.pb.h>

namespace local_engine
{

std::string join(const ActionsDAG::NodeRawConstPtrs & v, char c);

class SerializedPlanParser;
class LocalExecutor;

// Give a condition expression `cond_rel_`, found all columns with nullability that must not containt
// null after this filter.
// It's used to remove nullability of the columns for performance reason.
class NonNullableColumnsResolver
{
public:
    explicit NonNullableColumnsResolver(const DB::Block & header_, SerializedPlanParser & parser_, const substrait::Expression & cond_rel_);
    ~NonNullableColumnsResolver() = default;

    // return column names
    std::set<String> resolve();

private:
    DB::Block header;
    SerializedPlanParser & parser;
    const substrait::Expression & cond_rel;

    std::set<String> collected_columns;

    void visit(const substrait::Expression & expr);
    void visitNonNullable(const substrait::Expression & expr);

    String safeGetFunctionName(const String & function_signature, const substrait::Expression_ScalarFunction & function) const;
};

class SerializedPlanParser
{
private:
    friend class RelParser;
    friend class RelRewriter;
    friend class ASTParser;
    friend class FunctionParser;
    friend class AggregateFunctionParser;
    friend class FunctionExecutor;
    friend class NonNullableColumnsResolver;
    friend class JoinRelParser;
    friend class CrossRelParser;
    friend class MergeTreeRelParser;
    friend class ProjectRelParser;

    std::unique_ptr<LocalExecutor> createExecutor(DB::QueryPlanPtr query_plan, const substrait::Plan & s_plan);

public:
    explicit SerializedPlanParser(const ContextPtr & context);

    /// visible for UT
    DB::QueryPlanPtr parse(const substrait::Plan & plan);
    std::unique_ptr<LocalExecutor> createExecutor(const substrait::Plan & plan);
    DB::QueryPipelineBuilderPtr buildQueryPipeline(DB::QueryPlan & query_plan);
    ///
    std::unique_ptr<LocalExecutor> createExecutor(const std::string_view plan);

    void addInputIter(jobject iter, bool materialize_input)
    {
        input_iters.emplace_back(iter);
        materialize_inputs.emplace_back(materialize_input);
    }

    std::pair<jobject, bool> getInputIter(size_t index)
    {
        if (index > input_iters.size())
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Index({}) is overflow input_iters's size({})", index, input_iters.size());
        return {input_iters[index], materialize_inputs[index]};
    }

    void addSplitInfo(std::string && split_info) { split_infos.emplace_back(std::move(split_info)); }

    int nextSplitInfoIndex()
    {
        if (split_info_index >= split_infos.size())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "split info index out of range, split_info_index: {}, split_infos.size(): {}",
                split_info_index,
                split_infos.size());
        return split_info_index++;
    }

    const String & nextSplitInfo()
    {
        auto next_index = nextSplitInfoIndex();
        return split_infos.at(next_index);
    }

    void parseExtensions(const ::google::protobuf::RepeatedPtrField<substrait::extensions::SimpleExtensionDeclaration> & extensions);
    DB::ActionsDAG expressionsToActionsDAG(
        const std::vector<substrait::Expression> & expressions, const DB::Block & header, const DB::Block & read_schema);
    RelMetricPtr getMetric() { return metrics.empty() ? nullptr : metrics.at(0); }
    const std::unordered_map<std::string, std::string> & getFunctionMapping() { return function_mapping; }

    std::string getFunctionName(const std::string & function_sig, const substrait::Expression_ScalarFunction & function);
    std::optional<std::string> getFunctionSignatureName(UInt32 function_ref) const;

    IQueryPlanStep * addRemoveNullableStep(QueryPlan & plan, const std::set<String> & columns);
    IQueryPlanStep * addRollbackFilterHeaderStep(QueryPlanPtr & query_plan, const Block & input_header);

    static std::pair<DataTypePtr, Field> parseLiteral(const substrait::Expression_Literal & literal);
    ContextPtr getContext() const { return context; }

    std::vector<QueryPlanPtr> extra_plan_holder;

private:
    DB::QueryPlanPtr parseOp(const substrait::Rel & rel, std::list<const substrait::Rel *> & rel_stack);

    void parseFunctionOrExpression(
        const substrait::Expression & rel, std::string & result_name, DB::ActionsDAG & actions_dag, bool keep_result = false);
    void parseJsonTuple(
        const substrait::Expression & rel,
        std::vector<String> & result_names,
        DB::ActionsDAG & actions_dag,
        bool keep_result = false,
        bool position = false);
    const ActionsDAG::Node * parseFunctionWithDAG(
        const substrait::Expression & rel, std::string & result_name, DB::ActionsDAG & actions_dag, bool keep_result = false);
    ActionsDAG::NodeRawConstPtrs parseArrayJoinWithDAG(
        const substrait::Expression & rel,
        std::vector<String> & result_name,
        DB::ActionsDAG & actions_dag,
        bool keep_result = false,
        bool position = false);
    void parseFunctionArguments(
        DB::ActionsDAG & actions_dag,
        ActionsDAG::NodeRawConstPtrs & parsed_args,
        const substrait::Expression_ScalarFunction & scalar_function);

    void parseArrayJoinArguments(
        DB::ActionsDAG & actions_dag,
        const std::string & function_name,
        const substrait::Expression_ScalarFunction & scalar_function,
        bool position,
        ActionsDAG::NodeRawConstPtrs & parsed_args,
        bool & is_map);


    const DB::ActionsDAG::Node * parseExpression(DB::ActionsDAG & actions_dag, const substrait::Expression & rel);
    const ActionsDAG::Node *
    toFunctionNode(ActionsDAG & actions_dag, const String & function, const DB::ActionsDAG::NodeRawConstPtrs & args);
    // remove nullable after isNotNull
    void removeNullableForRequiredColumns(const std::set<String> & require_columns, ActionsDAG & actions_dag) const;
    std::string getUniqueName(const std::string & name) { return name + "_" + std::to_string(name_no++); }
    void wrapNullable(
        const std::vector<String> & columns, ActionsDAG & actions_dag, std::map<std::string, std::string> & nullable_measure_names);
    static std::pair<DB::DataTypePtr, DB::Field> convertStructFieldType(const DB::DataTypePtr & type, const DB::Field & field);

    bool isFunction(substrait::Expression_ScalarFunction rel, String function_name);

    int name_no = 0;
    std::unordered_map<std::string, std::string> function_mapping;
    std::vector<jobject> input_iters;
    std::vector<std::string> split_infos;
    int split_info_index = 0;
    std::vector<bool> materialize_inputs;
    ContextPtr context;
    std::vector<RelMetricPtr> metrics;

public:
    const ActionsDAG::Node * addColumn(DB::ActionsDAG & actions_dag, const DataTypePtr & type, const Field & field);
};

}

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

#include "ReadRelParser.h"
#include <memory>
#include <Core/Block.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Operator/BlocksBufferPoolTransform.h>
#include <Parser/RelParsers/MergeTreeRelParser.h>
#include <Parser/SubstraitParserUtils.h>
#include <Parser/TypeParser.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Storages/SourceFromJavaIter.h>
#include <Storages/SubstraitSource/SubstraitFileSource.h>
#include <Storages/SubstraitSource/SubstraitFileSourceStep.h>
#include <google/protobuf/wrappers.pb.h>
#include <Common/BlockTypeUtils.h>


namespace DB::ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace local_engine
{
DB::QueryPlanPtr ReadRelParser::parse(DB::QueryPlanPtr query_plan, const substrait::Rel & rel, std::list<const substrait::Rel *> &)
{
    if (query_plan)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Source node's input plan should be null");
    const auto & read = rel.read();
    if (read.has_local_files() || (!read.has_extension_table() && !isReadFromMergeTree(read)))
    {
        assert(read.has_base_schema());
        DB::QueryPlanStepPtr read_step;
        if (isReadRelFromJava(read))
            read_step = parseReadRelWithJavaIter(read);
        else
            read_step = parseReadRelWithLocalFile(read);
        query_plan = std::make_unique<DB::QueryPlan>();
        steps.emplace_back(read_step.get());
        query_plan->addStep(std::move(read_step));

        if (getContext()->getSettingsRef().max_threads > 1)
        {
            auto buffer_step = std::make_unique<BlocksBufferPoolStep>(query_plan->getCurrentDataStream());
            steps.emplace_back(buffer_step.get());
            query_plan->addStep(std::move(buffer_step));
        }
    }
    else
    {
        substrait::ReadRel::ExtensionTable extension_table;
        if (read.has_extension_table())
            extension_table = read.extension_table();
        else
        {
            extension_table = BinaryToMessage<substrait::ReadRel::ExtensionTable>(split_info);
            logDebugMessage(extension_table, "extension_table");
        }
        MergeTreeRelParser mergeTreeParser(getPlanParser(), getContext());
        query_plan = mergeTreeParser.parseReadRel(std::make_unique<DB::QueryPlan>(), read, extension_table);
        steps = mergeTreeParser.getSteps();
    }
    return query_plan;
}

bool ReadRelParser::isReadRelFromJava(const substrait::ReadRel & rel)
{
    return rel.has_local_files() && rel.local_files().items().size() == 1
        && rel.local_files().items().at(0).uri_file().starts_with("iterator");
}

bool ReadRelParser::isReadFromMergeTree(const substrait::ReadRel & rel)
{
    assert(rel.has_advanced_extension());
    bool is_read_from_merge_tree;
    google::protobuf::StringValue optimization;
    optimization.ParseFromString(rel.advanced_extension().optimization().value());
    ReadBufferFromString in(optimization.value());
    if (!checkString("isMergeTree=", in))
        return false;
    readBoolText(is_read_from_merge_tree, in);
    assertChar('\n', in);
    return is_read_from_merge_tree;
}

DB::QueryPlanStepPtr ReadRelParser::parseReadRelWithJavaIter(const substrait::ReadRel & rel)
{
    GET_JNIENV(env)
    SCOPE_EXIT({CLEAN_JNIENV});
    auto first_block = SourceFromJavaIter::peekBlock(env, input_iter);

    /// Try to decide header from the first block read from Java iterator. Thus AggregateFunction with parameters has more precise types.
    auto header = first_block.has_value() ? first_block->cloneEmpty() : TypeParser::buildBlockFromNamedStruct(rel.base_schema());
    auto source = std::make_shared<SourceFromJavaIter>(
        getContext(), std::move(header), input_iter, is_input_iter_materialize, std::move(first_block));

    QueryPlanStepPtr source_step = std::make_unique<ReadFromPreparedSource>(Pipe(source));
    source_step->setStepDescription("Read From Java Iter");
    return source_step;
}

QueryPlanStepPtr ReadRelParser::parseReadRelWithLocalFile(const substrait::ReadRel & rel)
{
    auto header = TypeParser::buildBlockFromNamedStruct(rel.base_schema());
    substrait::ReadRel::LocalFiles local_files;
    if (rel.has_local_files())
        local_files = rel.local_files();
    else
    {
        local_files = BinaryToMessage<substrait::ReadRel::LocalFiles>(getPlanParser()->nextSplitInfo());
        logDebugMessage(local_files, "local_files");
    }
    auto source = std::make_shared<SubstraitFileSource>(getContext(), header, local_files);
    auto source_pipe = Pipe(source);
    auto source_step = std::make_unique<SubstraitFileSourceStep>(getContext(), std::move(source_pipe), "substrait local files");
    source_step->setStepDescription("read local files");
    if (rel.has_filter())
    {
        DB::ActionsDAG actions_dag{blockToNameAndTypeList(header)};
        const DB::ActionsDAG::Node * filter_node = parseExpression(actions_dag, rel.filter());
        actions_dag.addOrReplaceInOutputs(*filter_node);
        assert(filter_node == &(actions_dag.findInOutputs(filter_node->result_name)));
        source_step->addFilter(std::move(actions_dag), filter_node->result_name);
    }
    return source_step;
}

void registerReadRelParser(RelParserFactory & factory)
{
    auto builder = [](SerializedPlanParser * plan_parser_) { return std::make_unique<ReadRelParser>(plan_parser_); };
    factory.registerBuilder(substrait::Rel::RelTypeCase::kRead, builder);
}
}

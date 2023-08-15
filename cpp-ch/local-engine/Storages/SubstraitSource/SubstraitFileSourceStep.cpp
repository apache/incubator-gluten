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

#include <Storages/SubstraitSource/SubstraitFileSourceStep.h>
#include <Storages/SubstraitSource/SubstraitFileSource.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeArray.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int TYPE_MISMATCH;
}
}


namespace local_engine
{

SubstraitFileSourceStep::SubstraitFileSourceStep(DB::ContextPtr context_, DB::Pipe pipe_, const String &)
    : SourceStepWithFilter(DB::DataStream{.header = pipe_.getHeader()}), pipe(std::move(pipe_)), context(context_) 
{
    DB::Processors processors = pipe.getProcessors();
    for (size_t i = 0; i < processors.size(); ++i)
    {
        DB::ProcessorPtr processor = processors[i];
        const SubstraitFileSource * source = static_cast<const SubstraitFileSource *>(processor.get());
        if (source)
        {
            partition_keys = source->getPartitionKeys();
            file_format = source->getFileFormat();
        }
    }
}


void SubstraitFileSourceStep::initializePipeline(DB::QueryPipelineBuilder & pipeline, const DB::BuildQueryPipelineSettings &)
{
    for (const auto & processor : pipe.getProcessors())
        processors.emplace_back(processor);

    pipeline.init(std::move(pipe));
}

DB::NamesAndTypesList SubstraitFileSourceStep::extractParquetFileColumnPathAndTypeForComplexType(const DB::String & column_name, const DB::DataTypePtr & column_type)
{
    DB::NamesAndTypesList path_and_type;
    auto extract_path_and_type = [&](const DB::String & name, const DB::DataTypePtr & type, DB::NamesAndTypesList & result) -> void
    {
        DB::WhichDataType type_which(type);
        if (type_which.isMap() || type_which.isArray() || type_which.isTuple())
        {
            DB::NamesAndTypesList names_and_types = extractParquetFileColumnPathAndTypeForComplexType(name, type);
            result.insert(result.end(), names_and_types.begin(), names_and_types.end());
        }
        else
        {
            DB::NameAndTypePair path_type_pair(name, type);
            result.push_back(path_type_pair);
        }
    };

    DB::WhichDataType which_type(column_type);
    if (which_type.isMap())
    {
        const DB::DataTypeMap * map_type = dynamic_cast<const DB::DataTypeMap *>(column_type.get());
        DB::String key_path = column_name + ".key_value.key";
        DB::String value_path = column_name + ".key_value.value";
        DB::NameAndTypePair key_path_type_pair(key_path, map_type->getKeyType());
        path_and_type.push_back(key_path_type_pair);
        extract_path_and_type(value_path, removeNullable(map_type->getValueType()), path_and_type);
    }
    else if (which_type.isArray())
    {
        const DB::DataTypeArray * array_type = dynamic_cast<const DB::DataTypeArray *>(column_type.get());
        DB::String element_path = column_name + ".list.element";
        DB::DataTypePtr element_type = array_type->getNestedType();
        extract_path_and_type(element_path, removeNullable(element_type), path_and_type);
    }
    else if (which_type.isTuple())
    {
        const DB::DataTypeTuple * tuple_type = dynamic_cast<const DB::DataTypeTuple *>(column_type.get());
        DB::Names names = tuple_type->getElementNames();
        for (size_t i = 0; i < names.size(); ++i)
            extract_path_and_type(column_name + "." + names[i], removeNullable(tuple_type->getElement(i)), path_and_type);
    }
    else
        throw DB::Exception(DB::ErrorCodes::TYPE_MISMATCH, "Column {} type is not map/array/tuple, which not supported", column_name);
    
    return path_and_type;

}

void SubstraitFileSourceStep::applyFilters()
{
    if (filter_dags.size() == 0 || filter_nodes.nodes.size() == 0)
        return;
    std::vector<SourceFilter> filters;
    const DB::Block header = pipe.getHeader();
    const DB::ColumnsWithTypeAndName columns = header.getColumnsWithTypeAndName();
    std::unordered_map<std::string, DB::ColumnWithTypeAndName> node_name_to_input_column;
    DB::NamesAndTypesList filter_column_keys;
    for (size_t i=0; i < columns.size(); ++i)
    {
        DB::ColumnWithTypeAndName column = columns[i];
        auto ret = std::find(partition_keys.begin(), partition_keys.end(), column.name);
        if (ret == partition_keys.end())
        {
            node_name_to_input_column.insert({column.name, column});
            DB::DataTypePtr column_type_non_nullable = removeNullable(column.type);
            DB::WhichDataType which_type(column_type_non_nullable);
            if (which_type.isMap() || which_type.isArray() || which_type.isTuple())
            {
                if (file_format == "parquet")
                {
                    DB::NamesAndTypesList names_and_types = extractParquetFileColumnPathAndTypeForComplexType(column.name, column_type_non_nullable);
                    filter_column_keys.insert(filter_column_keys.end(), names_and_types.begin(), names_and_types.end());
                }
            }
            else
            {
                DB::NameAndTypePair name_and_type(column.name, column.type);
                filter_column_keys.push_back(name_and_type);
            }
        }
    }

    if (!filter_column_keys.empty())
    {
        std::shared_ptr<DB::ExpressionActions> filter_expr = std::make_shared<DB::ExpressionActions>(filter_dags[0], DB::ExpressionActionsSettings::fromContext(context));
        DB::ActionsDAGPtr filter_actions_dag = DB::ActionsDAG::buildFilterActionsDAG(filter_nodes.nodes, node_name_to_input_column, context);
        DB::KeyCondition filter_condition(filter_actions_dag, context, filter_column_keys.getNames(), filter_expr, DB::NameSet{});
        SourceFilter filter{filter_condition, filter_column_keys};
        filters.push_back(filter);
    }
    DB::Processors processors = pipe.getProcessors();
    for (size_t i = 0; i < processors.size(); ++i)
    {
        DB::ProcessorPtr processor = processors[i];
        const SubstraitFileSource * source = static_cast<const SubstraitFileSource *>(processor.get());
        if (source)
            source->applyFilters(filters);
    }
}

}

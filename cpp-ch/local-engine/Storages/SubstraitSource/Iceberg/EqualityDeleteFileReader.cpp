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

#include "EqualityDeleteFileReader.h"

#include <Columns/ColumnConst.h>
#include <Columns/ColumnSet.h>
#include <DataTypes/DataTypeSet.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/Formats/Impl/ParquetBlockInputFormat.h>
#include <Storages/SubstraitSource/Iceberg/SimpleParquetReader.h>
#include <Common/BlockTypeUtils.h>
using namespace DB;

namespace local_engine
{

namespace iceberg
{

const ActionsDAG::Node & EqualityDeleteActionBuilder::lastMerge()
{
    assert(!andArgs.empty());

    if (andArgs.size() == 1)
        return actions.addAlias(*andArgs[0], COLUMN_NAME);

    const std::string And_{"and"};
    auto andBuilder = FunctionFactory::instance().get(And_, context);
    return actions.addFunction(andBuilder, andArgs, COLUMN_NAME);
}

const ActionsDAG::Node * EqualityDeleteActionBuilder::Or(const ActionsDAG::NodeRawConstPtrs & orArgs)
{
    if (orArgs.size() == 1)
        return orArgs[0];

    const std::string Or_{"or"};
    auto OrBuilder = FunctionFactory::instance().get(Or_, context);
    return &addFunction(OrBuilder, orArgs);
}

const ActionsDAG::Node &
EqualityDeleteActionBuilder::addFunction(const FunctionOverloadResolverPtr & function, ActionsDAG::NodeRawConstPtrs args)
{
    return actions.addFunction(function, std::move(args), getUniqueName(function->getName()));
}

void EqualityDeleteActionBuilder::notIn(Block deleteBlock, const std::string & column_name)
{
    assert(deleteBlock.columns() == 1);
    const auto & elem_block = deleteBlock.getByPosition(0);

    const std::string notIn{"notIn"};

    ActionsDAG::NodeRawConstPtrs args;
    const auto & colName = column_name.empty() ? elem_block.name : column_name;
    args.push_back(&actions.findInOutputs(colName));
    PreparedSets prepared_sets;
    FutureSet::Hash emptyKey;
    auto future_set = prepared_sets.addFromTuple(emptyKey, nullptr, {elem_block}, context->getSettingsRef());
    auto arg = ColumnSet::create(1, std::move(future_set));
    args.emplace_back(&actions.addColumn(ColumnWithTypeAndName(std::move(arg), std::make_shared<DataTypeSet>(), "__set")));

    auto function_builder = FunctionFactory::instance().get(notIn, context);
    andArgs.push_back(&addFunction(function_builder, std::move(args)));
}

void EqualityDeleteActionBuilder::notEquals(Block deleteBlock, const Names & column_names)
{
    auto numDeleteFields = deleteBlock.columns();
    assert(deleteBlock.columns() > 1);

    const std::string notEqual{"notEquals"};
    auto notEqualBuilder = FunctionFactory::instance().get(notEqual, context);

    auto numDeletedValues = deleteBlock.rows();
    for (size_t i = 0; i < numDeletedValues; i++)
    {
        ActionsDAG::NodeRawConstPtrs orArgs = {};
        for (size_t j = 0; j < numDeleteFields; j++)
        {
            ActionsDAG::NodeRawConstPtrs args;
            const auto & column = deleteBlock.getByPosition(j);
            auto u_column = column.type->createColumn();
            u_column->reserve(1);
            u_column->insertFrom(*column.column, i);

            const auto & colName = column_names.empty() ? column.name : column_names[j];
            args.push_back(&actions.findInOutputs(colName));
            args.push_back(
                &actions.addColumn(ColumnWithTypeAndName(ColumnConst::create(std::move(u_column), 1), column.type, getUniqueName("c"))));
            orArgs.push_back(&addFunction(notEqualBuilder, std::move(args)));
        }
        assert(!orArgs.empty());
        andArgs.push_back(Or(orArgs));
    }
}

ExpressionActionsPtr EqualityDeleteActionBuilder::finish()
{
    if (andArgs.empty())
        return nullptr;

    actions.addOrReplaceInOutputs(lastMerge());
    return std::make_shared<ExpressionActions>(std::move(actions), ExpressionActionsSettings(context, CompileExpressions::no));
}

EqualityDeleteFileReader::EqualityDeleteFileReader(
    const ContextPtr & context, const Block & read_header, const SubstraitIcebergDeleteFile & deleteFile)
    : context_(context), deleteFile_(deleteFile)
{
    assert(deleteFile_.recordcount() > 0);
    assert(data_file_schema_for_delete_.columns() == 0);
    for (int i = 0; i < deleteFile_.equalityfieldids_size(); i++)
    {
        //TODO: deleteFile_.equalityfieldids(i) - 1 ? why
        auto index = deleteFile_.equalityfieldids(i) - 1;
        assert(index < read_header.columns());
        data_file_schema_for_delete_.insert(read_header.getByPosition(index).cloneEmpty());
    }
}

void EqualityDeleteFileReader::readDeleteValues(EqualityDeleteActionBuilder & expressionInputs) const
{
    assert(data_file_schema_for_delete_.columns() != 0);
    SimpleParquetReader reader{context_, deleteFile_};

    Block deleteBlock = reader.next();
    assert(deleteBlock.rows() > 0 && "Iceberg equality delete file should have at least one row.");
    auto numDeleteFields = deleteBlock.columns();
    assert(numDeleteFields > 0 && "Iceberg equality delete file should have at least one field.");

    assert(deleteBlock.columns() == data_file_schema_for_delete_.columns());
    Names names{data_file_schema_for_delete_.getNames()};

    while (deleteBlock.rows() > 0)
    {
        if (deleteBlock.columns() == 1)
            expressionInputs.notIn(std::move(deleteBlock), names[0]);
        else
            expressionInputs.notEquals(std::move(deleteBlock), names);

        deleteBlock = reader.next();
    }
}

ExpressionActionsPtr EqualityDeleteFileReader::createDeleteExpr(
    const ContextPtr & context,
    const Block & data_file_header,
    const google::protobuf::RepeatedPtrField<SubstraitIcebergDeleteFile> & delete_files,
    const std::vector<int> & equality_delete_files,
    Block & reader_header)
{
    assert(!equality_delete_files.empty());

    EqualityDeleteActionBuilder expressionInputs{context, data_file_header.getNamesAndTypesList()};
    for (auto deleteIndex : equality_delete_files)
    {
        const auto & delete_file = delete_files[deleteIndex];
        assert(delete_file.filecontent() == IcebergReadOptions::EQUALITY_DELETES);
        if (delete_file.recordcount() > 0)
        {
            EqualityDeleteFileReader delete_file_reader{context, data_file_header, delete_file};
            for (const auto & col : delete_file_reader.data_file_schema_for_delete_)
                if (!reader_header.has(col.name))
                    reader_header.insert(col.cloneEmpty());
            delete_file_reader.readDeleteValues(expressionInputs);
        }
    }
    return expressionInputs.finish();
}

}

}
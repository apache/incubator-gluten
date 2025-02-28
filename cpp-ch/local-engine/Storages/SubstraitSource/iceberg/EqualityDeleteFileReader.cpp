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

#include <DataTypes/DataTypeSet.h>
#include <Formats/FormatFactory.h>
#include <Functions/FunctionFactory.h>
#include <Processors/Formats/Impl/ArrowBufferedStreams.h>
#include <Processors/Formats/Impl/ParquetBlockInputFormat.h>
#include <Storages/Parquet/ParquetMeta.h>
#include <Storages/Parquet/VectorizedParquetRecordReader.h>
#include <Storages/SubstraitSource/ReadBufferBuilder.h>
#include <substrait/algebra.pb.h>
#include <Poco/URI.h>
#include <Common/BlockTypeUtils.h>


using namespace DB;

namespace local_engine
{

SimpleParquetReader::SimpleParquetReader(const ContextPtr & context, const substraitInputFile & file_info)
{
    const Poco::URI file_uri{file_info.uri_file()};
    ReadBufferBuilderPtr read_buffer_builder = ReadBufferBuilderFactory::instance().createBuilder(file_uri.getScheme(), context);
    read_buffer_ = read_buffer_builder->build(file_info);
    FormatSettings format_settings = getFormatSettings(context);
    ParquetMetaBuilder metaBuilder{
        .case_insensitive = format_settings.parquet.case_insensitive_column_matching,
        .allow_missing_columns = false,
        .collectPageIndex = true,
        .collectSchema = true};
    metaBuilder.build(*read_buffer_);
    std::atomic<int> is_stopped{0};
    auto arrow_file = asArrowFile(*read_buffer_, format_settings, is_stopped, "Parquet", PARQUET_MAGIC_BYTES);
    provider_ = std::make_unique<ColumnIndexRowRangesProvider>(metaBuilder);
    fileHeader_ = std::move(metaBuilder.fileHeader);
    reader_ = std::make_unique<VectorizedParquetRecordReader>(fileHeader_, format_settings);
    reader_->initialize(arrow_file, *provider_);
}

SimpleParquetReader::~SimpleParquetReader() = default;

Block SimpleParquetReader::next() const
{
    Chunk chunk = reader_->nextBatch();
    return chunk.hasRows() ? fileHeader_.cloneWithColumns(chunk.detachColumns()) : fileHeader_.cloneEmpty();
}

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

void EqualityDeleteActionBuilder::notEquals(Block deleteBlock, const DB::Names & column_names)
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
    if (andArgs.size() == 0)
        return nullptr;

    actions.addOrReplaceInOutputs(lastMerge());
    return std::make_shared<ExpressionActions>(std::move(actions), ExpressionActionsSettings(context, CompileExpressions::no));
}

namespace
{
substraitInputFile fromDeleteFile(const substraitIcebergDeleteFile & deleteFile)
{
    assert(deleteFile.filecontent() == IcebergReadOptions::EQUALITY_DELETES);
    assert(deleteFile.has_parquet());
    substraitInputFile file;
    file.set_uri_file(deleteFile.filepath());
    file.set_start(0);
    file.set_length(deleteFile.filesize());
    return file;
}

}

EqualityDeleteFileReader::EqualityDeleteFileReader(
    const ContextPtr & context, const DB::Block & read_header, const substraitIcebergDeleteFile & deleteFile)
    : reader_(context, fromDeleteFile(deleteFile)), read_header_(read_header), deleteFile_(deleteFile)
{
    assert(deleteFile_.recordcount() > 0);
}

void EqualityDeleteFileReader::readDeleteValues(EqualityDeleteActionBuilder & expressionInputs) const
{
    Block deleteBlock = reader_.next();
    assert(deleteBlock.rows() > 0 && "Iceberg equality delete file should have at least one row.");
    auto numDeleteFields = deleteBlock.columns();
    assert(numDeleteFields > 0 && "Iceberg equality delete file should have at least one field.");

    assert(deleteFile_.equalityfieldids_size() == deleteBlock.columns());
    Names names;
    //TODO: deleteFile_.equalityfieldids(i) - 1 ? why
    for (int i = 0; i < deleteFile_.equalityfieldids_size(); i++)
        names.push_back(read_header_.getByPosition(deleteFile_.equalityfieldids(i) - 1).name);


    while (deleteBlock.rows() > 0)
    {
        if (deleteBlock.columns() == 1)
            expressionInputs.notIn(std::move(deleteBlock), names[0]);
        else
            expressionInputs.notEquals(std::move(deleteBlock), names);

        deleteBlock = reader_.next();
    }
}
}

}
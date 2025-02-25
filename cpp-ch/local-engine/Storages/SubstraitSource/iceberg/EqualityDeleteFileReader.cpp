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

#include <Formats/FormatFactory.h>
#include <Processors/Formats/Impl/ArrowBufferedStreams.h>
#include <Processors/Formats/Impl/ParquetBlockInputFormat.h>
#include <Storages/Parquet/ParquetMeta.h>
#include <Storages/Parquet/VectorizedParquetRecordReader.h>
#include <Storages/SubstraitSource/ReadBufferBuilder.h>
#include <substrait/algebra.pb.h>
#include <Poco/URI.h>
#include <Common/BlockTypeUtils.h>
#include "Parsers/ASTFunction.h"
#include "Parsers/ASTIdentifier.h"
#include "Parsers/ASTLiteral.h"

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

EqualityDeleteFileReader::EqualityDeleteFileReader(const ContextPtr & context, const substraitIcebergDeleteFile & deleteFile)
    : reader_(context, fromDeleteFile(deleteFile))
{
    assert(deleteFile.recordcount() > 0);
}
void EqualityDeleteFileReader::readDeleteValues(DB::ASTs & expressionInputs) const
{
    Block deleteBlock = reader_.next();
    assert(deleteBlock.rows() > 0 && "Iceberg equality delete file should have at least one row.");
    auto numDeleteFields = deleteBlock.columns();
    assert(numDeleteFields > 0 && "Iceberg equality delete file should have at least one field.");

    while (deleteBlock.rows() > 0)
    {
        auto numDeletedValues = deleteBlock.rows();

        for (int i = 0; i < numDeletedValues; i++)
        {
            ASTs arguments;
            for (int j = 0; j < numDeleteFields; j++)
            {
                auto name = std::make_shared<ASTIdentifier>(deleteBlock.getByPosition(j).name);
                auto value = std::make_shared<ASTLiteral>(deleteBlock.getByPosition(j).column->operator[](i));
                auto isNotEqualExpr = makeASTFunction("notEquals", DB::ASTs{name, value});
                arguments.emplace_back(isNotEqualExpr);
            }
            if (arguments.size() > 1)
                expressionInputs.emplace_back(makeASTFunction("or", arguments));
            else
                expressionInputs.emplace_back(arguments[0]);
        }
        deleteBlock = reader_.next();
    }
}
}

}
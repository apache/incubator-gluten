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

#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/Field.h>
#include <Interpreters/Context.h>
#include <Processors/Chunk.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/ISource.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Storages/SubstraitSource/FormatFile.h>
#include <Storages/SubstraitSource/ReadBufferBuilder.h>
#include <Storages/SubstraitSource/SubstraitFileSourceStep.h>
#include <base/types.h>

namespace local_engine
{
class FileReaderWrapper
{
public:
    explicit FileReaderWrapper(FormatFilePtr file_) : file(file_) { }
    virtual ~FileReaderWrapper() = default;
    virtual bool pull(DB::Chunk & chunk) = 0;

protected:
    FormatFilePtr file;

    static DB::ColumnPtr createConstColumn(DB::DataTypePtr type, const DB::Field & field, size_t rows);
    static DB::ColumnPtr createColumn(const String & value, DB::DataTypePtr type, size_t rows);
    static DB::Field buildFieldFromString(const String & value, DB::DataTypePtr type);
};

class NormalFileReader : public FileReaderWrapper
{
public:
    NormalFileReader(FormatFilePtr file_, DB::ContextPtr context_, const DB::Block & to_read_header_, const DB::Block & output_header_);
    ~NormalFileReader() override = default;
    bool pull(DB::Chunk & chunk) override;
    FormatFile::InputFormatPtr input_format;

private:
    DB::ContextPtr context;
    DB::Block to_read_header;
    DB::Block output_header;


    std::unique_ptr<DB::QueryPipeline> pipeline;
    std::unique_ptr<DB::PullingPipelineExecutor> reader;
};

class EmptyFileReader : public FileReaderWrapper
{
public:
    explicit EmptyFileReader(FormatFilePtr file_) : FileReaderWrapper(file_) { }
    ~EmptyFileReader() override = default;
    bool pull(DB::Chunk &) override { return false; }
};

class ConstColumnsFileReader : public FileReaderWrapper
{
public:
    ConstColumnsFileReader(
        FormatFilePtr file_, DB::ContextPtr context_, const DB::Block & header_, size_t block_size_ = DEFAULT_BLOCK_SIZE);
    ~ConstColumnsFileReader() override = default;
    bool pull(DB::Chunk & chunk) override;

private:
    DB::ContextPtr context;
    DB::Block header;
    size_t remained_rows;
    size_t block_size;
};

class SubstraitFileSource : public DB::ISource
{
public:
    SubstraitFileSource(DB::ContextPtr context_, const DB::Block & header_, const substrait::ReadRel::LocalFiles & file_infos);
    ~SubstraitFileSource() override = default;

    String getName() const override { return "SubstraitFileSource"; }

    void applyFilters(std::vector<SourceFilter> filters) const;
    std::vector<String> getPartitionKeys() const;
    DB::String getFileFormat() const;
    uint64_t arrow_to_ch_us = 0;

protected:
    DB::Chunk generate() override;

private:
    DB::ContextPtr context;
    DB::Block output_header; /// Sample header before flatten, may contains partitions keys
    DB::Block flatten_output_header; // Sample header after flatten, include partition keys
    DB::Block to_read_header; // Sample header after flatten, not include partition keys
    FormatFiles files;
    DB::NamesAndTypesList file_schema; /// The column names and types in the file

    /// The columns to skip flatten based on output_header
    /// Notice that not all tuple type columns need to be flatten.
    /// E.g. if parquet file schema is `info struct<name string, age int>`, and output_header is `info Tuple(name String, age Int32)`
    /// then there is not need to flatten `info` column, because null value of `info` column will be represented as null value of `info.name` and `info.age`, which is obviously wrong.
    std::unordered_set<size_t> columns_to_skip_flatten;
    std::vector<DB::KeyCondition> filters;

    UInt32 current_file_index = 0;
    std::unique_ptr<FileReaderWrapper> file_reader;
    ReadBufferBuilderPtr read_buffer_builder;

    bool tryPrepareReader();

    // E.g we have flatten columns correspond to header {a:int, b.x.i: int, b.x.j: string, b.y: string}
    // but we want to fold all the flatten struct columns into one struct column,
    // {a:int, b: {x: {i: int, j: string}, y: string}}
    // Notice, don't support list with named struct. ClickHouse may take advantage of this to support
    // nested table, but not the case in spark.
    static DB::Block
    foldFlattenColumns(const DB::Columns & cols, const DB::Block & header, const std::unordered_set<size_t> & columns_to_skip_flatten);

    static DB::ColumnWithTypeAndName
    foldFlattenColumn(DB::DataTypePtr col_type, const std::string & col_name, size_t & pos, const DB::Columns & cols);
};
}

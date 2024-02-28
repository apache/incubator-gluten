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
    virtual void applyKeyCondition(std::shared_ptr<const DB::KeyCondition> /*key_condition*/) { }

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

    void applyKeyCondition(std::shared_ptr<const DB::KeyCondition> key_condition) override
    {
        input_format->input->setKeyCondition(key_condition);
    }

private:
    DB::ContextPtr context;
    DB::Block to_read_header;
    DB::Block output_header;

    FormatFile::InputFormatPtr input_format;
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
        FormatFilePtr file_, DB::ContextPtr context_, const DB::Block & header_, size_t block_size_ = DB::DEFAULT_BLOCK_SIZE);
    ~ConstColumnsFileReader() override = default;
    bool pull(DB::Chunk & chunk) override;

private:
    DB::ContextPtr context;
    DB::Block header;
    size_t remained_rows;
    size_t block_size;
};

class SubstraitFileSource : public DB::SourceWithKeyCondition
{
public:
    SubstraitFileSource(DB::ContextPtr context_, const DB::Block & header_, const substrait::ReadRel::LocalFiles & file_infos);
    ~SubstraitFileSource() override = default;

    String getName() const override { return "SubstraitFileSource"; }

    void setKeyCondition(const DB::ActionsDAGPtr & filter_actions_dag, DB::ContextPtr context_) override;

protected:
    DB::Chunk generate() override;

private:
    DB::ContextPtr context;
    DB::Block output_header; /// Sample header may contains partitions keys
    DB::Block to_read_header; // Sample header not include partition keys
    FormatFiles files;

    UInt32 current_file_index = 0;
    std::unique_ptr<FileReaderWrapper> file_reader;
    ReadBufferBuilderPtr read_buffer_builder;

    bool tryPrepareReader();
};
}

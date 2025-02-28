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
#include <Core/Field.h>
#include <Processors/Chunk.h>
#include <Storages/SubstraitSource/FormatFile.h>

namespace local_engine
{
class ColumnIndexFilter;
using ColumnIndexFilterPtr = std::shared_ptr<ColumnIndexFilter>;

class FormatFile;
using FormatFilePtr = std::shared_ptr<FormatFile>;

class BaseReader
{
public:
    explicit BaseReader(const FormatFilePtr & file_, const DB::Block & to_read_header_, const DB::Block & header_)
        : file(file_), readHeader(to_read_header_), outputHeader(header_)
    {
    }
    virtual ~BaseReader() = default;

    void cancel()
    {
        bool already_cancelled = is_cancelled.exchange(true, std::memory_order_acq_rel);
        if (!already_cancelled)
            onCancel();
    }

    virtual bool pull(DB::Chunk & chunk) = 0;
    bool isCancelled() const { return is_cancelled.load(std::memory_order_acquire); }

    const DB::Block & getHeader() const { return outputHeader; }

protected:
    virtual void onCancel() { };

    DB::Columns addVirtualColumn(DB::Chunk dataChunk, size_t rowNum = 0) const;

    FormatFilePtr file;
    DB::Block readHeader;
    DB::Block outputHeader;

    std::atomic<bool> is_cancelled{false};


    static DB::ColumnPtr createConstColumn(DB::DataTypePtr type, const DB::Field & field, size_t rows);
    static DB::ColumnPtr createPartitionColumn(const String & value, const DB::DataTypePtr & type, size_t rows);
    static DB::Field buildFieldFromString(const String & value, DB::DataTypePtr type);

public:
    /// When run query "select count(*) from t", there is no any column to be read.
    /// The only necessary information is the number of rows.
    /// To handle these cases, we build blocks with a const virtual column to indicate how many rows are in it.
    static DB::Block buildRowCountHeader(const DB::Block & header);

    /// Factory
    static std::unique_ptr<BaseReader> create(
        const FormatFilePtr & current_file,
        const DB::Block & readHeader,
        const DB::Block & outputHeader,
        const std::shared_ptr<const DB::KeyCondition> & key_condition,
        const ColumnIndexFilterPtr & column_index_filter);
};

class NormalFileReader : public BaseReader
{
public:
    NormalFileReader(
        const FormatFilePtr & file_,
        const DB::Block & to_read_header_,
        const DB::Block & output_header_,
        const FormatFile::InputFormatPtr & input_format_);
    ~NormalFileReader() override = default;

    bool pull(DB::Chunk & chunk) override;

protected:
    virtual DB::Chunk doPull() { return input_format->generate(); }

private:
    void onCancel() override { input_format->cancel(); }
    FormatFile::InputFormatPtr input_format;
};

class ConstColumnsFileReader : public BaseReader
{
public:
    ConstColumnsFileReader(const FormatFilePtr & file_, const DB::Block & header_, size_t blockSize = DB::DEFAULT_BLOCK_SIZE);
    ~ConstColumnsFileReader() override = default;

    bool pull(DB::Chunk & chunk) override;

private:
    size_t remained_rows;
    const size_t block_size;
};
}
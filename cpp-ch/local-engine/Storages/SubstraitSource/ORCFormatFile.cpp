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
#include "ORCFormatFile.h"

#if USE_ORC
#    include <memory>
#    include <numeric>
#    include <Formats/FormatFactory.h>
#    include <IO/SeekableReadBuffer.h>
#    include <Processors/Formats/Impl/ArrowBufferedStreams.h>
#    include <Processors/Formats/Impl/NativeORCBlockInputFormat.h>
#    include <Storages/SubstraitSource/OrcUtil.h>

#    if USE_LOCAL_FORMATS
#        include <DataTypes/NestedUtils.h>
#        include <Formats/FormatSettings.h>
#        include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_READ_ALL_DATA;
}
}
#    endif

namespace local_engine
{
#    if USE_LOCAL_FORMATS
ORCBlockInputFormat::ORCBlockInputFormat(
    DB::ReadBuffer & in_, DB::Block header_, const DB::FormatSettings & format_settings_, const std::vector<StripeInformation> & stripes_)
    : IInputFormat(std::move(header_), in_), format_settings(format_settings_), stripes(stripes_)
{
}


void ORCBlockInputFormat::resetParser()
{
    IInputFormat::resetParser();

    file_reader.reset();
    include_indices.clear();
    include_column_names.clear();
    block_missing_values.clear();
    current_stripe = 0;
}


DB::Chunk ORCBlockInputFormat::generate()
{
    DB::Chunk res;
    block_missing_values.clear();

    if (!file_reader)
        prepareReader();

    if (is_stopped)
        return {};

    std::shared_ptr<arrow::RecordBatchReader> batch_reader;
    batch_reader = fetchNextStripe();
    if (!batch_reader)
    {
        return res;
    }

    std::shared_ptr<arrow::Table> table;
    arrow::Status table_status = batch_reader->ReadAll(&table);
    if (!table_status.ok())
    {
        throw DB::ParsingException(
            DB::ErrorCodes::CANNOT_READ_ALL_DATA, "Error while reading batch of ORC data: {}", table_status.ToString());
    }

    if (!table || !table->num_rows())
    {
        return res;
    }

    if (format_settings.use_lowercase_column_name)
        table = *table->RenameColumns(include_column_names);

    arrow_column_to_ch_column->arrowTableToCHChunk(res, table);
    /// If defaults_for_omitted_fields is true, calculate the default values from default expression for omitted fields.
    /// Otherwise fill the missing columns with zero values of its type.
    if (format_settings.defaults_for_omitted_fields)
        for (size_t row_idx = 0; row_idx < res.getNumRows(); ++row_idx)
            for (const auto & column_idx : missing_columns)
                block_missing_values.setBit(column_idx, row_idx);
    return res;
}


void ORCBlockInputFormat::prepareReader()
{
    std::shared_ptr<arrow::Schema> schema;
    OrcUtil::getFileReaderAndSchema(*in, file_reader, schema, format_settings, is_stopped);
    if (is_stopped)
        return;

    arrow_column_to_ch_column = std::make_unique<DB::OptimizedArrowColumnToCHColumn>(
        getPort().getHeader(), "ORC", format_settings.orc.import_nested, format_settings.orc.allow_missing_columns);
    missing_columns = arrow_column_to_ch_column->getMissingColumns(*schema);

    std::unordered_set<String> nested_table_names;
    if (format_settings.orc.import_nested)
        nested_table_names = DB::Nested::getAllTableNames(getPort().getHeader());


    /// In ReadStripe column indices should be started from 1,
    /// because 0 indicates to select all columns.
    int index = 1;
    for (int i = 0; i < schema->num_fields(); ++i)
    {
        /// LIST type require 2 indices, STRUCT - the number of elements + 1,
        /// so we should recursively count the number of indices we need for this type.
        int indexes_count = OrcUtil::countIndicesForType(schema->field(i)->type());
        const auto & name = schema->field(i)->name();
        if (getPort().getHeader().has(name) || nested_table_names.contains(name))
        {
            for (int j = 0; j != indexes_count; ++j)
            {
                include_indices.push_back(index + j);
                include_column_names.push_back(name);
            }
        }
        index += indexes_count;
    }
}

std::shared_ptr<arrow::RecordBatchReader> ORCBlockInputFormat::stepOneStripe()
{
    auto result = file_reader->NextStripeReader(format_settings.orc.row_batch_size, include_indices);
    current_stripe += 1;
    if (!result.ok())
    {
        throw DB::ParsingException(DB::ErrorCodes::CANNOT_READ_ALL_DATA, "Failed to create batch reader: {}", result.status().ToString());
    }
    std::shared_ptr<arrow::RecordBatchReader> batch_reader;
    batch_reader = std::move(result).ValueOrDie();
    return batch_reader;
}

std::shared_ptr<arrow::RecordBatchReader> ORCBlockInputFormat::fetchNextStripe()
{
    if (current_stripe >= stripes.size())
        return nullptr;
    auto & strip = stripes[current_stripe];
    file_reader->Seek(strip.start_row);
    return stepOneStripe();
}
#    endif

ORCFormatFile::ORCFormatFile(
    DB::ContextPtr context_, const substrait::ReadRel::LocalFiles::FileOrFiles & file_info_, ReadBufferBuilderPtr read_buffer_builder_)
    : FormatFile(context_, file_info_, read_buffer_builder_)
{
}

FormatFile::InputFormatPtr ORCFormatFile::createInputFormat(const DB::Block & header)
{
    auto file_format = std::make_shared<FormatFile::InputFormat>();
    file_format->read_buffer = read_buffer_builder->build(file_info);

    std::vector<StripeInformation> stripes;
    [[maybe_unused]] UInt64 total_stripes = 0;
    if (auto * seekable_in = dynamic_cast<DB::SeekableReadBuffer *>(file_format->read_buffer.get()))
    {
        stripes = collectRequiredStripes(seekable_in, total_stripes);
        seekable_in->seek(0, SEEK_SET);
    }
    else
        stripes = collectRequiredStripes(total_stripes);

    auto format_settings = DB::getFormatSettings(context);

#    if USE_LOCAL_FORMATS
    format_settings.orc.import_nested = true;
    auto input_format = std::make_shared<local_engine::ORCBlockInputFormat>(*file_format->read_buffer, header, format_settings, stripes);
#    else
    std::vector<int> total_stripe_indices(total_stripes);
    std::iota(total_stripe_indices.begin(), total_stripe_indices.end(), 0);

    std::vector<UInt64> required_stripe_indices(stripes.size());
    for (size_t i = 0; i < stripes.size(); ++i)
        required_stripe_indices[i] = stripes[i].index;

    std::vector<int> skip_stripe_indices;
    std::set_difference(
        total_stripe_indices.begin(),
        total_stripe_indices.end(),
        required_stripe_indices.begin(),
        required_stripe_indices.end(),
        std::back_inserter(skip_stripe_indices));

    format_settings.orc.skip_stripes = std::unordered_set<int>(skip_stripe_indices.begin(), skip_stripe_indices.end());
    auto input_format = std::make_shared<DB::NativeORCBlockInputFormat>(*file_format->read_buffer, header, format_settings);
#    endif
    file_format->input = input_format;
    return file_format;
}

std::optional<size_t> ORCFormatFile::getTotalRows()
{
    {
        std::lock_guard lock(mutex);
        if (total_rows)
            return total_rows;
    }

    UInt64 _;
    auto required_stripes = collectRequiredStripes(_);

    {
        std::lock_guard lock(mutex);
        if (total_rows)
            return total_rows;

        size_t num_rows = 0;
        for (const auto stipe_info : required_stripes)
            num_rows += stipe_info.num_rows;

        total_rows = num_rows;
        return total_rows;
    }
}

std::vector<StripeInformation> ORCFormatFile::collectRequiredStripes(UInt64 & total_stripes)
{
    auto in = read_buffer_builder->build(file_info);
    return collectRequiredStripes(in.get(), total_stripes);
}

std::vector<StripeInformation> ORCFormatFile::collectRequiredStripes(DB::ReadBuffer * read_buffer, UInt64 & total_stripes)
{
    DB::FormatSettings format_settings{
        .seekable_read = true,
    };
    std::atomic<int> is_stopped{0};
    auto arrow_file = DB::asArrowFile(*read_buffer, format_settings, is_stopped, "ORC", ORC_MAGIC_BYTES);
    auto orc_reader = OrcUtil::createOrcReader(arrow_file);
    total_stripes = orc_reader->getNumberOfStripes();

    size_t total_num_rows = 0;
    std::vector<StripeInformation> stripes;
    stripes.reserve(total_stripes);
    for (size_t i = 0; i < total_stripes; ++i)
    {
        auto stripe_metadata = orc_reader->getStripe(i);

        auto offset = stripe_metadata->getOffset() + stripe_metadata->getLength() / 2;
        if (file_info.start() <= offset && offset < file_info.start() + file_info.length())
        {
            StripeInformation stripe_info;
            stripe_info.index = i;
            stripe_info.offset = stripe_metadata->getLength();
            stripe_info.length = stripe_metadata->getLength();
            stripe_info.num_rows = stripe_metadata->getNumberOfRows();
            stripe_info.start_row = total_num_rows;
            stripes.emplace_back(stripe_info);
        }

        total_num_rows += stripe_metadata->getNumberOfRows();
    }
    return stripes;
}
}

#endif

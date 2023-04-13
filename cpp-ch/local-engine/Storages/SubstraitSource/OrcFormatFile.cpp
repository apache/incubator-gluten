#include "OrcFormatFile.h"
#include <memory>
#include <strings.h>
#include <DataTypes/NestedUtils.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatSettings.h>
#include <IO/SeekableReadBuffer.h>
#include <Processors/Formats/Impl/ArrowBufferedStreams.h>
#include <Storages/SubstraitSource/OrcUtil.h>
#include <arrow/adapters/orc/adapter.h>
#include <arrow/type.h>
#include <arrow/util/key_value_metadata.h>
#include <Common/logger_useful.h>
#include <bits/types/FILE.h>
#include <orc/Reader.hh>
#include <Poco/Logger.h>
#include <Common/Exception.h>
#include <Common/Stopwatch.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_READ_ALL_DATA;
}
}
namespace local_engine
{

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

OrcFormatFile::OrcFormatFile(
    DB::ContextPtr context_, const substrait::ReadRel::LocalFiles::FileOrFiles & file_info_, ReadBufferBuilderPtr read_buffer_builder_)
    : FormatFile(context_, file_info_, read_buffer_builder_)
{
}

FormatFile::InputFormatPtr OrcFormatFile::createInputFormat(const DB::Block & header)
{
    auto read_buffer = read_buffer_builder->build(file_info);
    auto format_settings = DB::getFormatSettings(context);
    format_settings.orc.import_nested = true;
    auto file_format = std::make_shared<FormatFile::InputFormat>();
    file_format->read_buffer = std::move(read_buffer);
    std::vector<StripeInformation> stripes;
    if (auto * seekable_in = dynamic_cast<DB::SeekableReadBuffer *>(file_format->read_buffer.get()))
    {
        stripes = collectRequiredStripes(seekable_in);
        seekable_in->seek(0, SEEK_SET);
    }
    else
    {
        stripes = collectRequiredStripes();
    }
    auto input_format = std::make_shared<local_engine::ORCBlockInputFormat>(*file_format->read_buffer, header, format_settings, stripes);
    file_format->input = input_format;
    return file_format;
}

std::optional<size_t> OrcFormatFile::getTotalRows()
{
    {
        std::lock_guard lock(mutex);
        if (total_rows)
            return total_rows;
    }
    auto required_stripes = collectRequiredStripes();
    {
        std::lock_guard lock(mutex);
        if (total_rows)
            return total_rows;
        size_t num_rows = 0;
        for (const auto stipe_info : required_stripes)
        {
            num_rows += stipe_info.num_rows;
        }
        total_rows = num_rows;
        return total_rows;
    }
}

std::vector<StripeInformation> OrcFormatFile::collectRequiredStripes()
{
    auto in = read_buffer_builder->build(file_info);
    return collectRequiredStripes(in.get());
}

std::vector<StripeInformation> OrcFormatFile::collectRequiredStripes(DB::ReadBuffer* read_buffer)
{
    std::vector<StripeInformation> stripes;
    DB::FormatSettings format_settings;
    format_settings.seekable_read = true;
    std::atomic<int> is_stopped{0};
    auto arrow_file = DB::asArrowFile(*read_buffer, format_settings, is_stopped, "ORC", ORC_MAGIC_BYTES);
    auto orc_reader = OrcUtil::createOrcReader(arrow_file);
    auto num_stripes = orc_reader->getNumberOfStripes();

    size_t total_num_rows = 0;
    for (size_t i = 0; i < num_stripes; ++i)
    {
        auto stripe_metadata = orc_reader->getStripe(i);
        auto offset = stripe_metadata->getOffset();
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

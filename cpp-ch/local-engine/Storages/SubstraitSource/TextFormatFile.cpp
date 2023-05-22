#include "TextFormatFile.h"

#include <memory>
#include <string>
#include <utility>

#include <Columns/ColumnNullable.h>
#include <Core/SettingsEnums.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/Serializations/SerializationDate32.h>
#include <DataTypes/Serializations/SerializationNullable.h>
#include <Formats/FormatSettings.h>
#include <IO/PeekableReadBuffer.h>
#include <IO/SeekableReadBuffer.h>
#include <Processors/Formats/IRowInputFormat.h>
#include <Storages/HDFS/ReadBufferFromHDFS.h>
#include <Storages/Serializations/ExcelSerialization.h>


namespace local_engine
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

TextFormatFile::TextFormatFile(DB::ContextPtr context_, const substrait::ReadRel::LocalFiles::FileOrFiles & file_info_, ReadBufferBuilderPtr read_buffer_builder_)
    :FormatFile(context_, file_info_, read_buffer_builder_) {}

FormatFile::InputFormatPtr TextFormatFile::createInputFormat(const DB::Block & header)
{
    auto res = std::make_shared<FormatFile::InputFormat>();
    res->read_buffer = std::move(read_buffer_builder->build(file_info, true));

    DB::FormatSettings format_settings = createFormatSettings();
    size_t max_block_size = file_info.text().max_block_size();
    DB::RowInputFormatParams in_params = {max_block_size};

    std::shared_ptr<DB::PeekableReadBuffer> buffer = std::make_unique<DB::PeekableReadBuffer>(*(res->read_buffer));
    std::vector<std::string> column_names;
    column_names.reserve(file_info.text().schema().names_size());
    for (const auto & item : file_info.text().schema().names()) {
        column_names.push_back(item);
    }

    std::shared_ptr<local_engine::TextRowInputFormat> txt_input_format =
        std::make_shared<local_engine::TextRowInputFormat>(header, buffer, in_params, format_settings, column_names);
    res->input = txt_input_format;
    return res;
}

DB::FormatSettings TextFormatFile::createFormatSettings()
{
    DB::FormatSettings format_settings = DB::getFormatSettings(context);
    format_settings.with_names_use_header = true;
    format_settings.with_types_use_header = false;
    format_settings.skip_unknown_fields = true;
    std::string delimiter = file_info.text().field_delimiter();
    format_settings.csv.delimiter = *delimiter.data();
    format_settings.csv.skip_first_lines = file_info.text().header();
    format_settings.csv.null_representation = file_info.text().null_value();

    char quote = *file_info.text().quote().data();
    if (quote == '\'')
    {
        format_settings.csv.allow_single_quotes = true;
        format_settings.csv.allow_double_quotes = false;
    }
    else if (quote == '"')
    {
        format_settings.csv.allow_single_quotes = false;
        format_settings.csv.allow_double_quotes = true;
    }
    else
    {
        format_settings.csv.allow_single_quotes = false;
        format_settings.csv.allow_double_quotes = false;
    }

    return format_settings;
}

TextRowInputFormat::TextRowInputFormat(
    const DB::Block & header_,
    std::shared_ptr<DB::PeekableReadBuffer> & buf_,
    const DB::RowInputFormatParams & params_,
    const DB::FormatSettings & format_settings_,
    std::vector<std::string> input_schema_)
    : DB::CSVRowInputFormat(
        header_, buf_, params_, true, false, format_settings_, std::make_unique<TextFormatReader>(*buf_, input_schema_, format_settings_))
    , input_schema(input_schema_)
{
    DB::Serializations gluten_serializations;
    for (const auto & item : data_types)
    {
        const auto & nest_type = item->isNullable() ? *static_cast<const DataTypeNullable &>(*item).getNestedType() : *item;
        if (item->isNullable())
        {
            gluten_serializations.insert(
                gluten_serializations.end(),
                std::make_shared<SerializationNullable>(std::make_shared<ExcelSerialization>(nest_type.getDefaultSerialization())));
        }
        else
        {
            gluten_serializations.insert(
                gluten_serializations.end(), std::make_shared<ExcelSerialization>(nest_type.getDefaultSerialization()));
        }
    }

    serializations = gluten_serializations;
}

void TextRowInputFormat::readPrefix()
{
    CSVRowInputFormat::readPrefix();
    std::vector<std::string> column_names = column_mapping->names_of_columns;

    std::map<std::string, size_t> column_names_set;
    for (size_t i = 0; i < getPort().getHeader().getNames().size(); ++i)
    {
        column_names_set[getPort().getHeader().getNames().at(i)] = i;
    }

    for (size_t j = 0; j < input_schema.size(); ++j)
    {
        auto name = input_schema.at(j);
        if (column_names_set.contains(name))
        {
            column_mapping->column_indexes_for_input_fields[j] = std::optional<size_t>(column_names_set.at(name));
        }
        else
        {
            column_mapping->column_indexes_for_input_fields[j] = std::optional<size_t>();
        }
    }
}

TextFormatReader::TextFormatReader(
    DB::PeekableReadBuffer & buf_, std::vector<std::string> input_schema_, const DB::FormatSettings & format_settings_)
    : DB::CSVFormatReader(buf_, format_settings_), input_schema(input_schema_)
{
}

std::vector<String> TextFormatReader::readNames()
{
    return input_schema;
}

}

#include "ExcelTextFormatFile.h"


#include <memory>
#include <string>
#include <utility>

#include <DataTypes/DataTypeNullable.h>
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
    extern const int INCORRECT_DATA;
}


FormatFile::InputFormatPtr ExcelTextFormatFile::createInputFormat(const DB::Block & header)
{
    auto res = std::make_shared<FormatFile::InputFormat>();
    res->read_buffer = std::move(read_buffer_builder->build(file_info, true));

    DB::FormatSettings format_settings = createFormatSettings();
    size_t max_block_size = file_info.text().max_block_size();
    DB::RowInputFormatParams in_params = {max_block_size};

    std::shared_ptr<DB::PeekableReadBuffer> buffer = std::make_unique<DB::PeekableReadBuffer>(*(res->read_buffer));
    DB::Names column_names;
    column_names.reserve(file_info.text().schema().names_size());
    for (const auto & item : file_info.text().schema().names())
    {
        column_names.push_back(item);
    }

    std::shared_ptr<local_engine::ExcelRowInputFormat> txt_input_format = std::make_shared<local_engine::ExcelRowInputFormat>(
        header, buffer, in_params, format_settings, column_names, file_info.text().escape());
    res->input = txt_input_format;
    return res;
}

DB::FormatSettings ExcelTextFormatFile::createFormatSettings()
{
    DB::FormatSettings format_settings = DB::getFormatSettings(context);
    format_settings.with_names_use_header = true;
    format_settings.with_types_use_header = false;
    format_settings.skip_unknown_fields = true;
    std::string delimiter = file_info.text().field_delimiter();
    format_settings.csv.delimiter = *delimiter.data();
    format_settings.csv.skip_first_lines = file_info.text().header();
    format_settings.csv.null_representation = file_info.text().null_value();

    if (format_settings.csv.null_representation.empty())
        format_settings.csv.empty_as_default = true;
    else
        format_settings.csv.empty_as_default = false;

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
        format_settings.csv.allow_double_quotes = true;
    }

    return format_settings;
}


ExcelRowInputFormat::ExcelRowInputFormat(
    const DB::Block & header_,
    std::shared_ptr<DB::PeekableReadBuffer> & buf_,
    const DB::RowInputFormatParams & params_,
    const DB::FormatSettings & format_settings_,
    DB::Names & input_field_names_,
    String escape_)
    : CSVRowInputFormat(
        header_,
        buf_,
        params_,
        true,
        false,
        format_settings_,
        std::make_unique<ExcelTextFormatReader>(*buf_, input_field_names_, format_settings_))
    , escape(escape_)
{
    DB::Serializations gluten_serializations;
    for (const auto & item : data_types)
    {
        const auto & nest_type = item->isNullable() ? *static_cast<const DataTypeNullable &>(*item).getNestedType() : *item;
        if (item->isNullable())
        {
            gluten_serializations.insert(
                gluten_serializations.end(),
                std::make_shared<SerializationNullable>(std::make_shared<ExcelSerialization>(nest_type.getDefaultSerialization(), escape)));
        }
        else
        {
            gluten_serializations.insert(
                gluten_serializations.end(), std::make_shared<ExcelSerialization>(nest_type.getDefaultSerialization(), escape));
        }
    }

    serializations = gluten_serializations;
}


ExcelTextFormatReader::ExcelTextFormatReader(
    DB::PeekableReadBuffer & buf_, DB::Names & input_field_names_, const DB::FormatSettings & format_settings_)
    : CSVFormatReader(buf_, format_settings_), input_field_names(input_field_names_)
{
}


std::vector<String> ExcelTextFormatReader::readNames()
{
    return input_field_names;
}

std::vector<String> ExcelTextFormatReader::readTypes()
{
    throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "ExcelTextRowInputFormat::readTypes is not implemented");
}

bool ExcelTextFormatReader::readField(
    DB::IColumn & column,
    const DB::DataTypePtr & type,
    const DB::SerializationPtr & serialization,
    bool is_last_file_column,
    const String & column_name)
{
    preSkipNullValue();
    return CSVFormatReader::readField(column, type, serialization, is_last_file_column, column_name);
}

void ExcelTextFormatReader::preSkipNullValue()
{
    // null_representation is empty and value is "" or '' in spark return null
    if (format_settings.csv.null_representation.empty()
        && (format_settings.csv.allow_single_quotes || format_settings.csv.allow_double_quotes)
        && (*buf->position() == '\'' || *buf->position() == '\"'))
    {
        PeekableReadBufferCheckpoint checkpoint{*buf, false};
        char maybe_quote = *buf->position();
        ++buf->position();

        if (!buf->eof() && *buf->position() == maybe_quote)
            ++buf->position();
        else
        {
            buf->rollbackToCheckpoint();
            return;
        }

        bool at_delimiter = !buf->eof() && *buf->position() == format_settings.csv.delimiter;
        bool at_line_end = buf->eof() || *buf->position() == '\n' || *buf->position() == '\r';

        if (!at_delimiter && !at_line_end)
            buf->rollbackToCheckpoint();
    }
}

void ExcelTextFormatReader::skipRowEndDelimiter()
{
    skipWhitespacesAndTabs(*buf);

    if (buf->eof())
        return;

    /// we support the extra delimiter at the end of the line
    if (*buf->position() == format_settings.csv.delimiter)
        ++buf->position();

    skipWhitespacesAndTabs(*buf);
    if (buf->eof())
        return;

    if (*buf->position() != '\r' && *buf->position() != '\n')
    {
        // remove unused chars
        skipField();
        skipRowEndDelimiter();
    }
    else
    {
        skipEndOfLine(*buf);
    }
}

void ExcelTextFormatReader::skipEndOfLine(DB::ReadBuffer & in)
{
    /// \n (Unix) or \r\n (DOS/Windows) or \n\r (Mac OS Classic)

    if (*in.position() == '\n')
    {
        ++in.position();
        if (!in.eof() && *in.position() == '\r')
            ++in.position();
    }
    else if (*in.position() == '\r')
    {
        ++in.position();
        if (!in.eof() && *in.position() == '\n')
            ++in.position();
        /// removed \r check
    }
    else if (!in.eof())
        throw DB::Exception(DB::ErrorCodes::INCORRECT_DATA, "Expected end of line");
}


inline void ExcelTextFormatReader::skipWhitespacesAndTabs(DB::ReadBuffer & in)
{
    /// Skip `whitespace` symbols allowed in CSV.
    while (!in.eof() && (*in.position() == ' ' || *in.position() == '\t'))
        ++in.position();
}


}

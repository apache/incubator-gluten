#include "ExcelTextFormatFile.h"


#include <memory>
#include <string>
#include <utility>

#include <DataTypes/DataTypeDecimalBase.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/Serializations/SerializationNullable.h>
#include <Formats/FormatSettings.h>
#include <IO/PeekableReadBuffer.h>
#include <IO/SeekableReadBuffer.h>
#include <Processors/Formats/IRowInputFormat.h>
#include <Storages/HDFS/ReadBufferFromHDFS.h>
#include <Storages/Serializations/ExcelDecimalSerialization.h>
#include <Storages/Serializations/ExcelSerialization.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int INCORRECT_DATA;
}
}

namespace local_engine
{

FormatFile::InputFormatPtr ExcelTextFormatFile::createInputFormat(const DB::Block & header)
{
    auto res = std::make_shared<FormatFile::InputFormat>();
    res->read_buffer = read_buffer_builder->build(file_info, true);

    DB::FormatSettings format_settings = createFormatSettings();
    size_t max_block_size = file_info.text().max_block_size();
    DB::RowInputFormatParams params = {.max_block_size = max_block_size};

    std::shared_ptr<DB::PeekableReadBuffer> buffer = std::make_unique<DB::PeekableReadBuffer>(*(res->read_buffer));
    DB::Names column_names;
    column_names.reserve(file_info.text().schema().names_size());
    for (const auto & item : file_info.text().schema().names())
    {
        column_names.push_back(item);
    }

    std::shared_ptr<local_engine::ExcelRowInputFormat> txt_input_format = std::make_shared<local_engine::ExcelRowInputFormat>(
        header, buffer, params, format_settings, column_names, file_info.text().escape());
    res->input = txt_input_format;
    return res;
}

DB::FormatSettings ExcelTextFormatFile::createFormatSettings()
{
    DB::FormatSettings format_settings = DB::getFormatSettings(context);
    format_settings.csv.trim_whitespaces = false;
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
        const DataTypePtr nest_type = item->isNullable() ? static_cast<const DataTypeNullable &>(*item).getNestedType() : item;
        SerializationPtr nest_serialization;
        WhichDataType which(nest_type->getTypeId());
        if (which.isDecimal32())
        {
            const auto & decimal_type = static_cast<const DataTypeDecimalBase<Decimal32> &>(*nest_type);
            nest_serialization = std::make_shared<ExcelDecimalSerialization<Decimal32>>(
                nest_type->getDefaultSerialization(), decimal_type.getPrecision(), decimal_type.getScale());
        }
        else if (which.isDecimal64())
        {
            const auto & decimal_type = static_cast<const DataTypeDecimalBase<Decimal64> &>(*nest_type);
            nest_serialization = std::make_shared<ExcelDecimalSerialization<Decimal64>>(
                nest_type->getDefaultSerialization(), decimal_type.getPrecision(), decimal_type.getScale());
        }
        else if (which.isDecimal128())
        {
            const auto & decimal_type = static_cast<const DataTypeDecimalBase<Decimal128> &>(*nest_type);
            nest_serialization = std::make_shared<ExcelDecimalSerialization<Decimal128>>(
                nest_type->getDefaultSerialization(), decimal_type.getPrecision(), decimal_type.getScale());
        }
        else if (which.isDecimal256())
        {
            const auto & decimal_type = static_cast<const DataTypeDecimalBase<Decimal256> &>(*nest_type);
            nest_serialization = std::make_shared<ExcelDecimalSerialization<Decimal256>>(
                nest_type->getDefaultSerialization(), decimal_type.getPrecision(), decimal_type.getScale());
        }
        else
            nest_serialization = std::make_shared<ExcelSerialization>(nest_type->getDefaultSerialization(), escape);


        if (item->isNullable())
            gluten_serializations.insert(gluten_serializations.end(), std::make_shared<SerializationNullable>(nest_serialization));
        else
            gluten_serializations.insert(gluten_serializations.end(), nest_serialization);
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
    const String & )
{
    preSkipNullValue();
    PeekableReadBufferCheckpoint checkpoint{*buf, false};
    size_t column_size = column.size();
    try
    {
        if (format_settings.csv.trim_whitespaces || isFloat(removeNullable(type))) [[unlikely]]
            skipWhitespacesAndTabs(*buf);

        const bool at_delimiter = !buf->eof() && *buf->position() == format_settings.csv.delimiter;
        const bool at_last_column_line_end = is_last_file_column && (buf->eof() || *buf->position() == '\n' || *buf->position() == '\r');

        /// Note: Tuples are serialized in CSV as separate columns, but with empty_as_default or null_as_default
        /// only one empty or NULL column will be expected
        if (format_settings.csv.empty_as_default && (at_delimiter || at_last_column_line_end))
        {
            /// Treat empty unquoted column value as default value, if
            /// specified in the settings. Tuple columns might seem
            /// problematic, because they are never quoted but still contain
            /// commas, which might be also used as delimiters. However,
            /// they do not contain empty unquoted fields, so this check
            /// works for tuples as well.
            column.insertDefault();
            return false;
        }

        if (format_settings.null_as_default && !isNullableOrLowCardinalityNullable(type))
        {
            /// If value is null but type is not nullable then use default value instead.
            return SerializationNullable::deserializeTextCSVImpl(column, *buf, format_settings, serialization);
        }

        /// Read the column normally.
        serialization->deserializeTextCSV(column, *buf, format_settings);
        return true;
    }
    catch (Exception & e)
    {
        /// Logic for possible skipping of errors.
        if (!isParseError(e.code()))
            throw;

        buf->rollbackToCheckpoint();
        skipField();

        if (column_size == column.size())
            column.insertDefault();
        return false;
    }
}

void ExcelTextFormatReader::preSkipNullValue()
{
    /// null_representation is empty and value is "" or '' in spark return null
    if (format_settings.csv.null_representation.empty()
        && ((format_settings.csv.allow_single_quotes && *buf->position() == '\'')
            || (format_settings.csv.allow_double_quotes && *buf->position() == '\"')))
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
        skipEndOfLine(*buf);
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

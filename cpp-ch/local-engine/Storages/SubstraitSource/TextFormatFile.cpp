#include "TextFormatFile.h"

#include <memory>
#include <string>
#include <utility>

#include <Core/Defines.h>
#include <Formats/FormatSettings.h>
#include <IO/SeekableReadBuffer.h>
#include <IO/PeekableReadBuffer.h>
#include <Storages/HDFS/ReadBufferFromHDFS.h>
#include <Processors/Formats/IRowInputFormat.h>

namespace local_engine
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

static DB::FormatSettings updateFormatSettings(const DB::FormatSettings & settings, const DB::Block & header)
{
    DB::FormatSettings updated = settings;
    updated.skip_unknown_fields = true;
    updated.with_names_use_header = true;
    updated.date_time_input_format = DB::FormatSettings::DateTimeInputFormat::BestEffort;
    updated.csv.delimiter = updated.hive_text.fields_delimiter;
    if (settings.hive_text.input_field_names.empty())
        updated.hive_text.input_field_names = header.getNames();
    return updated;
}

TextFormatFile::TextFormatFile(DB::ContextPtr context_, const substrait::ReadRel::LocalFiles::FileOrFiles & file_info_, ReadBufferBuilderPtr read_buffer_builder_)
    :FormatFile(context_, file_info_, read_buffer_builder_) {}

FormatFile::InputFormatPtr TextFormatFile::createInputFormat(const DB::Block & header)
{
    auto res = std::make_shared<FormatFile::InputFormat>();
    res->read_buffer = std::move(read_buffer_builder->build(file_info, true));
    DB::FormatSettings format_settings = DB::getFormatSettings(context);
    format_settings.with_names_use_header = true;
    format_settings.skip_unknown_fields = true;
    std::string text_field_delimiter = file_info.text().field_delimiter();
    size_t max_block_size = file_info.text().max_block_size();
    format_settings.hive_text.fields_delimiter = *text_field_delimiter.data();
    DB::RowInputFormatParams in_params = {max_block_size};
    std::shared_ptr<local_engine::TextRowInputFormat> txt_input_format = 
        std::make_shared<local_engine::TextRowInputFormat>(header, *(res->read_buffer), in_params, format_settings, file_info.text().schema());
    res->input = txt_input_format;
    return res;
}

TextRowInputFormat::TextRowInputFormat(
    const DB::Block & header_, 
    DB::ReadBuffer & in_, 
    const DB::RowInputFormatParams & params_, 
    const DB::FormatSettings & format_settings_,
    const substrait::NamedStruct & input_schema_)
    : TextRowInputFormat(header_, std::make_unique<DB::PeekableReadBuffer>(in_), params_, updateFormatSettings(format_settings_, header_))
{
    input_schema = input_schema_;
}

TextRowInputFormat::TextRowInputFormat(
    const DB::Block & header_, std::shared_ptr<DB::PeekableReadBuffer> buf_, const DB::RowInputFormatParams & params_, const DB::FormatSettings & format_settings_)
    : DB::CSVRowInputFormat(
        header_, buf_, params_, true, false, format_settings_, std::make_unique<TextFormatReader>(*buf_, format_settings_))
{
}

void TextRowInputFormat::readPrefix()
{
    CSVRowInputFormat::readPrefix();
    std::vector<std::string> column_names = column_mapping->names_of_columns;
    for (size_t i = 0; i < column_names.size(); ++i)
    {
        for (int j = 0; j < input_schema.names_size(); ++j) 
        {
            const char * file_field_name = input_schema.names(j).data();
            if (strcasecmp(file_field_name, column_names[i].data()) == 0) 
            {
                auto column_index = column_mapping->column_indexes_for_input_fields[i];
                if (column_index && static_cast<int>(*column_index) != j)
                {
                    column_mapping->column_indexes_for_input_fields[j] = std::optional<size_t>(i);
                    column_mapping->column_indexes_for_input_fields[i] = std::optional<size_t>();
                }
                break;
            }
        }
    }
}

TextFormatReader::TextFormatReader(DB::PeekableReadBuffer & buf_, const DB::FormatSettings & format_settings_)
    :DB::CSVFormatReader(buf_, format_settings_), input_field_names(format_settings_.hive_text.input_field_names)
{
}

std::vector<String> TextFormatReader::readNames()
{
    DB::PeekableReadBufferCheckpoint checkpoint{*buf, true};
    auto values = readHeaderRow();
    input_field_names.resize(values.size());
    return input_field_names;
}

}

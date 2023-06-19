#include "TextFormatFile.h"

#include <memory>
#include <Formats/FormatSettings.h>
#include <Processors/Formats/Impl/HiveTextRowInputFormat.h>

namespace local_engine
{

TextFormatFile::TextFormatFile(
    DB::ContextPtr context_, const substrait::ReadRel::LocalFiles::FileOrFiles & file_info_, ReadBufferBuilderPtr read_buffer_builder_)
    : FormatFile(context_, file_info_, read_buffer_builder_)
{
}

FormatFile::InputFormatPtr TextFormatFile::createInputFormat(const DB::Block & header)
{
    auto res = std::make_shared<FormatFile::InputFormat>();
    res->read_buffer = std::move(read_buffer_builder->build(file_info, true));

    /// Initialize format params
    size_t max_block_size = file_info.text().max_block_size();
    DB::RowInputFormatParams params = {.max_block_size = max_block_size};

    /// Initialize format settings
    DB::FormatSettings format_settings = DB::getFormatSettings(context);
    const auto & schema = file_info.text().schema();
    for (const auto & name : schema.names())
        format_settings.hive_text.input_field_names.push_back(name);
    std::string text_field_delimiter = file_info.text().field_delimiter();
    if (text_field_delimiter == "\t" || text_field_delimiter == " ")
    {
       format_settings.csv.allow_whitespace_or_tab_as_delimiter = true;
    }
    format_settings.hive_text.fields_delimiter = file_info.text().field_delimiter()[0];
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
        format_settings.csv.allow_double_quotes = false;
    }
    res->input = std::make_shared<DB::HiveTextRowInputFormat>(header, *(res->read_buffer), params, format_settings);
    return res;
}

}

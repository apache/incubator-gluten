#include "JsonFormatFile.h"

#include <Formats/FormatSettings.h>
#include <Formats/FormatFactory.h>
#include <Processors/Formats/Impl/JSONEachRowRowInputFormat.h>

namespace local_engine
{

JsonFormatFile::JsonFormatFile(DB::ContextPtr context_, const substrait::ReadRel::LocalFiles::FileOrFiles & file_info_, ReadBufferBuilderPtr read_buffer_builder_)
    :FormatFile(context_, file_info_, read_buffer_builder_) {}

FormatFile::InputFormatPtr JsonFormatFile::createInputFormat(const DB::Block & header) 
{
    auto res = std::make_shared<FormatFile::InputFormat>();
    res->read_buffer = std::move(read_buffer_builder->build(file_info, true));
    DB::FormatSettings format_settings = DB::getFormatSettings(context);
    format_settings.with_names_use_header = true;
    format_settings.skip_unknown_fields = true;
    size_t max_block_size = file_info.json().max_block_size();
    DB::RowInputFormatParams in_params = {max_block_size};
    std::shared_ptr<DB::JSONEachRowRowInputFormat> json_input_format = 
        std::make_shared<DB::JSONEachRowRowInputFormat>(*(res->read_buffer), header, in_params, format_settings, false);
    res->input = json_input_format;
    return res;
}

}

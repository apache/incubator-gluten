#include "ParquetOutputFormatFile.h"

#if USE_PARQUET

#    include <memory>
#    include <string>
#    include <utility>

#    include <Formats/FormatFactory.h>
#    include <Formats/FormatSettings.h>
#    include <Processors/Formats/Impl/ArrowBufferedStreams.h>
#    include <Processors/Formats/Impl/CHColumnToArrowColumn.h>
#    include <Processors/Formats/Impl/ParquetBlockOutputFormat.h>
#    include <parquet/arrow/writer.h>
#    include <Common/Config.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}
}

namespace local_engine
{
ParquetOutputFormatFile::ParquetOutputFormatFile(
    DB::ContextPtr context_, const std::string & file_uri_, WriteBufferBuilderPtr write_buffer_builder_)
    : OutputFormatFile(context_, file_uri_, write_buffer_builder_)
{
}

OutputFormatFile::OutputFormatPtr ParquetOutputFormatFile::createOutputFormat(const DB::Block & header)
{
    auto res = std::make_shared<OutputFormatFile::OutputFormat>();
    res->write_buffer = std::move(write_buffer_builder->build(file_uri));

    //TODO: align spark parquet config with ch parquet config
    auto format_settings = DB::getFormatSettings(context);
    auto output_format = std::make_shared<DB::ParquetBlockOutputFormat>(*(res->write_buffer), header, format_settings);

    res->output = output_format;
    return res;
}

}
#endif

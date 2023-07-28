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
    DB::ContextPtr context_,
    const std::string & file_uri_,
    WriteBufferBuilderPtr write_buffer_builder_,
    std::vector<std::string> & preferred_column_names_)
    : OutputFormatFile(context_, file_uri_, write_buffer_builder_, preferred_column_names_)
{
}

OutputFormatFile::OutputFormatPtr ParquetOutputFormatFile::createOutputFormat(const DB::Block & header)
{
    auto res = std::make_shared<OutputFormatFile::OutputFormat>();
    res->write_buffer = write_buffer_builder->build(file_uri);
    // TODO: align all spark parquet config with ch parquet config
    auto format_settings = DB::getFormatSettings(context);

    if (!preferred_column_names.empty())
    {
        //create a new header with the preferred column name
        DB::NamesAndTypesList names_types_list = header.getNamesAndTypesList();
        DB::ColumnsWithTypeAndName cols;
        size_t index = 0;
        for (const auto & name_type : header.getNamesAndTypesList())
        {
            if (name_type.name.starts_with("__bucket_value__"))
                continue;

            DB::ColumnWithTypeAndName col(name_type.type->createColumn(), name_type.type, preferred_column_names.at(index++));
            cols.emplace_back(col);
        }
        assert(preferred_column_names.size() == index);
        const DB::Block new_header(cols);
        auto output_format = std::make_shared<DB::ParquetBlockOutputFormat>(*(res->write_buffer), new_header, format_settings);
        res->output = output_format;
        return res;
    }
    else
    {
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "preferred_column_names is empty");
    }
}

}
#endif

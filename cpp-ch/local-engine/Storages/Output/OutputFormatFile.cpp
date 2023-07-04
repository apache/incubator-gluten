#include "OutputFormatFile.h"
#include <boost/algorithm/string/case_conv.hpp>
#include "ParquetOutputFormatFile.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}
}
namespace local_engine
{
OutputFormatFile::OutputFormatFile(
    DB::ContextPtr context_,
    const std::string & file_uri_,
    WriteBufferBuilderPtr write_buffer_builder_,
    std::vector<std::string> & preferred_column_names_)
    : context(context_), file_uri(file_uri_), write_buffer_builder(write_buffer_builder_), preferred_column_names(preferred_column_names_)
{
}

OutputFormatFilePtr OutputFormatFileUtil::createFile(
    DB::ContextPtr context,
    local_engine::WriteBufferBuilderPtr write_buffer_builder,
    const std::string & file_uri,
    std::vector<std::string> & preferred_column_names,
    const std::string & format_hint)
{
#if USE_PARQUET
    if (boost::to_lower_copy(file_uri).ends_with(".parquet") || "parquet" == format_hint)
    {
        return std::make_shared<ParquetOutputFormatFile>(context, file_uri, write_buffer_builder, preferred_column_names);
    }
#endif

    throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Format not supported for file :{}", file_uri);
}
}

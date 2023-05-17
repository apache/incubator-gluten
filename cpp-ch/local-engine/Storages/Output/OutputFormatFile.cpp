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
OutputFormatFile::OutputFormatFile(DB::ContextPtr context_, const std::string & file_uri_, WriteBufferBuilderPtr write_buffer_builder_)
    : context(context_), file_uri(file_uri_), write_buffer_builder(write_buffer_builder_)
{
}

OutputFormatFilePtr OutputFormatFileUtil::createFile(
    DB::ContextPtr context, local_engine::WriteBufferBuilderPtr write_buffer_builder, const std::string & file_uri)
{
#if USE_PARQUET
    //TODO: can we support parquet file with suffix like .parquet1?
    if (boost::to_lower_copy(file_uri).ends_with(".parquet"))
    {
        return std::make_shared<ParquetOutputFormatFile>(context, file_uri, write_buffer_builder);
    }
#endif

    throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Format not supported for file :{}", file_uri);
}
}

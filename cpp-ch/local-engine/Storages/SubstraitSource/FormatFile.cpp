#include "FormatFile.h"

#include <memory>
#include <IO/ReadBufferFromFile.h>
#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <Common/logger_useful.h>

#if USE_PARQUET
#include <Storages/SubstraitSource/ParquetFormatFile.h>
#endif

#if USE_ORC
#include <Storages/SubstraitSource/OrcFormatFile.h>
#endif
#if USE_HIVE
#include <Storages/SubstraitSource/TextFormatFile.h>
#endif
#include <Storages/SubstraitSource/JsonFormatFile.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}
}
namespace local_engine
{
FormatFile::FormatFile(
    DB::ContextPtr context_, const substrait::ReadRel::LocalFiles::FileOrFiles & file_info_, ReadBufferBuilderPtr read_buffer_builder_)
    : context(context_), file_info(file_info_), read_buffer_builder(read_buffer_builder_)
{
    PartitionValues part_vals = StringUtils::parsePartitionTablePath(file_info.uri_file());
    for (const auto & part : part_vals)
    {
        partition_keys.push_back(part.first);
        partition_values[part.first] = part.second;
    }
}

FormatFilePtr FormatFileUtil::createFile(
    DB::ContextPtr context, ReadBufferBuilderPtr read_buffer_builder, const substrait::ReadRel::LocalFiles::FileOrFiles & file)
{
#if USE_PARQUET
    if (file.has_parquet())
    {
        return std::make_shared<ParquetFormatFile>(context, file, read_buffer_builder);
    }
#endif

#if USE_ORC
    if (file.has_orc())
    {
        return std::make_shared<OrcFormatFile>(context, file, read_buffer_builder);
    }
#endif

#if USE_HIVE
    if (file.has_text())
    {
        return std::make_shared<TextFormatFile>(context, file, read_buffer_builder);
    }
#endif

    if (file.has_json())
    {
        return std::make_shared<JsonFormatFile>(context, file, read_buffer_builder);
    }
    throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Format not supported:{}", file.DebugString());
    __builtin_unreachable();
}
}

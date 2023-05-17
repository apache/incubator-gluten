#pragma once

#include "config.h"

#if USE_PARQUET

#    include <memory>
#    include <IO/WriteBuffer.h>
#    include <Storages/Output/OutputFormatFile.h>

namespace local_engine
{
//TODO: support ORC
class ParquetOutputFormatFile : public OutputFormatFile
{
public:
    explicit ParquetOutputFormatFile(DB::ContextPtr context_, const std::string & file_uri_, WriteBufferBuilderPtr write_buffer_builder_);
    ~ParquetOutputFormatFile() override = default;
    OutputFormatFile::OutputFormatPtr createOutputFormat(const DB::Block & header) override;
};

}
#endif

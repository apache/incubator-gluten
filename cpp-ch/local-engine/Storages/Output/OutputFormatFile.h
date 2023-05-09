#pragma once

#include <memory>
#include <optional>
#include <vector>

#include <Core/Block.h>
#include <IO/WriteBuffer.h>
#include <Interpreters/Context.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Storages/Output/WriteBufferBuilder.h>

namespace local_engine
{
class OutputFormatFile
{
public:
    struct OutputFormat
    {
    public:
        DB::OutputFormatPtr output;
        std::unique_ptr<DB::WriteBuffer> write_buffer;
    };
    using OutputFormatPtr = std::shared_ptr<OutputFormat>;

    OutputFormatFile(DB::ContextPtr context_, const std::string & file_uri_, WriteBufferBuilderPtr write_buffer_builder_);

    virtual ~OutputFormatFile() = default;

    virtual OutputFormatPtr createOutputFormat(const DB::Block & header_) = 0;

    //TODO: support split
    //TODO: support partitioning

protected:
    DB::ContextPtr context;
    const std::string file_uri;
    WriteBufferBuilderPtr write_buffer_builder;
};
using OutputFormatFilePtr = std::shared_ptr<OutputFormatFile>;

class OutputFormatFileUtil
{
public:
    static OutputFormatFilePtr
    createFile(DB::ContextPtr context, WriteBufferBuilderPtr write_buffer_builder_, const std::string & file_uri_);
};
}

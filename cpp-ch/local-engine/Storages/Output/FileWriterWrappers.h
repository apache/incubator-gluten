#pragma once

#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/Field.h>
#include <Interpreters/Context.h>
#include <Parser/SerializedPlanParser.h>
#include <Processors/Chunk.h>
#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Processors/ISource.h>
#include <Storages/Output/OutputFormatFile.h>
#include <Storages/Output/WriteBufferBuilder.h>
#include <Storages/SourceFromJavaIter.h>
#include <base/types.h>

namespace local_engine
{

class FileWriterWrapper
{
public:
    explicit FileWriterWrapper(OutputFormatFilePtr file_) : file(file_) { }
    virtual ~FileWriterWrapper() = default;
    virtual void consume(DB::Block & block) = 0;
    virtual void close() = 0;

protected:
    OutputFormatFilePtr file;
};

using FileWriterWrapperPtr = std::shared_ptr<FileWriterWrapper>;

class NormalFileWriter : public FileWriterWrapper
{
public:
    //TODO: EmptyFileReader and ConstColumnsFileReader ?
    //TODO: to support complex types
    NormalFileWriter(OutputFormatFilePtr file_, DB::ContextPtr context_);
    ~NormalFileWriter() override = default;
    void consume(DB::Block & block) override;
    void close() override;

private:
    DB::ContextPtr context;

    OutputFormatFile::OutputFormatPtr output_format;
    std::unique_ptr<DB::QueryPipeline> pipeline;
    std::unique_ptr<DB::PushingPipelineExecutor> writer;
};

FileWriterWrapper * createFileWriterWrapper(std::string file_uri);
}

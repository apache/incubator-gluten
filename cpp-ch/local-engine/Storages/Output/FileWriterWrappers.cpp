#include "FileWriterWrappers.h"
#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <QueryPipeline/QueryPipeline.h>

namespace local_engine
{

NormalFileWriter::NormalFileWriter(OutputFormatFilePtr file_, DB::ContextPtr context_) : FileWriterWrapper(file_), context(context_)
{
}


void NormalFileWriter::consume(DB::Block & block)
{
    if (!writer) [[unlikely]]
    {
        // init the writer at first block
        output_format = file->createOutputFormat(block.cloneEmpty());
        pipeline = std::make_unique<DB::QueryPipeline>(output_format->output);
        writer = std::make_unique<DB::PushingPipelineExecutor>(*pipeline);
    }
    writer->push(std::move(block));
}

void NormalFileWriter::close()
{
    writer->finish();
}

FileWriterWrapper * createFileWriterWrapper(std::string file_uri)
{
    Poco::URI poco_uri(file_uri);
    auto context = DB::Context::createCopy(local_engine::SerializedPlanParser::global_context);
    auto write_buffer_builder = WriteBufferBuilderFactory::instance().createBuilder(poco_uri.getScheme(), context);
    auto file = OutputFormatFileUtil::createFile(context, write_buffer_builder, file_uri);
    return new NormalFileWriter(file, context);
}

}

#pragma once

#include <list>
#include <Processors/IProcessor.h>
#include <Processors/Port.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>


namespace local_engine
{
class BlocksBufferPoolStep : public DB::ITransformingStep
{
public:
    explicit BlocksBufferPoolStep(const DB::DataStream & input_stream_, size_t buffer_size_ = 4);
    ~BlocksBufferPoolStep() override = default;

    String getName() const override { return "BlocksBufferPoolStep"; }

    void transformPipeline(DB::QueryPipelineBuilder & pipeline, const DB::BuildQueryPipelineSettings & settings) override;
    void describePipeline(DB::IQueryPlanStep::FormatSettings & settings) const override;
private:
    DB::Block header;
    size_t buffer_size;
    void updateOutputStream() override;
};

class BlocksBufferPoolTransform  : public DB::IProcessor
{
public:
    using Status = DB::IProcessor::Status;
    explicit BlocksBufferPoolTransform(const DB::Block & header, size_t buffer_size_ = 4);
    ~BlocksBufferPoolTransform() override = default;

    Status prepare() override;
    void work() override;

    DB::String getName() const override { return "BlocksBufferPoolTransform"; }
private:
    std::list<DB::Chunk> pending_chunks;
    size_t buffer_size;
};
}

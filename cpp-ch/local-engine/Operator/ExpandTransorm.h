#pragma once
#include <set>
#include <vector>
#include <Core/Block.h>
#include <Processors/Chunk.h>
#include <Processors/IProcessor.h>
#include <Processors/Port.h>
namespace local_engine
{
// For handling substrait expand node.
// The implementation in spark for groupingsets/rollup/cube is different from Clickhouse.
// We have to ways to support groupingsets/rollup/cube
// - rewrite the substrait plan in local engine and reuse the implementation of clickhouse. This
//   may be more complex.
// - implement new transform to do the expandation. It's more simple, but may suffer some performance
//   issues. We try this first.
class ExpandTransform : public DB::IProcessor
{
public:
    using Status = DB::IProcessor::Status;
    ExpandTransform(
        const DB::Block & input_,
        const DB::Block & output_,
        const std::vector<size_t> & aggregating_expressions_columns_,
        const std::vector<std::set<size_t>> & grouping_sets_);

    Status prepare() override;
    void work() override;

    DB::String getName() const override { return "ExpandTransform"; }
private:
    std::vector<size_t> aggregating_expressions_columns;
    std::vector<std::set<size_t>> grouping_sets;
    bool has_input = false;
    bool has_output = false;

    DB::Chunk input_chunk;
    std::list<DB::Chunk> expanded_chunks;
    DB::Chunk nextChunk();
};
}

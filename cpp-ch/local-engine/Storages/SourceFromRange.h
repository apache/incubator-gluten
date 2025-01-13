#pragma once

#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Interpreters/Context.h>
#include <Processors/ISource.h>

namespace local_engine
{
class SourceFromRange : public DB::ISource
{
public:
    SourceFromRange(
        const DB::Block & header,
        Int64 start_,
        Int64 end_,
        Int64 step_,
        Int32 num_slices_,
        Int32 slice_index_,
        size_t max_block_size_ = 8192);
    ~SourceFromRange() override = default;

    String getName() const override { return "SourceFromRange"; }


private:
    DB::Chunk generate() override;

    Int128 getNumElements() const;

    const Int64 start;
    const Int64 end;
    const Int64 step;
    const Int32 num_slices;
    const Int32 slice_index;
    const size_t max_block_size;
    const Int128 num_elements;
    const bool is_empty_range;

    Int64 safe_partition_start;
    Int64 safe_partition_end;
    Int64 current;
    Int64 previous;
    bool overflow;
};
}


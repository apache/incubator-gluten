#pragma once

#include <Shuffle/ShuffleSplitter.h>

namespace DB
{
class Block;
}

namespace local_engine
{
class BlockCoalesceOperator
{
public:
    BlockCoalesceOperator(size_t buf_size_):buf_size(buf_size_){}
    virtual ~BlockCoalesceOperator();
    void mergeBlock(DB::Block & block);
    bool isFull();
    DB::Block* releaseBlock();

private:
    size_t buf_size;
    ColumnsBuffer block_buffer;
    DB::Block * cached_block = nullptr;

    void clearCache();
};
}



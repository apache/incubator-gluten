#include "BlockCoalesceOperator.h"
#include <Core/Block.h>

namespace local_engine
{
void BlockCoalesceOperator::mergeBlock(DB::Block & block)
{
    block_buffer.add(block, 0, block.rows());
}
bool BlockCoalesceOperator::isFull()
{
    return block_buffer.size() >= buf_size;
}
DB::Block* BlockCoalesceOperator::releaseBlock()
{
    clearCache();
    cached_block = new DB::Block(block_buffer.releaseColumns());
    return cached_block;
}
BlockCoalesceOperator::~BlockCoalesceOperator()
{
    clearCache();
}
void BlockCoalesceOperator::clearCache()
{
    if (cached_block)
    {
        delete cached_block;
        cached_block = nullptr;
    }
}
}


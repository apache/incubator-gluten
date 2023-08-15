#pragma once
#include <Core/Block.h>

namespace local_engine
{
struct SplitResult;
class ShuffleWriterBase
{
public:
    virtual void split(DB::Block & block) = 0;
    virtual size_t evictPartitions() {return 0;}
    virtual SplitResult stop() = 0;
    virtual ~ShuffleWriterBase() = default;
};
}

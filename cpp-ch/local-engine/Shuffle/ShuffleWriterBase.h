#pragma once
#include <Core/Block.h>

namespace local_engine
{
struct SplitResult;
class ShuffleWriterBase
{
public:
    virtual void split(DB::Block & block) = 0;
    virtual SplitResult stop() = 0;
    virtual ~ShuffleWriterBase() = default;
};
}

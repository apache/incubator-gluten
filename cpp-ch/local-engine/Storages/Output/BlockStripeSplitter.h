#pragma once

#include <Core/Block.h>

namespace local_engine
{
class BlockStripes
{
public:
    int64_t originalBlockAddress;
    std::vector<int64_t> blockAddresses;
    std::vector<int32_t> headingRowIndice;
    int originBlockColNum;
};

class BlockStripeSplitter
{
public:
    static BlockStripes split(const DB::Block & block, const std::vector<int> partitionColIndice, const bool hasBucket);
};

}

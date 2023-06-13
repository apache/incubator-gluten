#pragma once

#include <Core/Block.h>

namespace local_engine
{
class BlockStripe
{
public:
    long blockAddress;
    long headingRowAddress;
    int headingRowBytes;
    int bucketId;
    int rows;
    int columns;
};

class BlockStripeSplitter
{
public:
    static std::vector<BlockStripe> split(const DB::Block & block, const std::vector<int> partitionColIndice, const bool hasBucket);
};

}

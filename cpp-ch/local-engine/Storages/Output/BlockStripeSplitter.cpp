//
// Created by hongbin on 6/28/23.
//

#include "BlockStripeSplitter.h"

using namespace local_engine;

std::vector<BlockStripe>
local_engine::BlockStripeSplitter::split(const DB::Block & block, const std::vector<int> partitionColIndice, const bool hasBucket)
{
    std::vector<BlockStripe> ret;
    std::vector<size_t> splitPoints;

    std::vector<int> columns = partitionColIndice;
    if (hasBucket)
        columns.push_back(block.columns() - 1);
    for (size_t i = 0; i < columns.size(); i++)
    {
        auto columnPtr = block.getColumns().at(columns.at(i));
        for (size_t j = 1; j < block.rows(); ++j)
        {
            if (columnPtr->compareAt(j - 1, j, *columnPtr, 1) != 0)
            {
                splitPoints.push_back(j);
            }
        }
    }
    //sort split points
    std::sort(splitPoints.begin(), splitPoints.end());
    //dedup split points
    splitPoints.erase(std::unique(splitPoints.begin(), splitPoints.end()), splitPoints.end());
    splitPoints.push_back(block.rows());

    for (size_t i = 0; i < splitPoints.size(); i++)
    {
        size_t from = i == 0 ? 0 : splitPoints.at(i - 1);
        size_t to = splitPoints.at(i);
        DB::Block stripe = block.cloneWithCutColumns(from, to - from + 1);
    }
}

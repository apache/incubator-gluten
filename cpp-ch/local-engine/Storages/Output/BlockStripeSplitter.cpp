//
// Created by hongbin on 6/28/23.
//

#include "BlockStripeSplitter.h"

using namespace local_engine;

BlockStripes
local_engine::BlockStripeSplitter::split(const DB::Block & block, const std::vector<int> partitionColIndice, const bool hasBucket)
{
    BlockStripes ret;
    ret.originalBlockAddress = reinterpret_cast<int64_t>(&block);
    ret.originBlockColNum = block.columns();

    std::vector<size_t> splitPoints;

    std::vector<int> columns = partitionColIndice;
    if (hasBucket)
        columns.push_back(block.columns() - 1);
    for (size_t i = 0; i < columns.size(); i++)
    {
        auto columnPtr = block.getColumns().at(columns.at(i));
        if (columnPtr->compareAt(0, block.rows() - 1, *columnPtr, 1) == 0)
        {
            // no value changes for this column
            continue;
        }

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

    // create output block by ignoring the partition cols
    DB::ColumnsWithTypeAndName outputColumns;
    for (size_t colIndex = 0; colIndex < block.columns(); ++colIndex)
    {
        // partition columns will not be written to the file (they're written to folder name)
        if (std::find(partitionColIndice.begin(), partitionColIndice.end(), colIndex) == partitionColIndice.end())
        {
            outputColumns.push_back(block.getByPosition(colIndex));
        }
    }
    DB::Block outputBlock(outputColumns);

    for (size_t i = 0; i < splitPoints.size(); i++)
    {
        size_t from = i == 0 ? 0 : splitPoints.at(i - 1);
        size_t to = splitPoints.at(i);
        const DB::Block & cutColumns = outputBlock.cloneWithCutColumns(from, to - from);
        DB::Block * p = new DB::Block(cutColumns);
        //TODO: what if it's single partition? do we need to clone? double release?
        // test cases: 1. 100 rows, 100 partitions 2. 100 rows, 1 partition 3. 0 rows

        ret.headingRowIndice.push_back(from);
        ret.blockAddresses.push_back(reinterpret_cast<int64_t>(p));
    }
    return ret;
}

//
// Created by hongbin on 6/28/23.
//

#include "BlockStripeSplitter.h"
#include <Columns/ColumnNullable.h>


using namespace local_engine;

BlockStripes
local_engine::BlockStripeSplitter::split(const DB::Block & block, const std::vector<size_t>& partitionColIndice, const bool hasBucket)
{
    BlockStripes ret;
    ret.originalBlockAddress = reinterpret_cast<int64_t>(&block);
    ret.originBlockColNum = static_cast<int>(block.columns());

    std::vector<size_t> splitPoints;

    std::vector<size_t> columns = partitionColIndice;
    if (hasBucket)
        columns.push_back(block.columns() - 1);
    for (size_t i = 0; i < columns.size(); i++)
    {
        auto columnPtr = block.getColumns().at(columns.at(i));
        if (i == 0 && columnPtr->compareAt(0, block.rows() - 1, *columnPtr, 1) == 0)
        {
            // no value changes for this whole column
            continue;
        }

        for (size_t j = 1; j < block.rows(); ++j)
        {
            if (columnPtr->compareAt(j - 1, j, *columnPtr, 1) != 0)
            {
                //                std::cout << "j-1(nullable): " << columnPtr->isNullAt(j - 1) << "; j(nullable):" << columnPtr->isNullAt(j) << std::endl;
                //                std::cout << "j-1: " << static_cast<const DB::ColumnNullable *>(columnPtr.get())->getNestedColumn().getInt(j - 1) <<
                //                    "; j:" << static_cast<const DB::ColumnNullable *>(columnPtr.get())->getNestedColumn().getInt(j ) << std::endl;
                splitPoints.push_back(j);
            }
        }
    }

    //sort split points
    std::sort(splitPoints.begin(), splitPoints.end());
    //dedup split points
    splitPoints.erase(std::unique(splitPoints.begin(), splitPoints.end()), splitPoints.end());
    splitPoints.push_back(block.rows());

    //    if (splitPoints.size() == 1)
    //    {
    //        // if no need to split this block
    //        ret.noNeedSplit = true;
    //        ret.headingRowIndice.push_back(0);
    //        ret.blockAddresses.push_back(ret.originalBlockAddress);
    //        return ret;
    //    }

    // create output block by ignoring the partition cols
    DB::ColumnsWithTypeAndName outputColumns;
    for (size_t colIndex = 0; colIndex < block.columns(); ++colIndex)
    {
        // partition columns will not be written to the file (they're written to folder name)
        if (std::find(partitionColIndice.begin(), partitionColIndice.end(), colIndex) != partitionColIndice.end())
            continue;

        // the last column is a column representing bucketing hash value (__bucket_value__), which is not written to the file
        if (hasBucket && colIndex == block.columns() - 1)
            continue;

        outputColumns.push_back(block.getByPosition(colIndex));
    }
    DB::Block outputBlock(outputColumns);

    for (size_t i = 0; i < splitPoints.size(); i++)
    {
        size_t from = i == 0 ? 0 : splitPoints.at(i - 1);
        size_t to = splitPoints.at(i);

        DB::Block * p = nullptr;
        if (splitPoints.size() != 1)
        {
            const DB::Block & cutColumns = outputBlock.cloneWithCutColumns(from, to - from);
            p = new DB::Block(cutColumns);
        }
        else
        {
            // optimization for no split
            p = new DB::Block(outputBlock);
        }

        ret.headingRowIndice.push_back(static_cast<int32_t>(from));
        ret.blockAddresses.push_back(reinterpret_cast<int64_t>(p));
    }
    return ret;
}

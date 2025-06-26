/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "ColumnIndexFilterUtils.h"
#include <Storages/Parquet/RowRanges.h>
#include <Storages/Parquet/ParquetReadState.h>

struct FilteredOffsetIndex final : local_engine::OffsetIndex
{
    std::unique_ptr<OffsetIndex> offset_index_;
    std::vector<int32_t> index_map_;

    FilteredOffsetIndex(std::unique_ptr<OffsetIndex> offset_index, std::vector<int32_t> index_map)
        : offset_index_(std::move(offset_index)), index_map_(std::move(index_map))
    {
    }

    int32_t getPageCount() const override { return static_cast<int32_t>(index_map_.size()); }

    int64_t getOffset(int32_t pageIndex) const override { return offset_index_->getOffset(index_map_[pageIndex]); }

    int32_t getCompressedPageSize(int32_t pageIndex) const override { return offset_index_->getCompressedPageSize(index_map_[pageIndex]); }

    int64_t getFirstRowIndex(int32_t pageIndex) const override { return offset_index_->getFirstRowIndex(index_map_[pageIndex]); }

    int64_t getLastRowIndex(int32_t pageIndex) const override { return offset_index_->getLastRowIndex(index_map_[pageIndex]);}

};

namespace local_engine
{

std::unique_ptr<OffsetIndex> ColumnIndexFilterUtils::filterOffsetIndex(
    const std::vector<parquet::PageLocation> & page_locations, const RowRanges & rowRanges, int64_t rowGroupRowCount)
{
    auto offset_index = std::make_unique<OffsetIndexImpl>(page_locations, rowGroupRowCount);
    std::vector<int32_t> index_map;
    const auto pageCount = offset_index->getPageCount();
    for (int32_t i = 0; i < pageCount; ++i)
    {
        int64_t from = offset_index->getFirstRowIndex(i);
        if (rowRanges.isOverlapping(from, offset_index->getLastRowIndex(i)))
            index_map.push_back(i);
    }
    return std::make_unique<FilteredOffsetIndex>(std::move(offset_index), std::move(index_map));
}

ReadRanges ColumnIndexFilterUtils::calculateReadRanges(
    const OffsetIndex & offset_index, const arrow::io::ReadRange & chunk_range, int64_t firstPageOffset)
{
    // offsetIndex could be a FilteredOffsetIndex or OffsetIndexImpl
    int32_t n = offset_index.getPageCount();
    if (n <= 0)
        return {};

    ReadRanges ranges;

    // Add a range for dictionary page if required
    int64_t rowGroupOffset = chunk_range.offset;
    if (rowGroupOffset < firstPageOffset)
        ranges.emplace_back(rowGroupOffset, firstPageOffset - rowGroupOffset);

    // initialize the first range if needed.
    int32_t pageIndex = 0;
    if (ranges.empty())
    {
        ranges.emplace_back(offset_index.getOffset(pageIndex), offset_index.getCompressedPageSize(pageIndex));
        pageIndex +=1;
    }

    auto extend = [](arrow::io::ReadRange & read_range, const int64_t offset, const int64_t length)
    {
        if (read_range.offset + read_range.length == offset)
        {
            read_range.length += length;
            return true;
        }
        return false;
    };

    assert(!ranges.empty());
    for (; pageIndex < n; ++pageIndex)
    {
        int64_t offset = offset_index.getOffset(pageIndex);
        int32_t length = offset_index.getCompressedPageSize(pageIndex);
        if (!extend(ranges.back(), offset, length))
        {
            // create a new range
            ranges.emplace_back(offset, length);
        }
    }
    return ranges;
}
ReadSequence ColumnIndexFilterUtils::calculateReadSequence(const OffsetIndex & offset_index, const RowRanges & rowRanges)
{
    ParquetReadState2 read_state(rowRanges);
    ReadSequence read_sequence;

    read_state.setSkipFunc([& read_sequence](int64_t number)
    {
        assert(number > 0);
        if (read_sequence.empty() || read_sequence.back() > 0)
            read_sequence.push_back(-number);
        else
            read_sequence.back() -= number;
    });

    read_state.setReadFunc(
        [&read_sequence](int64_t number)
        {
            assert(number > 0);
            if (read_sequence.empty() || read_sequence.back() < 0)
                read_sequence.push_back(number);
            else
                read_sequence.back() += number;
        });

    read_state.resetForNewBatch(std::numeric_limits<int32_t>::max());
    for (int32_t pageIndex = 0; pageIndex < offset_index.getPageCount(); ++pageIndex)
    {
        read_state.resetForNewPage(offset_index.getLastRowIndex(pageIndex) - offset_index.getFirstRowIndex(pageIndex) + 1,
                                   offset_index.getFirstRowIndex(pageIndex));
        read_state.read();
    }

    assert(!read_sequence.empty());
    return read_sequence;
}

ReadRanges ColumnIndexFilterUtils::calculateReadRanges(
    const int64_t rowGroupRowCount,
    const arrow::io::ReadRange & chunk_range,
    const std::vector<parquet::PageLocation> & page_locations,
    const RowRanges & rowRanges)
{
    //TODO: implement in product codes
    if (rowRanges.rowCount() == rowGroupRowCount)
        return {chunk_range};

    auto offset_index = filterOffsetIndex(page_locations, rowRanges, rowGroupRowCount);
    return calculateReadRanges(*offset_index, chunk_range, page_locations[0].offset);
}

ReadSequence ColumnIndexFilterUtils::calculateReadSequence(
    int64_t rowGroupRowCount, const std::vector<parquet::PageLocation> & page_locations, const RowRanges & rowRanges)
{
    //TODO: implement in product codes
    if (rowRanges.rowCount() == rowGroupRowCount)
        return {rowGroupRowCount};

    auto offset_index = filterOffsetIndex(page_locations, rowRanges, rowGroupRowCount);
    return calculateReadSequence(*offset_index, rowRanges);
}

}
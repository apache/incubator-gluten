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
#pragma once
#include <functional>
#include <optional>
#include <vector>
#include <Storages/Parquet/OffsetIndex.h>
#include <Storages/Parquet/RowRanges.h>
#include <base/defines.h>

namespace local_engine
{
/**
* Helper class to store intermediate state while reading a Parquet column chunk.
*/
class ParquetReadState2
{
public:
    using SkipFunc = std::function<int64_t(int64_t)>;
    using ReadFunc = std::function<int64_t(int64_t)>;

    using ReadPageFunc = std::function<void()>;

private:
    /** A special row range used when there is no row indexes (hence all rows must be included) */
    static const Range MAX_ROW_RANGE;

    /**
     * A special row range used when the row indexes are present AND all the row ranges have been
     * processed. This serves as a sentinel at the end indicating that all rows come after the last
     * row range should be skipped.
     */
    static const Range END_ROW_RANGE;


    /** Iterator over all row ranges, only not-null if column index is present */
    std::optional<RowRanges> row_ranges_;
    std::vector<Range>::iterator row_ranges_it_;

    /** The current row range */
    const Range * current_row_range_;

    /** The current index over all rows within the column chunk. This is used to check if the
     * current row should be skipped by comparing against the row ranges. */
    size_t rowId_;

    /** The remaining number of values to read in the current page */
    int32_t valuesToReadInPage;

    /** The remaining number of rows to read in the current batch */
    int32_t rowsToReadInBatch;

    ReadFunc read_func_;
    SkipFunc skip_func_;

    std::unique_ptr<OffsetIndex> offset_index_;
    int32_t page_index_;

    const int64_t total_read_;
    int64_t already_read_;

#ifndef NDEBUG
    bool reading{false}; // Used to detect re-entrance in debug builds
#endif

    const Range * getNextRange()
    {
        if (row_ranges_)
        {
            if (row_ranges_it_ == row_ranges_->getRanges().end())
                return &END_ROW_RANGE;

            return &*row_ranges_it_++;
        }
        return &MAX_ROW_RANGE;
    }

    std::vector<Range>::iterator getRowRangesIt()
    {
        if (row_ranges_)
            return row_ranges_->getRanges().begin();
        return std::vector<Range>::iterator();
    }

public:
    bool hasMoreRead() const { return already_read_ < total_read_; }
    bool needSetPageFilter() const { return offset_index_ != nullptr; }
    void skipLastRecord() { /* do nothing */ }

    explicit ParquetReadState2(const std::optional<RowRanges> & row_ranges, std::unique_ptr<OffsetIndex> offset_index)
        : row_ranges_(row_ranges)
        , row_ranges_it_(getRowRangesIt())
        , current_row_range_(getNextRange())
        , rowId_(0)
        , valuesToReadInPage(0)
        , rowsToReadInBatch(-1)
        , read_func_(nullptr)
        , skip_func_(nullptr)
        , offset_index_(std::move(offset_index))
        , page_index_(0)
        , total_read_(row_ranges.transform([](const RowRanges & ranges) { return ranges.rowCount(); }).value_or(0))
        , already_read_(0)
    {
        chassert(row_ranges_.has_value());
        chassert(currentRangeStart() >= rowId_);
    }

    void setReadFunc(ReadFunc read_func) { read_func_ = std::move(read_func); }
    void setSkipFunc(SkipFunc skip_func) { skip_func_ = std::move(skip_func); }

    void nextRange() { current_row_range_ = getNextRange(); }

    /**
     * Must be called at the beginning of reading a new batch.
     */
    void resetForNewBatch(int32_t batchSize) { rowsToReadInBatch = batchSize; }

    /**
     * Must be called at the beginning of reading a new page.
     */
    void resetForNewPage()
    {
        chassert(offset_index_);
        chassert(valuesToReadInPage == 0);
        chassert(page_index_ >= 0 && page_index_ < offset_index_->getPageCount());
        if (page_index_ >= offset_index_->getPageCount())
            throw std::runtime_error("Invalid page index"); // TODO: use DB::Exception
#ifndef NDEBUG
        chassert(!reading);
#endif

        rowId_ = offset_index_->getFirstRowIndex(page_index_);
        valuesToReadInPage = offset_index_->getPageRowCount(page_index_);

        page_index_ += 1;
    }

    /**
     * Returns the start index of the current row range.
     */
    size_t currentRangeStart() const { return current_row_range_->from; }

    /**
     * Returns the end index of the current row range.
     */
    size_t currentRangeEnd() const { return current_row_range_->to; }

    void read();
    int64_t doRead(int64_t batch_size, const ReadPageFunc & readPage);

private:
    void doReadInPage();

    int64_t skipRecord(size_t count) const
    {
        chassert(skip_func_);
        return skip_func_(count);
    }

    int64_t readRecord(size_t count) const
    {
        chassert(read_func_);
        return read_func_(count);
    }

public:
    ///Used in UT
    int32_t getRowsToReadInBatch() const { return rowsToReadInBatch; }
};
}
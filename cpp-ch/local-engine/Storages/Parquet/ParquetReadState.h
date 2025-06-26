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
    using SkipFunc = std::function<void(int64_t)>;
    using ReadFunc = std::function<void(int64_t)>;

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

#ifndef NDEBUG
    bool reading{false}; // Used to detect re-entrance in debug builds
#endif

public:
    int32_t getRowsToReadInBatch() const { return rowsToReadInBatch; }

protected:
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
    explicit ParquetReadState2(const std::optional<RowRanges> & row_ranges)
        : row_ranges_(row_ranges)
        , row_ranges_it_(getRowRangesIt())
        , current_row_range_(getNextRange())
        , rowId_(0)
        , valuesToReadInPage(-1)
        , rowsToReadInBatch(-1)
        , skip_func_(nullptr)
        , read_func_(nullptr)
    {
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
    void resetForNewPage(int32_t totalValuesInPage, int64_t pageFirstRowIndex)
    {
#ifndef NDEBUG
        chassert(!reading);
#endif
        rowId_ = pageFirstRowIndex;
        valuesToReadInPage = totalValuesInPage;
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

private:
    void doReadInPage();

    void skipRecord(size_t count) const
    {
        chassert(skip_func_);
        skip_func_(count);
    }

    void readRecord(size_t count) const
    {
        chassert(read_func_);
        read_func_(count);
    }
};
}
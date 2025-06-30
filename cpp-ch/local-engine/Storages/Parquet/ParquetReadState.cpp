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

#include "ParquetReadState.h"

#include <base/scope_guard.h>

namespace local_engine
{
const Range ParquetReadState2::MAX_ROW_RANGE{0, std::numeric_limits<size_t>::max()};
const Range ParquetReadState2::END_ROW_RANGE{std::numeric_limits<size_t>::max(), 0};

void ParquetReadState2::read()
{
#ifndef NDEBUG
    SCOPE_EXIT({
        chassert(reading);
        reading = false;
    });

    reading = true;
#endif
    chassert(rowsToReadInBatch >= 0);
    chassert(valuesToReadInPage > 0);

    doReadInPage();
}
int64_t ParquetReadState2::doRead(int64_t batch_size)
{
    resetForNewBatch(batch_size);
#ifndef NDEBUG
    SCOPE_EXIT({
        chassert(reading);
        reading = false;
    });

    reading = true;
#endif
    chassert(rowsToReadInBatch >= 0);
    chassert(valuesToReadInPage > 0);
    doReadInPage();
    return rowsToReadInBatch;
}
void ParquetReadState2::doReadInPage()
{
    size_t rowId = rowId_;
    int32_t leftInBatch = rowsToReadInBatch;
    int32_t leftInPage = valuesToReadInPage;

    while (leftInBatch > 0 && leftInPage > 0)
    {
        int32_t n = std::min(leftInBatch, leftInPage);
        size_t rangeStart = currentRangeStart();
        size_t rangeEnd = currentRangeEnd();

        if (rowId + n < rangeStart)
        {
            [[maybe_unused]] const int64_t skipped = skipRecord(n);
            chassert(skipped == n);
            rowId += n;
            leftInPage -= n;
        }
        else if (rowId > rangeEnd)
        {
            nextRange();
        }
        else
        {
            // The range [rowId, rowId + n) overlaps with the current row range in state
            const int64_t start = std::max(rangeStart, rowId);
            const int64_t end = std::min(rangeEnd, rowId + n - 1);

            // Skip the part [rowId, start)
            if (int64_t toSkip = start - rowId; toSkip > 0)
            {
                [[maybe_unused]] const int64_t skipped = skipRecord(toSkip);
                chassert(skipped == toSkip);

                rowId += toSkip;
                leftInPage -= toSkip;
            }

            // Read the part [start, end]
            n = end - start + 1;
            already_read_ += readRecord(n);
            leftInBatch -= n;
            rowId += n;
            leftInPage -= n;
        }
    }

    rowsToReadInBatch = leftInBatch;
    valuesToReadInPage = leftInPage;
    rowId_ = rowId;
}
}
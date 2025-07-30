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

#include <optional>
#include <base/defines.h>
#include <parquet/page_index.h>

namespace local_engine
{

/**
 * Offset index containing the offset and size of the page and the index of the first row in the page.
 * See parquet::OffsetIndex and parquet::format::OffsetIndex
 */
class OffsetIndex
{
public:
    virtual ~OffsetIndex() = default;

    /**
     * @return the number of pages
     */
    virtual int32_t getPageCount() const = 0;

    /**
     * @param pageIndex the index of the page
     * @return the offset of the page in the file
     */
    virtual int64_t getOffset(int32_t pageIndex) const = 0;

    /**
     * @param pageIndex the index of the page
     * @return the compressed size of the page (including page header)
     */
    virtual int32_t getCompressedPageSize(int32_t pageIndex) const = 0;

    /**
     * @param pageIndex the index of the page
     * @return the index of the first row in the page
     */
    virtual int64_t getFirstRowIndex(int32_t pageIndex) const = 0;

    /**
     * @param pageIndex the index of the page
     * @return the original ordinal of the page in the column chunk
     */
    virtual int32_t getPageOrdinal(int32_t pageIndex) const { return pageIndex; }

    /**
     * @param pageIndex        the index of the page
     * @return the calculated index of the last row of the given page
     */
    virtual int64_t getLastRowIndex(int32_t pageIndex) const = 0;

    /**
     * @param pageIndex
     *          the index of the page
     * @return unencoded/uncompressed size for BYTE_ARRAY types; or empty for other types.
     *    Please note that even for BYTE_ARRAY types, this value might not have been written.
     */
    virtual std::optional<int64_t> getUnencodedByteArrayDataBytes(int32_t /*pageIndex*/) const
    {
        chassert(false && "Not implemented");
        return std::nullopt;
    }

    int64_t getPageRowCount(int32_t pageIndex) const { return getLastRowIndex(pageIndex) - getFirstRowIndex(pageIndex) + 1; }
};

class OffsetIndexImpl final : public OffsetIndex
{
    const std::vector<parquet::PageLocation> & page_locations_;
    int64_t rowGroupRowCount_;

public:
    explicit OffsetIndexImpl(const std::vector<parquet::PageLocation> & page_locations, int64_t rowGroupRowCount)
        : page_locations_(page_locations), rowGroupRowCount_(rowGroupRowCount)
    {
    }

    int32_t getPageCount() const override { return page_locations_.size(); }

    int64_t getOffset(int32_t pageIndex) const override { return page_locations_[pageIndex].offset; }

    int32_t getCompressedPageSize(int32_t pageIndex) const override { return page_locations_[pageIndex].compressed_page_size; }

    int64_t getFirstRowIndex(int32_t pageIndex) const override { return page_locations_[pageIndex].first_row_index; }

    int64_t getLastRowIndex(int32_t pageIndex) const override
    {
        int32_t nextPageIndex = pageIndex + 1;
        return (nextPageIndex >= getPageCount() ? rowGroupRowCount_ : getFirstRowIndex(nextPageIndex)) - 1;
    }
};
}
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
#include <memory>
#include <Storages/Parquet/OffsetIndex.h>

namespace local_engine
{
class RowRanges;
using ReadRanges = std::vector<arrow::io::ReadRange>;
using ReadSequence = std::vector<int64_t>;

namespace ColumnIndexFilterUtils
{
std::unique_ptr<OffsetIndex>
filterOffsetIndex(const std::vector<parquet::PageLocation> & page_locations, const RowRanges & rowRanges, int64_t rowGroupRowCount);

/// \brief Calculate read ranges for a given offset index and column range.
/// chunk_range: see computeColumnChunkRange
ReadRanges calculateReadRanges(const OffsetIndex & offset_index, const arrow::io::ReadRange & chunk_range, int64_t firstPageOffset);

ReadSequence calculateReadSequence(const OffsetIndex & offset_index, const RowRanges & rowRanges);

/// Used in UTs
ReadRanges calculateReadRanges(
    int64_t rowGroupRowCount,
    const arrow::io::ReadRange & chunk_range,
    const std::vector<parquet::PageLocation> & page_locations,
    const RowRanges & rowRanges);

ReadSequence calculateReadSequence(
    int64_t rowGroupRowCount,
    const std::vector<parquet::PageLocation> & page_locations,
    const RowRanges & rowRanges);

}
}
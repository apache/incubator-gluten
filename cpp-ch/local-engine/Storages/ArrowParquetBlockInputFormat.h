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

#include "config.h"

#if USE_PARQUET
#include <Common/ChunkBuffer.h>
#include "ch_parquet/OptimizedArrowColumnToCHColumn.h"
#include "ch_parquet/OptimizedParquetBlockInputFormat.h"
#include "ch_parquet/arrow/reader.h"

namespace arrow
{
class RecordBatchReader;
class Table;
}

namespace local_engine
{
class ArrowParquetBlockInputFormat : public DB::OptimizedParquetBlockInputFormat
{
public:
    ArrowParquetBlockInputFormat(
        DB::ReadBuffer & in,
        const DB::Block & header,
        const DB::FormatSettings & formatSettings,
        const std::vector<int> & row_group_indices_ = {});

private:
    DB::Chunk generate() override;

    int64_t convert_time = 0;
    int64_t non_convert_time = 0;
    std::shared_ptr<arrow::RecordBatchReader> current_record_batch_reader;
    std::vector<int> row_group_indices;
};

}

#endif

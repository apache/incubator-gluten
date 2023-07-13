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
#    include <memory>
#    include <IO/ReadBuffer.h>
#    include <Storages/SubstraitSource/FormatFile.h>

// clang-format off
#include <memory>
#include <IO/ReadBuffer.h>
#include <parquet/metadata.h>
#include <parquet/statistics.h>
#include <Storages/SubstraitSource/FormatFile.h>
// clang-format on
>>>>>>> support push down filter
namespace local_engine
{
struct RowGroupInfomation
{
    UInt32 index = 0;
    UInt64 start = 0;
    UInt64 total_compressed_size = 0;
    UInt64 total_size = 0;
    UInt64 num_rows = 0;
};
class ParquetFormatFile : public FormatFile
{
public:
    explicit ParquetFormatFile(
        DB::ContextPtr context_, const substrait::ReadRel::LocalFiles::FileOrFiles & file_info_, ReadBufferBuilderPtr read_buffer_builder_);
    ~ParquetFormatFile() override = default;

    FormatFile::InputFormatPtr createInputFormat(const DB::Block & header) override;

    std::optional<size_t> getTotalRows() override;

    bool supportSplit() const override { return true; }

private:
    std::mutex mutex;
    std::optional<size_t> total_rows;
    std::vector<RowGroupInfomation> collectRequiredRowGroups(int & total_row_groups);
    std::vector<RowGroupInfomation> collectRequiredRowGroups(DB::ReadBuffer * read_buffer, int & total_row_groups);

    bool checkRowGroupIfNeed(std::unique_ptr<parquet::RowGroupMetaData> meta, std::vector<std::string> & columns, std::vector<DB::DataTypePtr> & column_types)
    {
        std::vector<DB::Range> column_max_mins;
        const parquet::SchemaDescriptor* schema_desc = meta->schema();
        int num_columns = meta->num_columns();
        for (int i=0; i < num_columns; ++i)
        {
            auto column_chunk_meta = meta->ColumnChunk(i);
            const parquet::ColumnDescriptor * desc = schema_desc->Column(i);
            bool column_need = false;
            for (size_t j = 0; j < columns.size(); ++j)
            {
                if (columns[j] == desc->name())
                {
                    column_need = true;
                    break;
                }
            }
            if (column_need && column_chunk_meta->is_stats_set())
            {
                auto stats = std::dynamic_pointer_cast<parquet::Int32Statistics>(column_chunk_meta->statistics());
                if (stats && stats->HasMinMax())
                {
                    Int32 min_val = stats->min();
                    Int32 max_val = stats->max();
                    std::cout << "min_val:" << min_val << " max_val:" << max_val << std::endl;
                    DB::Range range(min_val, true, max_val, true);
                    column_max_mins.push_back(range);
                }
            }
        }
        bool row_group_need = true;
        for (size_t j=0; j < filters.size(); ++j)
        {
            if (column_max_mins.size() > 0 && !filters[j].checkInHyperrectangle(column_max_mins, column_types).can_be_true)
            {
                row_group_need = false;
                break;
            }
        }
        
        return row_group_need;
    }
};

}
#endif

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
#    include <parquet/metadata.h>
#    include <parquet/statistics.h>
#    include <DataTypes/DataTypesNumber.h>
#    include <DataTypes/DataTypeString.h>
#    include <DataTypes/DataTypeFixedString.h>

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
    DB::String getFileFormat() const override { return "parquet"; }

private:
    std::mutex mutex;
    std::optional<size_t> total_rows;
    bool enable_row_group_maxmin_index;
    std::vector<RowGroupInfomation> collectRequiredRowGroups(int & total_row_groups);
    std::vector<RowGroupInfomation> collectRequiredRowGroups(DB::ReadBuffer * read_buffer, int & total_row_groups);
    bool checkRowGroupIfRequired(parquet::RowGroupMetaData & meta);
    DB::Range getColumnMaxMin(std::shared_ptr<parquet::Statistics> statistics,
                    parquet::Type::type parquet_data_type,
                    DB::DataTypePtr data_type,
                    Int32 column_type_length);
};

}
#endif

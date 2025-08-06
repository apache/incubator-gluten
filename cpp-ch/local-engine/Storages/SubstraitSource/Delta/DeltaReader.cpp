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
#include "DeltaReader.h"

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Storages/Parquet/ParquetMeta.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}
}

using namespace DB;

namespace local_engine::delta
{

std::unique_ptr<DeltaReader> DeltaReader::create(
    const FormatFilePtr & file_,
    const Block & to_read_header_,
    const Block & output_header_,
    const FormatFile::InputFormatPtr & input_format_,
    const String & row_index_ids_encoded,
    const String & row_index_filter_type)
{
    std::shared_ptr<DeltaVirtualMeta::DeltaDVBitmapConfig> bitmap_config_;
    if (!row_index_ids_encoded.empty() && !row_index_filter_type.empty())
    {
        if (row_index_filter_type != DeltaVirtualMeta::DeltaDVBitmapConfig::DELTA_ROW_INDEX_FILTER_TYPE_IF_CONTAINED)
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Row index filter type does not support : {}", row_index_filter_type);

        bitmap_config_ = DeltaVirtualMeta::DeltaDVBitmapConfig::parse_config(row_index_ids_encoded);
    }
    return std::make_unique<DeltaReader>(file_, to_read_header_, output_header_, input_format_, bitmap_config_);
}

DeltaReader::DeltaReader(
    const FormatFilePtr & file_,
    const Block & to_read_header_,
    const Block & output_header_,
    const FormatFile::InputFormatPtr & input_format_,
    const std::shared_ptr<DeltaVirtualMeta::DeltaDVBitmapConfig> & bitmap_config_)
    : NormalFileReader(file_, to_read_header_, output_header_, input_format_)
    , bitmap_config(bitmap_config_)
{
    if (bitmap_config)
    {
        bitmap_array = std::make_unique<DeltaDVRoaringBitmapArray>();
        bitmap_array->rb_read(bitmap_config->path_or_inline_dv, bitmap_config->offset, bitmap_config->size_in_bytes, file->getContext());
    }
}

Chunk DeltaReader::doPull()
{
    while (true)
    {
        Chunk chunk = NormalFileReader::doPull();
        if (chunk.getNumRows() == 0)
            return chunk;

        deleteRowsByDV(chunk);

        if (chunk.getNumRows() != 0)
            return chunk;
    }
}

void DeltaReader::deleteRowsByDV(Chunk & chunk) const
{
    size_t num_rows = chunk.getNumRows();
    size_t deleted_row_pos = readHeader.getPositionByName(DeltaVirtualMeta::DELTA_INTERNAL_IS_ROW_DELETED);
    DB::DataTypePtr deleted_row_type = DeltaVirtualMeta::getMetaColumnType(readHeader);
    auto deleted_row_column_nest = DB::ColumnUInt8::create(num_rows);
    auto & vec = deleted_row_column_nest->getData();

    if (bitmap_array)
    {
        size_t tmp_row_id_pos_output = readHeader.getPositionByName(ParquetVirtualMeta::TMP_ROWINDEX);
        size_t tmp_row_id_pos = chunk.getNumColumns() - 1;
        if (tmp_row_id_pos_output < tmp_row_id_pos)
            tmp_row_id_pos = tmp_row_id_pos_output;
        for (int i = 0; i < num_rows; i++)
            vec[i] = bitmap_array->rb_contains(chunk.getColumns()[tmp_row_id_pos]->get64(i));
    }
    else
    {
        // the bitmap array is null, set the values of the '__delta_internal_is_row_deleted' column to the false
        for (int i = 0; i < num_rows; i++)
            vec[i] = false;
    }

    DB::ColumnPtr deleted_row_column;
    if (deleted_row_type->isNullable())
    {
        auto nullmap_column = DB::ColumnUInt8::create(deleted_row_column_nest->size(), 0);
        deleted_row_column = DB::ColumnNullable::create(std::move(deleted_row_column_nest), std::move(nullmap_column));
    }
    else
        deleted_row_column = std::move(deleted_row_column_nest);

    if (deleted_row_pos < chunk.getNumColumns())
        chunk.addColumn(deleted_row_pos, std::move(deleted_row_column));
    else
        chunk.addColumn(std::move(deleted_row_column));
}

}
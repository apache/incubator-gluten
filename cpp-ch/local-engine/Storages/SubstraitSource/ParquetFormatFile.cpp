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
#include "ParquetFormatFile.h"

#if USE_PARQUET

#    include <memory>
#    include <numeric>
#    include <utility>

#    include <Formats/FormatFactory.h>
#    include <Formats/FormatSettings.h>
#    include <IO/SeekableReadBuffer.h>
#    include <Processors/Formats/Impl/ArrowBufferedStreams.h>
#    include <Processors/Formats/Impl/ArrowColumnToCHColumn.h>
#    include <Processors/Formats/Impl/ParquetBlockInputFormat.h>
#    include <Storages/ArrowParquetBlockInputFormat.h>
#    include <Storages/SubstraitSource/SubstraitFileSourceStep.h>
#    include <parquet/arrow/reader.h>
#    include <parquet/metadata.h>
#    include <Common/Config.h>
#    include <Common/Exception.h>
#    include <DataTypes/DataTypesNumber.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_TYPE;
}
}

namespace local_engine
{
ParquetFormatFile::ParquetFormatFile(
    DB::ContextPtr context_, const substrait::ReadRel::LocalFiles::FileOrFiles & file_info_, ReadBufferBuilderPtr read_buffer_builder_)
    : FormatFile(context_, file_info_, read_buffer_builder_), enable_row_group_maxmin_index(file_info_.parquet().enable_row_group_maxmin_index()) 
{
}

FormatFile::InputFormatPtr ParquetFormatFile::createInputFormat(const DB::Block & header)
{
    auto res = std::make_shared<FormatFile::InputFormat>();
    res->read_buffer = read_buffer_builder->build(file_info);

    std::vector<RowGroupInfomation> required_row_groups;
    [[maybe_unused]] int total_row_groups = 0;
    if (auto * seekable_in = dynamic_cast<DB::SeekableReadBuffer *>(res->read_buffer.get()))
    {
        // reuse the read_buffer to avoid opening the file twice.
        // especially，the cost of opening a hdfs file is large.
        required_row_groups = collectRequiredRowGroups(seekable_in, total_row_groups);
        seekable_in->seek(0, SEEK_SET);
    }
    else
        required_row_groups = collectRequiredRowGroups(total_row_groups);

    auto format_settings = DB::getFormatSettings(context);
#    if USE_LOCAL_FORMATS
    format_settings.parquet.import_nested = true;

    std::vector<int> row_group_indices;
    row_group_indices.reserve(required_row_groups.size());
    for (const auto & row_group : required_row_groups)
        row_group_indices.emplace_back(row_group.index);

    auto input_format
        = std::make_shared<local_engine::ArrowParquetBlockInputFormat>(*(res->read_buffer), header, format_settings, row_group_indices);
#    else
    std::vector<int> total_row_group_indices(total_row_groups);
    std::iota(total_row_group_indices.begin(), total_row_group_indices.end(), 0);

    std::vector<int> required_row_group_indices(required_row_groups.size());
    for (size_t i = 0; i < required_row_groups.size(); ++i)
        required_row_group_indices[i] = required_row_groups[i].index;

    std::vector<int> skip_row_group_indices;
    std::set_difference(
        total_row_group_indices.begin(),
        total_row_group_indices.end(),
        required_row_group_indices.begin(),
        required_row_group_indices.end(),
        std::back_inserter(skip_row_group_indices));

    format_settings.parquet.skip_row_groups = std::unordered_set<int>(skip_row_group_indices.begin(), skip_row_group_indices.end());
    auto input_format = std::make_shared<DB::ParquetBlockInputFormat>(*(res->read_buffer), header, format_settings, 1, 8192);
#    endif
    res->input = input_format;
    return res;
}

std::optional<size_t> ParquetFormatFile::getTotalRows()
{
    {
        std::lock_guard lock(mutex);
        if (total_rows)
            return total_rows;
    }

    int _;
    auto rowgroups = collectRequiredRowGroups(_);
    size_t rows = 0;
    for (const auto & rowgroup : rowgroups)
        rows += rowgroup.num_rows;

    {
        std::lock_guard lock(mutex);
        total_rows = rows;
        return total_rows;
    }
}

std::vector<RowGroupInfomation> ParquetFormatFile::collectRequiredRowGroups(int & total_row_groups)
{
    auto in = read_buffer_builder->build(file_info);
    return collectRequiredRowGroups(in.get(), total_row_groups);
}

std::vector<RowGroupInfomation> ParquetFormatFile::collectRequiredRowGroups(DB::ReadBuffer * read_buffer, int & total_row_groups)
{
    DB::FormatSettings format_settings{
        .seekable_read = true,
    };
    std::atomic<int> is_stopped{0};
    std::unique_ptr<parquet::arrow::FileReader> reader;
    auto status = parquet::arrow::OpenFile(
        asArrowFile(*read_buffer, format_settings, is_stopped, "Parquet", PARQUET_MAGIC_BYTES), arrow::default_memory_pool(), &reader);
    if (!status.ok())
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Open file({}) failed. {}", file_info.uri_file(), status.ToString());

    auto file_meta = reader->parquet_reader()->metadata();
    total_row_groups = file_meta->num_row_groups();

    std::vector<RowGroupInfomation> row_group_metadatas;
    row_group_metadatas.reserve(total_row_groups);

    auto get_column_start_offset = [&](parquet::ColumnChunkMetaData & metadata_) {
        Int64 offset = metadata_.data_page_offset();
        if (metadata_.has_dictionary_page() && offset > metadata_.dictionary_page_offset())
        {
            offset = metadata_.dictionary_page_offset();
        }
        return offset;
    };

    for (int i = 0; i < total_row_groups; ++i)
    {
        auto row_group_meta = file_meta->RowGroup(i);
        Int64 start_offset = 0;
        Int64 total_bytes = 0;
        start_offset = get_column_start_offset(*row_group_meta->ColumnChunk(0));
        total_bytes = row_group_meta->total_compressed_size();
        if (!total_bytes)
        {
            for (int j = 0; j < row_group_meta->num_columns(); ++j)
            {
                total_bytes += row_group_meta->ColumnChunk(j)->total_compressed_size();
            }
        }

        if (enable_row_group_maxmin_index && !checkRowGroupIfRequired(*row_group_meta))
            continue;

        UInt64 midpoint_offset = static_cast<UInt64>(start_offset + total_bytes / 2);        
        /// Current row group has intersection with the required range.
        if (file_info.start() <= midpoint_offset && midpoint_offset < file_info.start() + file_info.length())
        {
            RowGroupInfomation info;
            info.index = i;
            info.num_rows = row_group_meta->num_rows();
            info.start = row_group_meta->file_offset();
            info.total_compressed_size = row_group_meta->total_compressed_size();
            info.total_size = row_group_meta->total_byte_size();
            row_group_metadatas.emplace_back(std::move(info));
        }
    }
    return row_group_metadatas;
}

bool ParquetFormatFile::checkRowGroupIfRequired(parquet::RowGroupMetaData & meta)
{
    std::vector<DB::Range> column_max_mins;
    DB::DataTypes column_types;
    const parquet::SchemaDescriptor* schema_desc = meta.schema();
    bool row_group_required = true;
    for (size_t i = 0; i < filters.size(); ++i)
    {
        std::vector<DB::Range> ranges;
        auto iter = filters[i].keys.begin();

        for (size_t j = 0; j < filters[i].keys.size(); ++j)
        {
            DB::String filter_col_key = iter->name;
            DB::DataTypePtr filter_col_type = iter->type;
            int column_index = schema_desc->ColumnIndex(filter_col_key);
            DB::Range range = DB::Range::createWholeUniverse();
            if (column_index < 0)
                ranges.emplace_back(std::move(range));
            else
            {
                const parquet::ColumnDescriptor * desc = schema_desc->Column(column_index);
                Int32 column_type_length = desc->type_length();
                auto column_chunk_meta = meta.ColumnChunk(column_index);
                if (column_chunk_meta->is_stats_set())
                    range = getColumnMaxMin(column_chunk_meta->statistics(), column_chunk_meta->type(), filter_col_type, column_type_length);
                
                 ranges.emplace_back(std::move(range));
            }

            ++iter;
        }
        if (ranges.size() > 0 && !filters[i].filter.checkInHyperrectangle(ranges, filters[i].keys.getTypes()).can_be_true)
        {
            row_group_required = false;
            break;
        }
    }
    return row_group_required;
}

DB::Range ParquetFormatFile::getColumnMaxMin(
    std::shared_ptr<parquet::Statistics> statistics,
    parquet::Type::type parquet_data_type,
    DB::DataTypePtr data_type,
    Int32 column_type_length)
{
    DB::Range maxmin = DB::Range::createWholeUniverse();
    DB::WhichDataType which_data_type(data_type);
    if (parquet_data_type == parquet::Type::type::FLOAT)
    {
        auto float_stats = std::dynamic_pointer_cast<parquet::FloatStatistics>(statistics);
        maxmin = float_stats->HasMinMax() && (!float_stats->HasNullCount() || float_stats->null_count() == 0) ?
            DB::Range(float_stats->min(), true, float_stats->max(), true) : DB::Range::createWholeUniverse();
    }
    else if (parquet_data_type == parquet::Type::type::DOUBLE)
    {
        auto double_stats = std::dynamic_pointer_cast<parquet::DoubleStatistics>(statistics);
        maxmin = double_stats->HasMinMax() && (!double_stats->HasNullCount() || double_stats->null_count() == 0) ?
            DB::Range(double_stats->min(), true, double_stats->max(), true) : DB::Range::createWholeUniverse();
            
    }
    else if (parquet_data_type == parquet::Type::type::INT32)
    {
        auto int32_stats = std::dynamic_pointer_cast<parquet::Int32Statistics>(statistics);
        maxmin = int32_stats->HasMinMax() && (!int32_stats->HasNullCount() || int32_stats->null_count() == 0) ?
            DB::Range(int32_stats->min(), true, int32_stats->max(), true) : DB::Range::createWholeUniverse();
            
    }
    else if (parquet_data_type == parquet::Type::type::INT64)
    {
        auto int64_stats = statistics ? std::dynamic_pointer_cast<parquet::Int64Statistics>(statistics) : nullptr;
        maxmin = int64_stats && int64_stats->HasMinMax() && (!int64_stats->HasNullCount() || int64_stats->null_count() == 0) ?
            DB::Range(int64_stats->min(), true, int64_stats->max(), true) : DB::Range::createWholeUniverse();
    }
    else if (parquet_data_type == parquet::Type::type::BOOLEAN)
    {
        auto bool_stats = std::dynamic_pointer_cast<parquet::BoolStatistics>(statistics);
        maxmin = bool_stats->HasMinMax() && (!bool_stats->HasNullCount() || bool_stats->null_count() == 0) ?
            DB::Range(bool_stats->min(), true, bool_stats->max(), true) : DB::Range::createWholeUniverse();
    }
    else if (parquet_data_type == parquet::Type::type::BYTE_ARRAY)
    {
        auto byte_array_stats = std::dynamic_pointer_cast<parquet::ByteArrayStatistics>(statistics) ;
        maxmin = byte_array_stats->HasMinMax() && (!byte_array_stats->HasNullCount() || byte_array_stats->null_count() == 0) ?
            DB::Range(parquet::ByteArrayToString(byte_array_stats->min()), true, parquet::ByteArrayToString(byte_array_stats->max()), true) :
            DB::Range::createWholeUniverse();
    }
    else if (parquet_data_type == parquet::Type::type::FIXED_LEN_BYTE_ARRAY)
    {
        if (which_data_type.isFixedString())
        {
            auto fixed_byte_array_stats = std::dynamic_pointer_cast<parquet::FLBAStatistics>(statistics) ;
            maxmin = fixed_byte_array_stats->HasMinMax() && (!fixed_byte_array_stats->HasNullCount() || fixed_byte_array_stats->null_count() == 0) ?
                DB::Range(parquet::FixedLenByteArrayToString(fixed_byte_array_stats->min(), column_type_length), true,
                    parquet::FixedLenByteArrayToString(fixed_byte_array_stats->max(), column_type_length), true) :
                DB::Range::createWholeUniverse();
        }
    }

    return maxmin;
}


}
#endif

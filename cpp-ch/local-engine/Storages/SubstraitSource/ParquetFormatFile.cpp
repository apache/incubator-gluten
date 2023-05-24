#include "ParquetFormatFile.h"
// clang-format off
#if USE_PARQUET

#include <memory>
#include <string>
#include <utility>

#include <parquet/arrow/reader.h>
#include <Common/Config.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatSettings.h>
#include <IO/SeekableReadBuffer.h>
#include <Storages/ArrowParquetBlockInputFormat.h>
#include <Processors/Formats/Impl/ArrowBufferedStreams.h>
#include <Processors/Formats/Impl/ParquetBlockInputFormat.h>
#include <Processors/Formats/Impl/ArrowColumnToCHColumn.h>

// clang-format on
namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}
}

namespace local_engine
{
ParquetFormatFile::ParquetFormatFile(
    DB::ContextPtr context_, const substrait::ReadRel::LocalFiles::FileOrFiles & file_info_, ReadBufferBuilderPtr read_buffer_builder_)
    : FormatFile(context_, file_info_, read_buffer_builder_)
{
}

FormatFile::InputFormatPtr ParquetFormatFile::createInputFormat(const DB::Block & header)
{
    auto res = std::make_shared<FormatFile::InputFormat>();
    res->read_buffer = std::move(read_buffer_builder->build(file_info));

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
// clang-format off
#if USE_LOCAL_FORMATS
    // clang-format on
    format_settings.parquet.import_nested = true;

    std::vector<int> row_group_indices;
    row_group_indices.reserve(required_row_groups.size());
    for (const auto & row_group : required_row_groups)
        row_group_indices.emplace_back(row_group.index);

    auto input_format
        = std::make_shared<local_engine::ArrowParquetBlockInputFormat>(res->read_buffer.get(), header, format_settings, row_group_indices);
// clang-format off
#else
    // clang-format on
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
    auto input_format = std::make_shared<DB::ParquetBlockInputFormat>(res->read_buffer.get(), nullptr, header, format_settings, 1, 8192);
// clang-format off
#endif
    // clang-format on
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
    for (int i = 0; i < total_row_groups; ++i)
    {
        auto row_group_meta = file_meta->RowGroup(i);

        auto offset = static_cast<UInt64>(row_group_meta->file_offset());
        if (!offset)
            offset = static_cast<UInt64>(row_group_meta->ColumnChunk(0)->file_offset());

        /// Current row group has intersection with the required range.
        if (file_info.start() <= offset && offset < file_info.start() + file_info.length())
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
}
#endif

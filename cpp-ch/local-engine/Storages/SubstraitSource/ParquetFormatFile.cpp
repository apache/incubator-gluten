#include <memory>
#include <string>
#include <utility>
#include <Formats/FormatFactory.h>
#include <Formats/FormatSettings.h>
#include <IO/SeekableReadBuffer.h>
#include <Processors/Formats/Impl/ArrowBufferedStreams.h>
#include <Storages/ArrowParquetBlockInputFormat.h>
#include <Storages/SubstraitSource/ParquetFormatFile.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}
}

namespace local_engine
{
ParquetFormatFile::ParquetFormatFile(DB::ContextPtr context_, const substrait::ReadRel::LocalFiles::FileOrFiles & file_info_, ReadBufferBuilderPtr read_buffer_builder_)
    : FormatFile(context_, file_info_, read_buffer_builder_)
{
}

FormatFile::InputFormatPtr ParquetFormatFile::createInputFormat(const DB::Block & header)
{
    auto res = std::make_shared<FormatFile::InputFormat>();
    res->read_buffer = std::move(read_buffer_builder->build(file_info));
    std::vector<int> row_group_indices;
    std::vector<RowGroupInfomation> required_row_groups;
    if (auto * seekable_in = dynamic_cast<DB::SeekableReadBuffer *>(res->read_buffer.get()))
    {
        // reuse the read_buffer to avoid opening the file twice.
        // especially，the cost of opening a hdfs file is large.
        required_row_groups = collectRequiredRowGroups(seekable_in);
        seekable_in->seek(0, SEEK_SET);
    }
    else
    {
        required_row_groups = collectRequiredRowGroups();
    }
    for (const auto & row_group : required_row_groups)
    {
        row_group_indices.emplace_back(row_group.index);
    }
    auto format_settings = DB::getFormatSettings(context);
    format_settings.parquet.import_nested = true;
    auto input_format = std::make_shared<local_engine::ArrowParquetBlockInputFormat>(*(res->read_buffer), header, format_settings, row_group_indices);
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
    auto rowgroups = collectRequiredRowGroups();
    size_t rows = 0;
    for (const auto & rowgroup : rowgroups)
    {
        rows += rowgroup.num_rows;
    }
    {
        std::lock_guard lock(mutex);
        total_rows = rows;
        return total_rows;
    }
}

std::vector<RowGroupInfomation> ParquetFormatFile::collectRequiredRowGroups()
{
    auto in = read_buffer_builder->build(file_info);
    return collectRequiredRowGroups(in.get());
}

std::vector<RowGroupInfomation> ParquetFormatFile::collectRequiredRowGroups(DB::ReadBuffer * read_buffer)
{
    std::unique_ptr<parquet::arrow::FileReader> reader;
    DB::FormatSettings format_settings;
    format_settings.seekable_read = true;
    std::atomic<int> is_stopped{0};
    auto status = parquet::arrow::OpenFile(
        asArrowFile(*read_buffer, format_settings, is_stopped, "Parquet", PARQUET_MAGIC_BYTES), arrow::default_memory_pool(), &reader);
    if (!status.ok())
    {
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Open file({}) failed. {}", file_info.uri_file(), status.ToString());
    }

    auto file_meta = reader->parquet_reader()->metadata();
    std::vector<RowGroupInfomation> row_group_metadatas;
    for (int i = 0, n = file_meta->num_row_groups(); i < n; ++i)
    {
        auto row_group_meta = file_meta->RowGroup(i);
        auto offset = static_cast<UInt64>(row_group_meta->file_offset());
        if (!offset)
        {
            offset = static_cast<UInt64>(row_group_meta->ColumnChunk(0)->file_offset());
        }
        if (file_info.start() <=  offset && offset < file_info.start() + file_info.length())
        {
            RowGroupInfomation info;
            info.index = i;
            info.num_rows = row_group_meta->num_rows();
            info.start = row_group_meta->file_offset();
            info.total_compressed_size = row_group_meta->total_compressed_size();
            info.total_size = row_group_meta->total_byte_size();
            row_group_metadatas.emplace_back(info);
        }
    }
    return row_group_metadatas;

}
}

#pragma once
#include <memory>
#include <Storages/SubstraitSource/FormatFile.h>
#include <parquet/arrow/reader.h>
#include <IO/ReadBuffer.h>
#include <base/types.h>
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
    explicit ParquetFormatFile(DB::ContextPtr context_, const substrait::ReadRel::LocalFiles::FileOrFiles & file_info_, ReadBufferBuilderPtr read_buffer_builder_);
    ~ParquetFormatFile() override = default;
    FormatFile::InputFormatPtr createInputFormat(const DB::Block & header) override;
    std::optional<size_t> getTotalRows() override;
    bool supportSplit() override { return true; }

private:
    std::mutex mutex;
    std::optional<size_t> total_rows;

    std::vector<RowGroupInfomation> collectRequiredRowGroups();
    std::vector<RowGroupInfomation> collectRequiredRowGroups(DB::ReadBuffer * read_buffer);
};

}

#pragma once
#include <config.h>
#include <Common/Config.h>
// clang-format off
#if USE_ORC
#include <memory>
#include <IO/ReadBuffer.h>
#include <Interpreters/Context.h>
#include <Storages/SubstraitSource/FormatFile.h>
#include <base/types.h>

#include <Processors/Formats/Impl/ArrowColumnToCHColumn.h>

// clang-format on
namespace local_engine
{
struct StripeInformation
{
    UInt64 index;
    UInt64 offset;
    UInt64 length;
    UInt64 num_rows;
    UInt64 start_row;
};

class OrcFormatFile : public FormatFile
{
public:
    explicit OrcFormatFile(
        DB::ContextPtr context_, const substrait::ReadRel::LocalFiles::FileOrFiles & file_info_, ReadBufferBuilderPtr read_buffer_builder_);
    ~OrcFormatFile() override = default;
    FormatFile::InputFormatPtr createInputFormat(const DB::Block & header, bool) override;
    std::optional<size_t> getTotalRows() override;

    bool supportSplit() override { return true; }

private:
    std::mutex mutex;
    std::optional<size_t> total_rows;

    std::vector<StripeInformation> collectRequiredStripes(UInt64 & total_stripes);
    std::vector<StripeInformation> collectRequiredStripes(DB::ReadBuffer * read_buffer, UInt64 & total_strpes);
};
}

#endif

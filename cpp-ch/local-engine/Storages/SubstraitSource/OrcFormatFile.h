#pragma once
#include <memory>
#include <IO/ReadBuffer.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/Formats/IInputFormat.h>
#include <Storages/SubstraitSource/FormatFile.h>
#include <arrow/adapters/orc/adapter.h>
#include <base/types.h>
#include <Processors/Formats/Impl/ArrowColumnToCHColumn.h>
#include <Storages/ch_parquet/OptimizedArrowColumnToCHColumn.h>
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

// local engine's orc block input formatter
// the behavior of generate is different from DB::ORCBlockInputFormat
class ORCBlockInputFormat : public DB::IInputFormat
{
public:
    explicit ORCBlockInputFormat(
        DB::ReadBuffer & in_,
        DB::Block header_,
        const DB::FormatSettings & format_settings_,
        const std::vector<StripeInformation> & stripes_);

    String getName() const override { return "LocalEngineORCBlockInputFormat"; }

    void resetParser() override;

    const DB::BlockMissingValues & getMissingValues() const override { return block_missing_values; }

protected:
    DB::Chunk generate() override;

    void onCancel() override { is_stopped = 1; }
private:

    // TODO: check that this class implements every part of its parent

    std::unique_ptr<arrow::adapters::orc::ORCFileReader> file_reader;

    std::unique_ptr<DB::OptimizedArrowColumnToCHColumn> arrow_column_to_ch_column;

    // indices of columns to read from ORC file
    std::vector<int> include_indices;
    std::vector<String> include_column_names;

    std::vector<size_t> missing_columns;
    DB::BlockMissingValues block_missing_values;

    const DB::FormatSettings format_settings;

    std::vector<StripeInformation> stripes;
    UInt64 current_stripe = 0;

    std::atomic<int> is_stopped{0};

    void prepareReader();

    std::shared_ptr<arrow::RecordBatchReader> stepOneStripe();

    std::shared_ptr<arrow::RecordBatchReader> fetchNextStripe();
};

class OrcFormatFile : public FormatFile
{
public:

    explicit OrcFormatFile(
        DB::ContextPtr context_, const substrait::ReadRel::LocalFiles::FileOrFiles & file_info_, ReadBufferBuilderPtr read_buffer_builder_);
    ~OrcFormatFile() override = default;
    FormatFile::InputFormatPtr createInputFormat(const DB::Block & header) override;
    std::optional<size_t> getTotalRows() override;

    bool supportSplit() override { return true; }

private:
    std::mutex mutex;
    std::optional<size_t> total_rows;

    std::vector<StripeInformation> collectRequiredStripes();
    std::vector<StripeInformation> collectRequiredStripes(DB::ReadBuffer * read_buffer);
};
}

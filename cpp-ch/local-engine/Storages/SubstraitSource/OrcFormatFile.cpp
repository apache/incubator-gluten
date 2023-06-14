#include "OrcFormatFile.h"
// clang-format off
#if USE_ORC
#include <memory>
#include <Formats/FormatFactory.h>
#include <IO/SeekableReadBuffer.h>
#include <Processors/Formats/Impl/ArrowBufferedStreams.h>
#include <Storages/SubstraitSource/OrcUtil.h>

namespace local_engine
{

OrcFormatFile::OrcFormatFile(
    DB::ContextPtr context_, const substrait::ReadRel::LocalFiles::FileOrFiles & file_info_, ReadBufferBuilderPtr read_buffer_builder_)
    : FormatFile(context_, file_info_, read_buffer_builder_)
{
}

FormatFile::InputFormatPtr OrcFormatFile::createInputFormat(const DB::Block & header, bool-v vb)
{
    auto file_format = std::make_shared<FormatFile::InputFormat>();
    file_format->read_buffer = read_buffer_builder->build(file_info);

    std::vector<StripeInformation> stripes;
    [[maybe_unused]] UInt64 total_stripes = 0;
    if (auto * seekable_in = dynamic_cast<DB::SeekableReadBuffer *>(file_format->read_buffer.get()))
    {
        stripes = collectRequiredStripes(seekable_in, total_stripes);
        seekable_in->seek(0, SEEK_SET);
    }
    else
        stripes = collectRequiredStripes(total_stripes);

    auto format_settings = DB::getFormatSettings(context);

    std::vector<int> total_stripe_indices(total_stripes);
    std::iota(total_stripe_indices.begin(), total_stripe_indices.end(), 0);

    std::vector<UInt64> required_stripe_indices(stripes.size());
    for (size_t i = 0; i < stripes.size(); ++i)
        required_stripe_indices[i] = stripes[i].index;

    std::vector<int> skip_stripe_indices;
    std::set_difference(
        total_stripe_indices.begin(),
        total_stripe_indices.end(),
        required_stripe_indices.begin(),
        required_stripe_indices.end(),
        std::back_inserter(skip_stripe_indices));

    format_settings.orc.skip_stripes = std::unordered_set<int>(skip_stripe_indices.begin(), skip_stripe_indices.end());
    auto input_format = std::make_shared<DB::ORCBlockInputFormat>(*file_format->read_buffer, header, format_settings);
    file_format->input = input_format;
    return file_format;
}

std::optional<size_t> OrcFormatFile::getTotalRows()
{
    {
        std::lock_guard lock(mutex);
        if (total_rows)
            return total_rows;
    }

    UInt64 _;
    auto required_stripes = collectRequiredStripes(_);
    {
        std::lock_guard lock(mutex);
        if (total_rows)
            return total_rows;
        size_t num_rows = 0;
        for (const auto stipe_info : required_stripes)
        {
            num_rows += stipe_info.num_rows;
        }
        total_rows = num_rows;
        return total_rows;
    }
}

std::vector<StripeInformation> OrcFormatFile::collectRequiredStripes(UInt64 & total_stripes)
{
    auto in = read_buffer_builder->build(file_info);
    return collectRequiredStripes(in.get(), total_stripes);
}

std::vector<StripeInformation> OrcFormatFile::collectRequiredStripes(DB::ReadBuffer * read_buffer, UInt64 & total_stripes)
{
    DB::FormatSettings format_settings{
        .seekable_read = true,
    };
    std::atomic<int> is_stopped{0};
    auto arrow_file = DB::asArrowFile(*read_buffer, format_settings, is_stopped, "ORC", ORC_MAGIC_BYTES);
    auto orc_reader = OrcUtil::createOrcReader(arrow_file);
    total_stripes = orc_reader->getNumberOfStripes();

    size_t total_num_rows = 0;
    std::vector<StripeInformation> stripes;
    stripes.reserve(total_stripes);
    for (size_t i = 0; i < total_stripes; ++i)
    {
        auto stripe_metadata = orc_reader->getStripe(i);

        auto offset = stripe_metadata->getOffset();
        if (file_info.start() <= offset && offset < file_info.start() + file_info.length())
        {
            StripeInformation stripe_info;
            stripe_info.index = i;
            stripe_info.offset = stripe_metadata->getLength();
            stripe_info.length = stripe_metadata->getLength();
            stripe_info.num_rows = stripe_metadata->getNumberOfRows();
            stripe_info.start_row = total_num_rows;
            stripes.emplace_back(stripe_info);
        }

        total_num_rows += stripe_metadata->getNumberOfRows();
    }
    return stripes;
}
}

#endif

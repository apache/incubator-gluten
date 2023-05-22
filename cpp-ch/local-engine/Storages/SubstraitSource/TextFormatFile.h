#pragma once

#include "config.h"
#include <memory>
#include <Columns/IColumn.h>
#include <IO/ReadBuffer.h>
#include <IO/PeekableReadBuffer.h>
#include <Storages/SubstraitSource/FormatFile.h>
#include <Processors/Formats/IRowInputFormat.h>
#include <Processors/Formats/Impl/CSVRowInputFormat.h>

namespace local_engine
{
class TextFormatFile : public FormatFile
{
public:
    explicit TextFormatFile(DB::ContextPtr context_, const substrait::ReadRel::LocalFiles::FileOrFiles & file_info_, ReadBufferBuilderPtr read_buffer_builder_);
    ~TextFormatFile() override = default;
    FormatFile::InputFormatPtr createInputFormat(const DB::Block & header) override;
    std::optional<size_t> getTotalRows() override { return 1; }
    bool supportSplit() override { return true; }

private:
    DB::FormatSettings createFormatSettings();
};

class TextFormatReader final : public DB::CSVFormatReader
{
public:
    explicit TextFormatReader(DB::PeekableReadBuffer & buf_, std::vector<std::string> input_schema, const DB::FormatSettings & format_settings_);
private:
    std::vector<String> readNames() override;
    std::vector<String> input_schema;
    
};

/// A stream for input data in Text format.
class TextRowInputFormat final : public DB::CSVRowInputFormat
{
public:
    TextRowInputFormat(
        const DB::Block & header_, 
        std::shared_ptr<DB::PeekableReadBuffer> & buf_,
        const DB::RowInputFormatParams & params_, 
        const DB::FormatSettings & format_settings_,
        std::vector<std::string> input_schema
    );

    String getName() const override { return "TextRowInputFormat"; }

protected:
    void readPrefix() override;

private:

    std::vector<std::string> input_schema;
};


}

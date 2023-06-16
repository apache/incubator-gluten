#pragma once


#include <memory>
#include <Columns/IColumn.h>
#include <IO/PeekableReadBuffer.h>
#include <IO/ReadBuffer.h>
#include <Processors/Formats/IRowInputFormat.h>
#include <Processors/Formats/Impl/CSVRowInputFormat.h>
#include <Storages/SubstraitSource/FormatFile.h>
#include "config.h"

namespace local_engine
{
/// Read file from excel export.

class ExcelTextFormatFile : public FormatFile
{
public:
    explicit ExcelTextFormatFile(
        DB::ContextPtr context_, const substrait::ReadRel::LocalFiles::FileOrFiles & file_info_, ReadBufferBuilderPtr read_buffer_builder_)
        : FormatFile(context_, file_info_, read_buffer_builder_){};

    ~ExcelTextFormatFile() override = default;
    FormatFile::InputFormatPtr createInputFormat(const DB::Block & header) override;

private:
    DB::FormatSettings createFormatSettings();
};


class ExcelRowInputFormat final : public DB::CSVRowInputFormat
{
public:
    ExcelRowInputFormat(
        const DB::Block & header_,
        std::shared_ptr<DB::PeekableReadBuffer> & buf_,
        const DB::RowInputFormatParams & params_,
        const DB::FormatSettings & format_settings_,
        DB::Names & input_field_names_,
        String escape_);

    String getName() const { return "ExcelRowInputFormat"; }

private:
    String escape;
};

class ExcelTextFormatReader final : public DB::CSVFormatReader
{
public:
    ExcelTextFormatReader(DB::PeekableReadBuffer & buf_, DB::Names & input_field_names_, const DB::FormatSettings & format_settings_);

    std::vector<String> readNames() override;
    std::vector<String> readTypes() override;
    void skipRowEndDelimiter() override;
    bool readField(DB::IColumn & column, const DB::DataTypePtr & type, const DB::SerializationPtr & serialization, bool is_last_file_column, const String & column_name) override;

private:
    void preSkipNullValue();
    static void skipEndOfLine(DB::ReadBuffer & in);
    static void skipWhitespacesAndTabs(DB::ReadBuffer & in);


    std::vector<String> input_field_names;
};
}

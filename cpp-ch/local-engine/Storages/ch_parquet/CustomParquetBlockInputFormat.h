#pragma once
#include <Processors/Formats/IInputFormat.h>
#include "ParquetFileReader.h"

namespace DB
{

class CustomParquetBlockInputFormat : public IInputFormat
{
public:
    CustomParquetBlockInputFormat(
        ReadBufferFromFileBase * buf,
        const Block & header,
        DB::ActionsDAGPtr pushdown_filter,
        const FormatSettings & format_settings);
    ~CustomParquetBlockInputFormat() override = default;

    void resetParser() override;

    String getName() const override { return "CustomParquetBlockInputFormat"; }

private:
    Chunk generate() override;
    void initializeIfNeeded();
    void onCancel() override
    {
        is_stopped = 1;
    }

    const FormatSettings format_settings;
    std::shared_ptr<ParquetFileReader> file_reader;
    std::atomic<int> is_stopped{0};
    DB::ActionsDAGPtr pushdown_filter;
    bool is_initialized = false;
};

}



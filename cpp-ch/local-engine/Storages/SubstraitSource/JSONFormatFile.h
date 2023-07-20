#pragma once

#include "config.h"
#include <Storages/SubstraitSource/FormatFile.h>

namespace local_engine
{
class JSONFormatFile : public FormatFile
{
public:
    explicit JSONFormatFile(DB::ContextPtr context_, const substrait::ReadRel::LocalFiles::FileOrFiles & file_info_, ReadBufferBuilderPtr read_buffer_builder_);
    ~JSONFormatFile() override = default;

    bool supportSplit() const override { return true; }

    FormatFile::InputFormatPtr createInputFormat(const DB::Block & header) override;

    DB::NamesAndTypesList getSchema() const override;
};
}

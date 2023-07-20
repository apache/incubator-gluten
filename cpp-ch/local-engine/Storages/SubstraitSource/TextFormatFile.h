#pragma once

#include <memory>
#include <Storages/SubstraitSource/FormatFile.h>
#include "config.h"

namespace local_engine
{
class TextFormatFile : public FormatFile
{
public:
    explicit TextFormatFile(
        DB::ContextPtr context_, const substrait::ReadRel::LocalFiles::FileOrFiles & file_info_, ReadBufferBuilderPtr read_buffer_builder_);
    ~TextFormatFile() override = default;

    FormatFile::InputFormatPtr createInputFormat(const DB::Block & header) override;

    DB::NamesAndTypesList getSchema() const override;

    bool supportSplit() const override { return true; }
};

}

#pragma once

#include "config.h"
#include <Storages/SubstraitSource/FormatFile.h>

namespace local_engine
{
class JsonFormatFile : public FormatFile
{
public:
    explicit JsonFormatFile(DB::ContextPtr context_, const substrait::ReadRel::LocalFiles::FileOrFiles & file_info_, ReadBufferBuilderPtr read_buffer_builder_);
    ~JsonFormatFile() override = default;
    FormatFile::InputFormatPtr createInputFormat(const DB::Block & header) override;
    std::optional<size_t> getTotalRows() override  { return 1; }
    bool supportSplit() override { return true; }
};
}

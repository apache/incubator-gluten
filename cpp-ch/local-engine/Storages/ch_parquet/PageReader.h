#pragma once

#include <IO/SeekableReadBuffer.h>
#include <generated/parquet_types.h>
#include <Common/PODArray.h>
#include <Common/PODArray_fwd.h>

namespace DB
{
static constexpr size_t HEADER_INIT_SIZE = 1024;

class PageReader
{
    friend class ParquetColumnChunkReader;
public:
    PageReader(SeekableReadBufferPtr stream, size_t length);
    ~PageReader() = default;

    void nextHeader();

    //
    const parquet::format::PageHeader * currentHeader() const { return &cur_page_header; }

    void readBytes(char * buffer, size_t size);

    void skipBytes(size_t size);

    uint64_t getOffset() const { return offset; }

private:
    // need a new seekable read buffer, shouldn't share read buffer with other page reader
    SeekableReadBufferPtr stream;
    parquet::format::PageHeader cur_page_header;
    PaddedPODArray<char> page_buffer;
    std::unique_ptr<PaddedPODArray<char>> page_data;
    uint64_t offset = 0;
    uint64_t next_header_pos = 0;

    uint64_t finish_offset = 0;
};

} // DB

#include "PageReader.h"

#include <Common/Exception.h>
#include <Common/PODArray.h>
#include "thrift/thrift_util.h"
#include "Utils.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

PageReader::PageReader(SeekableReadBufferPtr stream_, size_t length_) : stream(stream_), finish_offset(length_)
{
}
void PageReader::nextHeader()
{
    if (offset != next_header_pos) [[unlikely]]
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Try to parse parquet column header in wrong position, offset={} vs expect={}",
            offset,
            next_header_pos);
    }
    if (offset >= finish_offset)
    {
        throw EndOfFile();
    }


    size_t header_length = 0;
    size_t nbytes = HEADER_INIT_SIZE;
    size_t remaining = finish_offset - offset;
    nbytes = std::min(nbytes, remaining);
    page_buffer.reserve(nbytes);
    do
    {
        header_length = stream->read(page_buffer.data(), nbytes);
        try
        {
            header_length = deserialize_thrift_msg(reinterpret_cast<uint8_t *>(page_buffer.data()), header_length, TProtocolType::COMPACT, &cur_page_header);
            break;
        }
        catch (DB::Exception &)
        {
            if ((nbytes > 16384) || (offset + nbytes) >= finish_offset) [[unlikely]]
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to decode parquet page header");
            }
        }
        nbytes <<= 2;
        stream->seek(offset, SEEK_SET);
    } while (true);

    offset += header_length;
    stream->seek(offset, SEEK_SET);
    next_header_pos = offset + cur_page_header.compressed_page_size;
}

void PageReader::readBytes(char * buffer, size_t size)
{
    if (offset + size > next_header_pos) [[unlikely]]
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Size to read exceed page size");
    }
//    stream->seek(offset, SEEK_SET);
    size_t nbytes = stream->read(buffer, size);
    chassert(nbytes == size);
    offset += nbytes;
}
void PageReader::skipBytes(size_t size)
{
    if (offset + size > next_header_pos) [[unlikely]]
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Size to read exceed page size");
    }
    offset += size;
}
}

#pragma once

#include <memory>

#include <Common/PODArray.h>

#include <IO/WriteBuffer.h>
#include <IO/BufferWithOwnMemory.h>
#include <Compression/ICompressionCodec.h>
#include <Compression/CompressionFactory.h>


namespace local_engine
{

class CompressedWriteBuffer final : public DB::BufferWithOwnMemory<DB::WriteBuffer>
{
public:
    explicit CompressedWriteBuffer(
        DB::WriteBuffer & out_,
        DB::CompressionCodecPtr codec_ = DB::CompressionCodecFactory::instance().getDefaultCodec(),
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE);

    ~CompressedWriteBuffer() override;

    /// The amount of compressed data
    size_t getCompressedBytes()
    {
        nextIfAtEnd();
        return out.count();
    }

    /// How many uncompressed bytes were written to the buffer
    size_t getUncompressedBytes()
    {
        return count();
    }

    /// How many bytes are in the buffer (not yet compressed)
    size_t getRemainingBytes()
    {
        nextIfAtEnd();
        return offset();
    }

    size_t getCompressTime() const
    {
        return compress_time;
    }

    size_t getWriteTime() const
    {
        return write_time;
    }

private:
    void nextImpl() override;

    WriteBuffer & out;
    DB::CompressionCodecPtr codec;

    DB::PODArray<char> compressed_buffer;
    size_t compress_time = 0;
    size_t write_time = 0;
};

}

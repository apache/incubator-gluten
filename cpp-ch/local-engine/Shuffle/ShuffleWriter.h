#pragma once
#include <Shuffle/WriteBufferFromJavaOutputStream.h>
#include <Formats/NativeWriter.h>

namespace local_engine
{
class ShuffleWriter
{
public:
    ShuffleWriter(jobject output_stream, jbyteArray buffer, const std::string & codecStr, bool enable_compression, size_t customize_buffer_size);
    virtual ~ShuffleWriter();
    void write(const DB::Block & block);
    void flush();

private:
    std::unique_ptr<DB::CompressedWriteBuffer> compressed_out;
    std::unique_ptr<WriteBufferFromJavaOutputStream> write_buffer;
    std::unique_ptr<DB::NativeWriter> native_writer;
    bool compression_enable;
};
}

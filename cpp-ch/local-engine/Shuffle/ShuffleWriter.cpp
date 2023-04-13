#include "ShuffleWriter.h"
#include <Compression/CompressedWriteBuffer.h>
#include <Compression/CompressionFactory.h>
#include <boost/algorithm/string/case_conv.hpp>

using namespace DB;

namespace local_engine
{

ShuffleWriter::ShuffleWriter(jobject output_stream, jbyteArray buffer, const std::string & codecStr, bool enable_compression, size_t customize_buffer_size)
{
    compression_enable = enable_compression;
    write_buffer = std::make_unique<WriteBufferFromJavaOutputStream>(output_stream, buffer, customize_buffer_size);
    if (compression_enable)
    {
        auto codec = DB::CompressionCodecFactory::instance().get(boost::to_upper_copy(codecStr), {});
        compressed_out = std::make_unique<CompressedWriteBuffer>(*write_buffer, codec);
    }
}
void ShuffleWriter::write(const Block & block)
{
    if (!native_writer)
    {
        if (compression_enable)
        {
            native_writer = std::make_unique<NativeWriter>(*compressed_out, 0, block.cloneEmpty());
        }
        else
        {
            native_writer = std::make_unique<NativeWriter>(*write_buffer, 0, block.cloneEmpty());
        }
    }
    if (block.rows() > 0)
    {
        native_writer->write(block);
    }
}
void ShuffleWriter::flush()
{
    if (native_writer)
    {
        native_writer->flush();
    }
}
ShuffleWriter::~ShuffleWriter()
{
    if (native_writer)
    {
        native_writer->flush();
        if (compression_enable)
        {
            compressed_out->finalize();
        }
        write_buffer->finalize();
    }
}
}

#include "ShuffleReader.h"
#include <Common/DebugUtils.h>
#include <Common/Stopwatch.h>
#include <Common/JNIUtils.h>
#include <jni/jni_common.h>

using namespace DB;

namespace local_engine
{

local_engine::ShuffleReader::ShuffleReader(std::unique_ptr<ReadBuffer> in_, bool compressed) : in(std::move(in_))
{
    if (compressed)
    {
        compressed_in = std::make_unique<CompressedReadBuffer>(*in);
        input_stream = std::make_unique<NativeReader>(*compressed_in, 0);
    }
    else
    {
        input_stream = std::make_unique<NativeReader>(*in, 0);
    }
}
Block * local_engine::ShuffleReader::read()
{
    auto block = input_stream->read();
    setCurrentBlock(block);
    if (unlikely(header.columns() == 0))
        header = currentBlock().cloneEmpty();
    return &currentBlock();
}
ShuffleReader::~ShuffleReader()
{
    in.reset();
    compressed_in.reset();
    input_stream.reset();
}

jclass ShuffleReader::input_stream_class = nullptr;
jmethodID ShuffleReader::input_stream_read = nullptr;

bool ReadBufferFromJavaInputStream::nextImpl()
{
    int count = readFromJava();
    if (count > 0)
    {
        working_buffer.resize(count);
    }
    return count > 0;
}
int ReadBufferFromJavaInputStream::readFromJava()
{
    GET_JNIENV(env)
    jint count = safeCallIntMethod(env, java_in, ShuffleReader::input_stream_read, reinterpret_cast<jlong>(working_buffer.begin()), buffer_size);
    CLEAN_JNIENV
    return count;
}
ReadBufferFromJavaInputStream::ReadBufferFromJavaInputStream(jobject input_stream, size_t customize_buffer_size) : java_in(input_stream), buffer_size(customize_buffer_size)
{
}
ReadBufferFromJavaInputStream::~ReadBufferFromJavaInputStream()
{
    GET_JNIENV(env)
    env->DeleteGlobalRef(java_in);
    CLEAN_JNIENV

}

}

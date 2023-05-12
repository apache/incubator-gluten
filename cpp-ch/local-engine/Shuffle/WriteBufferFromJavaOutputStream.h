#pragma once
#include <jni.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/WriteBuffer.h>

namespace local_engine
{
class WriteBufferFromJavaOutputStream : public DB::BufferWithOwnMemory<DB::WriteBuffer>
{
public:
    static jclass output_stream_class;
    static jmethodID output_stream_write;
    static jmethodID output_stream_flush;

    WriteBufferFromJavaOutputStream(jobject output_stream, jbyteArray buffer, size_t customize_buffer_size);
    ~WriteBufferFromJavaOutputStream() override;

private:
    void nextImpl() override;

protected:
    void finalizeImpl() override;

private:
    jobject output_stream;
    jbyteArray buffer;
    size_t buffer_size;
};
}

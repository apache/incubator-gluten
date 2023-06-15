#pragma once
#include <functional>
#include <memory>
#include <IO/CompressionMethod.h>
#include <IO/ReadBuffer.h>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <boost/core/noncopyable.hpp>
#include <substrait/plan.pb.h>
#include <jni.h>
#include <jni/jni_common.h>
#include <Common/JNIUtils.h>

namespace local_engine
{
class ReadBufferBuilder
{
public:
    explicit ReadBufferBuilder(DB::ContextPtr context_) : context(context_) { }
    virtual ~ReadBufferBuilder() = default;
    /// build a new read buffer
    virtual std::unique_ptr<DB::ReadBuffer> build(const substrait::ReadRel::LocalFiles::FileOrFiles & file_info, const bool & set_read_util_position=false) = 0;
protected:
    DB::ContextPtr context;
};

using ReadBufferBuilderPtr = std::shared_ptr<ReadBufferBuilder>;

class ReadBufferBuilderFactory : public boost::noncopyable
{
public:
    using NewBuilder = std::function<ReadBufferBuilderPtr(DB::ContextPtr)>;
    static ReadBufferBuilderFactory & instance();
    ReadBufferBuilderPtr createBuilder(const String & schema, DB::ContextPtr context);

    void registerBuilder(const String & schema, NewBuilder newer);

private:
    std::map<String, NewBuilder> builders;
};

class HDFSFileCompressionCodec
{
public:
    static jclass compression_codec_class;
    static jmethodID compression_codec_get;

    static DB::CompressionMethod getCompressionCodec(std::string file_path) 
    {
        GET_JNIENV(env)
        jstring file_path_j = local_engine::stringTojstring(env, file_path.data());
        jobject codec_obj = local_engine::safeCallStaticObjectMethod(env,
            HDFSFileCompressionCodec::compression_codec_class, 
            HDFSFileCompressionCodec::compression_codec_get, 
            file_path_j);
        std::string codec = local_engine::jstring2string(env, static_cast<jstring>(codec_obj));
        CLEAN_JNIENV
        if (codec == "ZLib")
        {
            return DB::CompressionMethod::Zlib;
        }
        else if (codec == "BZip2")
        {
            return DB::CompressionMethod::Bzip2;
        }
        else if (codec == "None")
        {
            return DB::CompressionMethod::None;
        }
        else
        {
            throw DB::Exception(-1, "Compression method: {} is not supported, file path: {}", codec, file_path);
        }
    }
};

void registerReadBufferBuilders();
}

#pragma once
#include <functional>
#include <memory>
#include <IO/ReadBuffer.h>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <boost/core/noncopyable.hpp>
#include <substrait/plan.pb.h>

namespace local_engine
{
class ReadBufferBuilder
{
public:
    explicit ReadBufferBuilder(DB::ContextPtr context_) : context(context_) { }
    virtual ~ReadBufferBuilder() = default;

    /// build a new read buffer
    virtual std::unique_ptr<DB::ReadBuffer>
    build(const substrait::ReadRel::LocalFiles::FileOrFiles & file_info, bool set_read_util_position = false) = 0;

    /// build a new read buffer, consider compression method
    std::unique_ptr<DB::ReadBuffer> buildWithCompressionWrapper(const substrait::ReadRel::LocalFiles::FileOrFiles & file_info, bool set_read_util_position = false)
    {
        auto in = build(file_info, set_read_util_position);

        /// Wrap the read buffer with compression method if exists
        Poco::URI file_uri(file_info.uri_file());
        DB::CompressionMethod compression = DB::chooseCompressionMethod(file_uri.getPath(), "auto");
        return compression != DB::CompressionMethod::None ? DB::wrapReadBufferWithCompressionMethod(std::move(in), compression)
                                                          : std::move(in);
    }

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

void registerReadBufferBuilders();
}

#pragma once

#include <functional>
#include <memory>
#include <IO/ReadBuffer.h>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <boost/core/noncopyable.hpp>
namespace local_engine
{
class WriteBufferBuilder
{
public:
    explicit WriteBufferBuilder(DB::ContextPtr context_) : context(context_) { }
    virtual ~WriteBufferBuilder() = default;
    /// build a new write buffer
    virtual std::unique_ptr<DB::WriteBuffer> build(const std::string & file_uri_) = 0;

protected:
    DB::ContextPtr context;
};

using WriteBufferBuilderPtr = std::shared_ptr<WriteBufferBuilder>;

class WriteBufferBuilderFactory : public boost::noncopyable
{
public:
    using NewBuilder = std::function<WriteBufferBuilderPtr(DB::ContextPtr)>;
    static WriteBufferBuilderFactory & instance();
    WriteBufferBuilderPtr createBuilder(const String & schema, DB::ContextPtr context);

    void registerBuilder(const String & schema, NewBuilder newer);

private:
    std::map<String, NewBuilder> builders;
};

void registerWriteBufferBuilders();
}

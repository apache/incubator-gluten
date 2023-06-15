#pragma once

#include <thrift/transport/TVirtualTransport.h>
#include <IO/ReadBufferFromFileBase.h>
namespace DB
{

class ThriftFileTransport : public apache::thrift::transport::TVirtualTransport<ThriftFileTransport>
{
public:
    ThriftFileTransport(ReadBufferFromFileBase * file_)
        : file(file_), location(0){
    }


    uint32_t read(uint8_t * buf, uint32_t len) {
        size_t n = file->read(reinterpret_cast<char*>(buf), len);
        location += n;
        return static_cast<uint32_t>(n);
    }

    void setLocation(size_t location_) {
        location = location_;
        file->seek(location, SEEK_SET);
    }

private:
    ReadBufferFromFileBase * file;
    size_t location;
};
}

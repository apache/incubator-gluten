#pragma once

#include <thrift/TApplicationException.h>
#include <thrift/Thrift.h>
#include <thrift/concurrency/ThreadManager.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/protocol/TDebugProtocol.h>
#include <thrift/protocol/TJSONProtocol.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TSocket.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

enum TProtocolType
{
    // Use TCompactProtocol to deserialize msg
    COMPACT, // 0
    // Use TBinaryProtocol to deserialize msg
    BINARY, // 1
    // Use TJSONProtocol to deserialize msg
    JSON // 2
};

std::shared_ptr<apache::thrift::protocol::TProtocol>
create_deserialize_protocol(const std::shared_ptr<apache::thrift::transport::TMemoryBuffer> & mem, TProtocolType type)
{
    if (type == TProtocolType::COMPACT)
    {
        apache::thrift::protocol::TCompactProtocolFactoryT<apache::thrift::transport::TMemoryBuffer> tproto_factory;
        return tproto_factory.getProtocol(mem);
    }
    else if (type == TProtocolType::JSON)
    {
        apache::thrift::protocol::TJSONProtocolFactory tproto_factory;
        return tproto_factory.getProtocol(mem);
    }
    else
    {
        apache::thrift::protocol::TBinaryProtocolFactoryT<apache::thrift::transport::TMemoryBuffer> tproto_factory;
        return tproto_factory.getProtocol(mem);
    }
}

template <class T>
size_t deserialize_thrift_msg(const uint8_t * buf, const size_t len, TProtocolType type, T * deserialized_msg)
{
    // Deserialize msg bytes into c++ thrift msg using memory
    // transport. TMemoryBuffer is not const-safe, although we use it in
    // a const-safe way, so we have to explicitly cast away the const.
    std::shared_ptr<apache::thrift::transport::TMemoryBuffer> tmem_transport(
        new apache::thrift::transport::TMemoryBuffer(const_cast<uint8_t *>(buf), static_cast<uint32_t>(len)));
    std::shared_ptr<apache::thrift::protocol::TProtocol> tproto = create_deserialize_protocol(tmem_transport, type);

    try
    {
        deserialized_msg->read(tproto.get());
    }
    catch (std::exception & e)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "couldn't deserialize thrift msg:\n{}", e.what());
    }
    catch (...)
    {
        // TODO: Find the right exception for 0 bytes
        throw Exception(ErrorCodes::LOGICAL_ERROR, "unknown exception");
    }

    uint32_t bytes_left = tmem_transport->available_read();
    return len - bytes_left;
}
}

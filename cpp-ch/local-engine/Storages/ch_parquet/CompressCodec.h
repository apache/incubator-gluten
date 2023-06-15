#pragma once
#include <IO/CompressionMethod.h>
#include <generated/parquet_types.h>

namespace DB
{
CompressionMethod getCompressionMethod(parquet::format::CompressionCodec::type codec);

class ICompressCodec;

using CompressCodecPtr = std::shared_ptr<ICompressCodec>;

class ICompressCodec
{
public:
    static CompressCodecPtr getCompressCodec(CompressionMethod method);
    virtual ~ICompressCodec() = default;
    virtual void decompress(const char* compressed, size_t compressed_length, char* uncompressed, size_t uncompressed_length) = 0;
};


class SnappyCompressCodec : public ICompressCodec
{
public:
    static CompressCodecPtr instance();
    ~SnappyCompressCodec() override = default;
    void decompress(const char * compressed, size_t compressed_length, char * uncompressed, size_t uncompressed_length) override;
};
}



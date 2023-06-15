#include "CompressCodec.h"
#include <Common/Exception.h>
#include <snappy.h>
#include <magic_enum.hpp>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
CompressionMethod getCompressionMethod(parquet::format::CompressionCodec::type codec)
{
    switch (codec)
    {
        case parquet::format::CompressionCodec::UNCOMPRESSED:
            return CompressionMethod::None;
        case parquet::format::CompressionCodec::SNAPPY:
            return CompressionMethod::Snappy;
        case parquet::format::CompressionCodec::LZ4:
            return CompressionMethod::Lz4;
        case parquet::format::CompressionCodec::ZSTD:
            return CompressionMethod::Zstd;
        case parquet::format::CompressionCodec::GZIP:
            return CompressionMethod::Gzip;
        case parquet::format::CompressionCodec::BROTLI:
            return CompressionMethod::Brotli;
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "unknown compression codec");
    }
}
void SnappyCompressCodec::decompress(const char * compressed, size_t compressed_length, char * uncompressed, size_t uncompressed_length)
{
    size_t size;
    if (!snappy::RawUncompress(compressed, compressed_length, uncompressed)) {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "snappy decompress failed");
    }
    snappy::GetUncompressedLength(compressed, compressed_length, &size);
    chassert(size == uncompressed_length);
}
CompressCodecPtr SnappyCompressCodec::instance()
{
    static CompressCodecPtr codec = std::make_shared<SnappyCompressCodec>();
    return codec;
}
CompressCodecPtr ICompressCodec::getCompressCodec(CompressionMethod method)
{
    switch (method)
    {
        case CompressionMethod::Snappy:
            return SnappyCompressCodec::instance();
        case CompressionMethod::None:
            return nullptr;
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "unsupported codec {}", magic_enum::enum_name(method));
    }
}
}

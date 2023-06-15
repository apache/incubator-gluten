#include "Decoder.h"
#include <cmath>
#include <Common/Exception.h>
#include "RleEncoding.h"
#include "Utils.h"
#include "type.h"


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

void LevelDecoder::parse(parquet::format::Encoding::type encoding_, size_t max_level_, uint32_t num_levels_, Slice * slice)
{
    encoding = encoding_;
    bit_width = static_cast<size_t>(ceil(log2(max_level_ + 1)));
    num_levels = num_levels_;
    switch (encoding)
    {
        case parquet::format::Encoding::RLE: {
            chassert(slice->size >= 4);

            auto * data = slice->data;
            size_t num_bytes = decode_fixed32_le(data);
            chassert(num_bytes <= slice->size - 4);
            rle_decoder = std::make_shared<RleDecoder<level_t>>(data + 4, (int)num_bytes, (int)bit_width);

            slice->data += 4 + num_bytes;
            slice->size -= 4 + num_bytes;
            break;
        }
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "not supported encoding");
    }
}
size_t LevelDecoder::decode_batch(size_t n, level_t * levels)
{
    if (encoding == parquet::format::Encoding::RLE)
    {
        n = std::min((size_t)num_levels, n);
        auto num_decoded = rle_decoder->GetBatch(levels, n);
        num_levels -= num_decoded;
        return num_decoded;
    }

    return 0;
}
size_t LevelDecoder::get_repeated_value(size_t count)
{
    return rle_decoder->getRepeatedValue(count);
}

using TypeEncodingPair = std::pair<parquet::format::Type::type, parquet::format::Encoding::type>;

struct EncodingMapHash
{
    size_t operator()(const TypeEncodingPair & pair) const { return (pair.first << 5) ^ pair.second; }
};

template <parquet::format::Type::type type, parquet::format::Encoding::type encoding>
struct TypeEncodingTraits
{
};

template <parquet::format::Type::type type>
struct TypeEncodingTraits<type, parquet::format::Encoding::PLAIN>
{
    static std::unique_ptr<Decoder> create_decoder()
    {
        return std::make_unique<PlainDecoder<typename PhysicalTypeTraits<type>::CppType>>();
    }
};


template <parquet::format::Type::type type>
struct TypeEncodingTraits<type, parquet::format::Encoding::RLE_DICTIONARY>
{
    static std::unique_ptr<Decoder> create_decoder() { return std::make_unique<DictDecoder<typename PhysicalTypeTraits<type>::CppType>>(); }
};


template <parquet::format::Type::type type_arg, parquet::format::Encoding::type encoding_arg>
struct EncodingTraits : TypeEncodingTraits<type_arg, encoding_arg>
{
    static constexpr parquet::format::Type::type type = type_arg;
    static constexpr parquet::format::Encoding::type encoding = encoding_arg;
};


class EncodingInfoResolver
{
public:
    EncodingInfoResolver();
    ~EncodingInfoResolver();

    EncodingInfo get(parquet::format::Type::type type, parquet::format::Encoding::type encoding)
    {
        auto key = std::make_pair(type, encoding);
        auto it = encoding_map.find(key);
        if (it == std::end(encoding_map))
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "fail to find valid type encoding, type:{}, encoding:{}",
                ::parquet::format::to_string(type),
                ::parquet::format::to_string(encoding));
        }
        return *it->second;
    }

private:
    // Not thread-safe
    template <parquet::format::Type::type type, parquet::format::Encoding::type encoding>
    void add_map()
    {
        auto key = std::make_pair(type, encoding);
        auto it = encoding_map.find(key);
        if (it != encoding_map.end())
        {
            return;
        }
        EncodingTraits<type, encoding> traits;
        encoding_map.emplace(key, std::make_shared<EncodingInfo>(traits));
    }


    std::unordered_map<TypeEncodingPair, std::shared_ptr<EncodingInfo>, EncodingMapHash> encoding_map;
};

EncodingInfoResolver::EncodingInfoResolver()
{
    // BOOL
    //    add_map<parquet::format::Type::BOOLEAN, parquet::format::Encoding::PLAIN>();
    // INT32
    add_map<parquet::format::Type::INT32, parquet::format::Encoding::PLAIN>();
    add_map<parquet::format::Type::INT32, parquet::format::Encoding::RLE_DICTIONARY>();

    // INT64
    add_map<parquet::format::Type::INT64, parquet::format::Encoding::PLAIN>();
    add_map<parquet::format::Type::INT64, parquet::format::Encoding::RLE_DICTIONARY>();

    // INT96
    //    add_map<parquet::format::Type::INT96, parquet::format::Encoding::PLAIN>();
    //    add_map<parquet::format::Type::INT96, parquet::format::Encoding::RLE_DICTIONARY>();

    // FLOAT
    add_map<parquet::format::Type::FLOAT, parquet::format::Encoding::PLAIN>();
    add_map<parquet::format::Type::FLOAT, parquet::format::Encoding::RLE_DICTIONARY>();

    // DOUBLE
    add_map<parquet::format::Type::DOUBLE, parquet::format::Encoding::PLAIN>();
    add_map<parquet::format::Type::DOUBLE, parquet::format::Encoding::RLE_DICTIONARY>();

    // BYTE_ARRAY encoding
    add_map<parquet::format::Type::BYTE_ARRAY, parquet::format::Encoding::PLAIN>();
    add_map<parquet::format::Type::BYTE_ARRAY, parquet::format::Encoding::RLE_DICTIONARY>();

    // FIXED_LEN_BYTE_ARRAY encoding
    //    add_map<parquet::format::Type::FIXED_LEN_BYTE_ARRAY, parquet::format::Encoding::PLAIN>();
    //    add_map<parquet::format::Type::FIXED_LEN_BYTE_ARRAY, parquet::format::Encoding::RLE_DICTIONARY>();
}

EncodingInfoResolver::~EncodingInfoResolver()
{
    encoding_map.clear();
}

EncodingInfoResolver encoding_info_resolver;

template <typename TraitsClass>
EncodingInfo::EncodingInfo(TraitsClass /*traits*/)
    : create_decoder_func(TraitsClass::create_decoder), type(TraitsClass::type), encoding(TraitsClass::encoding)
{
}

EncodingInfo EncodingInfo::get(parquet::format::Type::type type, parquet::format::Encoding::type encoding)
{
    return encoding_info_resolver.get(type, encoding);
}

}

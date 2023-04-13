#pragma once

#include <city.h>
#include <base/types.h>

#ifdef __clang__
#    pragma clang diagnostic push
#    pragma clang diagnostic ignored "-Wused-but-marked-unused"
#endif

#include <Functions/FunctionsHashing.h>

namespace local_engine
{
using namespace DB;

/// For spark compatiability of ClickHouse.
/// The difference between spark xxhash64 and CH xxHash64
/// In Spark, the seed is 42
/// In CH, the seed is 0. So we need to add new impl ImplXxHash64Spark in CH with seed = 42.
struct ImplXxHashSpark64
{
    static constexpr auto name = "xxHashSpark64";
    using ReturnType = UInt64;
    using uint128_t = CityHash_v1_0_2::uint128;

    static auto apply(const char * s, const size_t len) { return XXH_INLINE_XXH64(s, len, 42); }

    /*
       With current implementation with more than 1 arguments it will give the results
       non-reproducible from outside of CH. (see comment on ImplXxHash32).
     */
    static auto combineHashes(UInt64 h1, UInt64 h2) { return CityHash_v1_0_2::Hash128to64(uint128_t(h1, h2)); }

    static constexpr bool use_int_hash_for_pods = false;
};


/// Block read - if your platform needs to do endian-swapping or can only
/// handle aligned reads, do the conversion here
static ALWAYS_INLINE uint32_t getblock32(const uint32_t * p, int i)
{
    uint32_t res;
    memcpy(&res, p + i, sizeof(res));
    return res;
}

static ALWAYS_INLINE uint32_t rotl32(uint32_t x, int8_t r)
{
    return (x << r) | (x >> (32 - r));
}

static void MurmurHashSpark3_x86_32(const void * key, size_t len, uint32_t seed, void * out)
{
    const uint8_t * data = static_cast<const uint8_t *>(key);
    const int nblocks = len / 4;

    uint32_t h1 = seed;

    const uint32_t c1 = 0xcc9e2d51;
    const uint32_t c2 = 0x1b873593;

    /// body
    const uint32_t * blocks = reinterpret_cast<const uint32_t *>(data + nblocks * 4);

    for (int i = -nblocks; i; i++)
    {
        uint32_t k1 = getblock32(blocks, i);

        k1 *= c1;
        k1 = rotl32(k1, 15);
        k1 *= c2;

        h1 ^= k1;
        h1 = rotl32(h1, 13);
        h1 = h1 * 5 + 0xe6546b64;
    }

    /// tail
    const uint8_t * tail = (data + nblocks * 4);
    uint32_t k1 = 0;
    while (tail != data + len)
    {
        k1 = *tail;

        k1 *= c1;
        k1 = rotl32(k1, 15);
        k1 *= c2;

        h1 ^= k1;
        h1 = rotl32(h1, 13);
        h1 = h1 * 5 + 0xe6546b64;

        ++tail;
    }

    /// finalization
    h1 ^= len;
    h1 ^= h1 >> 16;
    h1 *= 0x85ebca6b;
    h1 ^= h1 >> 13;
    h1 *= 0xc2b2ae35;
    h1 ^= h1 >> 16;

    /// output
    *static_cast<uint32_t *>(out) = h1;
}

/// For spark compatiability of ClickHouse.
/// The difference between spark hash and CH murmurHash3_32
/// 1. They calculate hash functions with different seeds
/// 2. Spark current impl is not right, but it is not fixed for backward compatiability. See: https://issues.apache.org/jira/browse/SPARK-23381
struct MurmurHashSpark3Impl32
{
    static constexpr auto name = "murmurHashSpark3_32";
    using ReturnType = UInt32;

    static UInt32 apply(const char * data, const size_t size)
    {
        union
        {
            UInt32 h;
            char bytes[sizeof(h)];
        };
        MurmurHashSpark3_x86_32(data, size, 42, bytes);
        return h;
    }

    static UInt32 combineHashes(UInt32 h1, UInt32 h2) { return IntHash32Impl::apply(h1) ^ h2; }

    static constexpr bool use_int_hash_for_pods = false;
};

using FunctionXxHashSpark64 = FunctionAnyHash<ImplXxHashSpark64>;
using FunctionMurmurHashSpark3_32 = FunctionAnyHash<MurmurHashSpark3Impl32>;

}

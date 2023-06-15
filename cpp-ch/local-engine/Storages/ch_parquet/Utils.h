#pragma once
#include <Common/Exception.h>
#include <Common/PODArray.h>
#include <Common/PODArray_fwd.h>
#include <generated/parquet_types.h>

namespace DB
{


struct Slice
{
    char * data;
    size_t size;
};

class EndOfFile : public Exception {};


inline uint32_t decode_fixed32_le(const char * buf)
{
    uint32_t res;
    memcpy(&res, buf, sizeof(res));
    if constexpr (std::endian::native != std::endian::little)
        std::byteswap(res);
    return res;
}

// Returns the 'num_bits' least-significant bits of 'v'.
inline uint64_t TrailingBits(uint64_t v, int num_bits)
{
    if (num_bits == 0)
        return 0;
    if (num_bits >= 64)
        return v;
    int n = 64 - num_bits;
    return (v << n) >> n;
}

static inline uint64_t ShiftLeftZeroOnOverflow(uint64_t v, int num_bits)
{
    if (num_bits >= 64)
        return 0;
    return v << num_bits;
}
constexpr static inline bool IsPowerOf2(int64_t value) { return (value & (value - 1)) == 0; }
static inline int64_t round_up(int64_t value, int64_t factor) { return (value + (factor - 1)) / factor * factor; }

constexpr static inline size_t roundUpNumBytes(size_t bits)
{
    return (bits + 7) >> 3;
}

// Returns the rounded up to 32 multiple. Used for conversions of bits to i32.
constexpr static inline uint32_t round_up_numi32(uint32_t bits) { return (bits + 31) >> 5; }


// Returns the ceil of value/divisor
static inline int Ceil(int value, int divisor)
{
    return value / divisor + (value % divisor != 0);
}

static inline int64_t RoundDownToPowerOf2(int64_t value, int64_t factor) {
    chassert((factor > 0) && ((factor & (factor - 1)) == 0));
    return value & ~(factor - 1);
}

/// Specialized round up and down functions for frequently used factors,
/// like 8 (bits->bytes), 32 (bits->i32), and 64 (bits->i64)
/// Returns the rounded up number of bytes that fit the number of bits.
constexpr static inline uint32_t RoundUpNumBytes(uint32_t bits) { return (bits + 7) >> 3; }


}

#pragma once
#include "Utils.h"
#include "bit_packing.inline.h"

namespace DB
{

// Utility class to read bit/byte stream.  This class can read bits or bytes
// that are either byte aligned or not.  It also has utilities to read multiple
// bytes in one read (e.g. encoded int).
class ParquetBitReader
{
public:
    // 'buffer' is the buffer to read from.  The buffer's length is 'buffer_len'.
    ParquetBitReader(char * buffer, int buffer_len);

    ParquetBitReader() = default;

    // Gets the next value from the buffer.  Returns true if 'v' could be read or false if
    // there are not enough bytes left. num_bits must be <= 32.
    template <typename T>
    bool GetValue(int num_bits, T * v);

    // Reads a 'num_bytes'-sized value from the buffer and stores it in 'v'. T needs to be a
    // little-endian native type and big enough to store 'num_bytes'. The value is assumed
    // to be byte-aligned so the stream will be advanced to the start of the next byte
    // before 'v' is read. Returns false if there are not enough bytes left.
    template <typename T>
    bool GetAligned(int num_bytes, T * v);

    // Reads a vlq encoded int from the stream.  The encoded int must start at the
    // beginning of a byte. Return false if there were not enough bytes in the buffer.
    bool GetVlqInt(uint32_t * v);

    // Returns the number of bytes left in the stream, not including the current byte (i.e.,
    // there may be an additional fraction of a byte).
    int bytes_left() const { return max_bytes - (byte_offset + Ceil(bit_offset, 8)); }

    // Current position in the stream, by bit.
    int position() const { return byte_offset * 8 + bit_offset; }

    // Rewind the stream by 'num_bits' bits
    void Rewind(int num_bits);

    // Seek to a specific bit in the buffer
    void SeekToBit(uint stream_position);

    // Maximum byte length of a vlq encoded int
    static const int MAX_VLQ_BYTE_LEN = 5;

    bool is_initialized() const { return buffer != nullptr; }

private:
    // Used by SeekToBit() and GetValue() to fetch the
    // the next word into buffer_.
    void BufferValues();

    char * buffer;
    int max_bytes = 0;

    // Bytes are memcpy'd from buffer_ and values are read from this variable. This is
    // faster than reading values byte by byte directly from buffer_.
    uint64_t buffered_values = 0;

    int byte_offset = 0; // Offset in buffer_
    int bit_offset = 0; // Offset in buffered_values_
};

template <typename T>
inline bool ParquetBitReader::GetAligned(int num_bytes, T * v)
{
    //    DCHECK_LE(num_bytes, sizeof(T));
    int bytes_read = Ceil(bit_offset, 8);
    if (byte_offset + bytes_read + num_bytes > max_bytes)
        return false;

    // Advance byte_offset to next unread byte and read num_bytes
    byte_offset += bytes_read;
    memcpy(v, buffer + byte_offset, num_bytes);
    byte_offset += num_bytes;

    // Reset buffered_values_
    bit_offset = 0;
    int bytes_remaining = max_bytes - byte_offset;
    if (bytes_remaining >= 8)
    {
        memcpy(&buffered_values, buffer + byte_offset, 8);
    }
    else
    {
        memcpy(&buffered_values, buffer + byte_offset, bytes_remaining);
    }
    return true;
}


inline ParquetBitReader::ParquetBitReader(char * buffer_, int buffer_len) : buffer(buffer_), max_bytes(buffer_len)
{
    int num_bytes = std::min(8, max_bytes);
    memcpy(&buffered_values, buffer + byte_offset, num_bytes);
}

inline void ParquetBitReader::BufferValues()
{
    int bytes_remaining = max_bytes - byte_offset;
    if (bytes_remaining >= 8)
    {
        memcpy(&buffered_values, buffer + byte_offset, 8);
    }
    else
    {
        memcpy(&buffered_values, buffer + byte_offset, bytes_remaining);
    }
}

template <typename T>
inline bool ParquetBitReader::GetValue(int num_bits, T * v)
{
    chassert(num_bits < 64);
    chassert((size_t)num_bits < sizeof(T) * 8);

    if (byte_offset * 8 + bit_offset + num_bits > max_bytes * 8)
        return false;

    *v = TrailingBits(buffered_values, bit_offset + num_bits) >> bit_offset;

    bit_offset += num_bits;
    if (bit_offset >= 64)
    {
        byte_offset += 8;
        bit_offset -= 64;
        BufferValues();
        // Read bits of v that crossed into new buffered_values_
        *v |= ShiftLeftZeroOnOverflow(TrailingBits(buffered_values, bit_offset), (num_bits - bit_offset));
    }
    chassert(bit_offset < 64);
    return true;
}

template bool ParquetBitReader::GetValue(int num_bits, size_t * v);

inline void ParquetBitReader::Rewind(int num_bits)
{
    bit_offset -= num_bits;
    if (bit_offset >= 0)
    {
        return;
    }
    while (bit_offset < 0)
    {
        int seek_back = std::min(byte_offset, 8);
        byte_offset -= seek_back;
        bit_offset += seek_back * 8;
    }
    // This should only be executed *if* rewinding by 'num_bits'
    // make the existing buffered_values_ invalid
    chassert(byte_offset > 0); // Check for underflow
    memcpy(&buffered_values, buffer + byte_offset, 8);
}

inline void ParquetBitReader::SeekToBit(uint stream_position)
{
    //    chassert(stream_position < max_bytes * 8);

    int delta = static_cast<int>(stream_position) - position();
    if (delta == 0)
    {
        return;
    }
    else if (delta < 0)
    {
        Rewind(position() - stream_position);
    }
    else
    {
        bit_offset += delta;
        while (bit_offset >= 64)
        {
            byte_offset += 8;
            bit_offset -= 64;
            if (bit_offset < 64)
            {
                // This should only be executed if seeking to
                // 'stream_position' makes the existing buffered_values_
                // invalid.
                BufferValues();
            }
        }
    }
}


inline bool ParquetBitReader::GetVlqInt(uint32_t * v)
{
    *v = 0;
    int shift = 0;
    [[maybe_unused]] int num_bytes = 0;
    uint8_t byte = 0;
    do
    {
        if (!GetAligned<uint8_t>(1, &byte))
            return false;
        *v |= (byte & 0x7F) << shift;
        shift += 7;
        chassert(++num_bytes < MAX_VLQ_BYTE_LEN);
    } while ((byte & 0x80) != 0);
    return true;
}
class BatchedBitReader {
public:
    BatchedBitReader() = default;

    void reset(const uint8_t* buffer, int64_t buffer_len) {
        _buffer_pos = buffer;
        _buffer_end = buffer + buffer_len;
    }

    // Reads an unpacked 'num_bytes'-sized value from the buffer and stores it in 'v'. T
    // needs to be a little-endian native type and big enough to store 'num_bytes'.
    // Returns false if there are not enough bytes left.
    template <typename T>
    bool get_bytes(int num_bytes, T* v);

    // Gets up to 'num_values' bit-packed values, starting from the current byte in the
    // buffer and advance the read position. 'bit_width' must be <= 64.
    // If 'bit_width' * 'num_values' is not a multiple of 8, the trailing bytes are
    // skipped and the next UnpackBatch() call will start reading from the next byte.
    //
    // If the caller does not want to drop trailing bits, 'num_values' must be exactly the
    // total number of values the caller wants to read from a run of bit-packed values, or
    // 'bit_width' * 'num_values' must be a multiple of 8. This condition is always
    // satisfied if 'num_values' is a multiple of 32.
    //
    // The output type 'T' must be an unsigned integer.
    //
    // Returns the number of values read.
    template <typename T>
    int unpack_batch(int bit_width, int num_values, T* v);

    /// Read an unsigned ULEB-128 encoded int from the stream. The encoded int must start
    /// at the beginning of a byte. Return false if there were not enough bytes in the
    /// buffer or the int is invalid. For more details on ULEB-128:
    /// https://en.wikipedia.org/wiki/LEB128
    /// UINT_T must be an unsigned integer type.
    template <typename UINT_T>
    bool get_lleb_128(UINT_T* v);

private:
    /// Returns the number of bytes left in the stream.
    int _bytes_left() { return static_cast<int>(_buffer_end - _buffer_pos); }

    /// Maximum byte length of a vlq encoded integer of type T.
    template <typename T>
    static constexpr int _max_vlq_byte_len() {
        return Ceil(sizeof(T) * 8, 7);
    }

    // current read position in the buffer
    const uint8_t* _buffer_pos = nullptr;

    // the end of buffer
    const uint8_t* _buffer_end = nullptr;
};


template <typename UINT_T>
inline bool BatchedBitReader::get_lleb_128(UINT_T * v)
{
    static_assert(std::is_integral<UINT_T>::value, "Integral type required.");
    static_assert(std::is_unsigned<UINT_T>::value, "Unsigned type required.");
    static_assert(!std::is_same<UINT_T, bool>::value, "Bools are not supported.");

    *v = 0;
    int shift = 0;
    uint8_t byte = 0;
    do
    {
        if (unlikely(shift >= _max_vlq_byte_len<UINT_T>() * 7))
            return false;
        if (!get_bytes(1, &byte))
            return false;

        /// We need to convert 'byte' to UINT_T so that the result of the bitwise and
        /// operation is at least as long an integer as '*v', otherwise the shift may be too
        /// big and lead to undefined behaviour.
        const UINT_T byte_as_UINT_T = byte;
        *v |= (byte_as_UINT_T & 0x7Fu) << shift;
        shift += 7;
    } while ((byte & 0x80u) != 0);
    return true;
}

template <typename T>
inline bool BatchedBitReader::get_bytes(int num_bytes, T * v)
{
    if (unlikely(_buffer_pos + num_bytes > _buffer_end))
    {
        return false;
    }
    *v = 0; // Ensure unset bytes are initialized to zero.
    memcpy(v, _buffer_pos, num_bytes);
    _buffer_pos += num_bytes;
    return true;
}

template <typename T>
inline int BatchedBitReader::unpack_batch(int bit_width, int num_values, T * v)
{
    int64_t num_read;
    std::tie(_buffer_pos, num_read) = BitPacking::UnpackValues(bit_width, _buffer_pos, _bytes_left(), num_values, v);
    return static_cast<int>(num_read);
}
}



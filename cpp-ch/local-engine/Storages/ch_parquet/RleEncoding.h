#pragma once

#include <IO/BitHelpers.h>
#include "Utils.h"
#include "bit_util.h"

namespace DB
{


template <typename T>
class RleDecoder
{
public:
    RleDecoder(char * buffer, int buffer_len, int bit_width_);

    RleDecoder() = default;

    size_t Skip(size_t to_skip);

    bool Get(T * val);

    void RewindOne();

    size_t GetNextRun(T * val, size_t max_run);

    size_t GetBatch(T * vals, size_t batch_num);

    size_t repeatedCount()
    {
        if (repeat_count > 0)
        {
            return repeat_count;
        }
        if (literal_count == 0)
        {
            ReadHeader();
        }
        return repeat_count;
    }

    size_t literalCount()
    {
        if (literal_count > 0)
        {
            return literal_count;
        }
        if (repeat_count == 0)
        {
            ReadHeader();
        }
        return literal_count;
    }

    T getRepeatedValue(size_t count)
    {
        //        DCHECK_GE(repeat_count_, count);
        repeat_count -= count;
        return current_value;
    }

private:
    bool ReadHeader();

    enum RewindState
    {
        REWIND_LITERAL,
        REWIND_RUN,
        CANT_REWIND
    };

    ParquetBitReader bit_reader;
    int bit_width;
    uint64_t current_value;
    uint32_t repeat_count;
    uint32_t literal_count;
    RewindState rewind_state;
};

template <typename T>
class RleBatchDecoder
{
public:
    RleBatchDecoder(uint8_t * buffer, int buffer_len, int bit_width) { Reset(buffer, buffer_len, bit_width); }

    RleBatchDecoder() = default;

    // Reset the decoder to read from a new buffer.
    void Reset(uint8_t * buffer, int buffer_len, int bit_width);

    // Return the size of the current repeated run. Returns zero if the current run is
    // a literal run or if no more runs can be read from the input.
    int32_t NextNumRepeats();

    // Get the value of the current repeated run and consume the given number of repeats.
    // Only valid to call when NextNumRepeats() > 0. The given number of repeats cannot
    // be greater than the remaining number of repeats in the run. 'num_repeats_to_consume'
    // can be set to 0 to peek at the value without consuming repeats.
    T GetRepeatedValue(int32_t num_repeats_to_consume);

    // Return the size of the current literal run. Returns zero if the current run is
    // a repeated run or if no more runs can be read from the input.
    int32_t NextNumLiterals();

    // Consume 'num_literals_to_consume' literals frsadfom the current literal run,
    // copying the values to 'values'. 'num_literals_to_consume' must be <=
    // NextNumLiterals(). Returns true if the requested number of literals were
    // successfully read or false if an error was encountered, e.g. the input was
    // truncated.
    bool GetLiteralValues(int32_t num_literals_to_consume, T * values);

    // Consume 'num_values_to_consume' values and copy them to 'values'.
    // Returns the number of consumed values or 0 if an error occurred.
    int32_t GetBatch(T * values, int32_t batch_num);

private:
    // Called when both 'literal_count_' and 'repeat_count_' have been exhausted.
    // Sets either 'literal_count_' or 'repeat_count_' to the size of the next literal
    // or repeated run, or leaves both at 0 if no more values can be read (either because
    // the end of the input was reached or an error was encountered decoding).
    void NextCounts();

    /// Fill the literal buffer. Invalid to call if there are already buffered literals.
    /// Return false if the input was truncated. This does not advance 'literal_count_'.
    bool FillLiteralBuffer();

    bool HaveBufferedLiterals() const { return literal_buffer_pos_ < num_buffered_literals_; }

    /// Output buffered literals, advancing 'literal_buffer_pos_' and decrementing
    /// 'literal_count_'. Returns the number of literals outputted.
    int32_t OutputBufferedLiterals(int32_t max_to_output, T * values);

    BatchedBitReader bit_reader_;

    // Number of bits needed to encode the value. Must be between 0 and 64 after
    // the decoder is initialized with a buffer. -1 indicates the decoder was not
    // initialized.
    int bit_width_ = -1;

    // If a repeated run, the number of repeats remaining in the current run to be read.
    // If the current run is a literal run, this is 0.
    int32_t repeat_count_ = 0;

    // If a literal run, the number of literals remaining in the current run to be read.
    // If the current run is a repeated run, this is 0.
    int32_t literal_count_ = 0;

    // If a repeated run, the current repeated value.
    T repeated_value_;

    // Size of buffer for literal values. Large enough to decode a full batch of 32
    // literals. The buffer is needed to allow clients to read in batches that are not
    // multiples of 32.
    static constexpr int LITERAL_BUFFER_LEN = 32;

    // Buffer containing 'num_buffered_literals_' values. 'literal_buffer_pos_' is the
    // position of the next literal to be read from the buffer.
    T literal_buffer_[LITERAL_BUFFER_LEN];
    int num_buffered_literals_ = 0;
    int literal_buffer_pos_ = 0;
};

template <typename T>
inline RleDecoder<T>::RleDecoder(char * buffer, int buffer_len, int bit_width_)
    : bit_reader(buffer, buffer_len), bit_width(bit_width_), current_value(0), repeat_count(0), literal_count(0), rewind_state(CANT_REWIND)
{
    //        DCHECK_GE(bit_width_, 0);
    //        DCHECK_LE(bit_width_, 64);
}

template <typename T>
inline size_t RleDecoder<T>::Skip(size_t to_skip)
{
    size_t set_count = 0;
    while (to_skip > 0)
    {
        ReadHeader();

        if (repeat_count > 0)
        {
            size_t nskip = (repeat_count < to_skip) ? repeat_count : to_skip;
            repeat_count -= nskip;
            to_skip -= nskip;
            if (current_value != 0)
            {
                set_count += nskip;
            }
        }
        else
        {
            chassert(literal_count > 0);
            size_t nskip = (literal_count < to_skip) ? literal_count : to_skip;
            literal_count -= nskip;
            to_skip -= nskip;
            for (; nskip > 0; nskip--)
            {
                T value;
                bit_reader.GetValue(bit_width, &value);
                if (value != 0)
                {
                    set_count++;
                }
            }
        }
    }
    return set_count;
}

template <typename T>
inline bool RleDecoder<T>::Get(T * val)
{
    if (!ReadHeader())
    {
        return false;
    }

    if (repeat_count > 0)
    {
        *val = current_value;
        --repeat_count;
        rewind_state = REWIND_RUN;
    }
    else
    {
        chassert(literal_count > 0);
        bit_reader.GetValue(bit_width, val);
        --literal_count;
        rewind_state = REWIND_LITERAL;
    }

    return true;
}

template <typename T>
inline void RleDecoder<T>::RewindOne()
{
    switch (rewind_state)
    {
        case CANT_REWIND:
            //            LOG(FATAL) << "Can't rewind more than once after each read!";
            break;
        case REWIND_RUN:
            ++repeat_count;
            break;
        case REWIND_LITERAL: {
            bit_reader.Rewind(bit_width);
            ++literal_count;
            break;
        }
    }

    rewind_state = CANT_REWIND;
}

template <typename T>
inline size_t RleDecoder<T>::GetNextRun(T * val, size_t max_run)
{
    chassert(max_run > 0);
    size_t ret = 0;
    size_t rem = max_run;
    while (ReadHeader())
    {
        if (repeat_count > 0)
        {
            if (ret > 0 && *val != current_value)
            {
                return ret;
            }
            *val = current_value;
            if (repeat_count >= rem)
            {
                // The next run is longer than the amount of remaining data
                // that the caller wants to read. Only consume it partially.
                repeat_count -= rem;
                ret += rem;
                return ret;
            }
            ret += repeat_count;
            rem -= repeat_count;
            repeat_count = 0;
        }
        else
        {
            chassert(literal_count > 0);
            if (ret == 0)
            {
                bit_reader.GetValue(bit_width, val);
                literal_count--;
                ret++;
                rem--;
            }

            while (literal_count > 0)
            {
                bit_reader.GetValue(bit_width, &current_value);
                if (current_value != *val || rem == 0)
                {
                    bit_reader.Rewind(bit_width);
                    return ret;
                }
                ret++;
                rem--;
                literal_count--;
            }
        }
    }
    return ret;
}

template <typename T>
inline size_t RleDecoder<T>::GetBatch(T * vals, size_t batch_num)
{
    size_t read_num = 0;
    while (read_num < batch_num)
    {
        size_t read_this_time = batch_num - read_num;

        if (repeat_count > 0)
        {
            read_this_time = std::min((size_t)repeat_count, read_this_time);
            std::fill(vals, vals + read_this_time, current_value);
            vals += read_this_time;
            repeat_count -= read_this_time;
            read_num += read_this_time;
        }
        else if (literal_count > 0)
        {
            read_this_time = std::min((size_t)literal_count, read_this_time);
            for (size_t i = 0; i < read_this_time; ++i)
            {
                bit_reader.GetValue(bit_width, vals);
                vals++;
            }
            literal_count -= read_this_time;
            read_num += read_this_time;
        }
        else
        {
            if (!ReadHeader())
            {
                return read_num;
            }
        }
    }
    return read_num;
}

template <typename T>
inline bool RleDecoder<T>::ReadHeader()
{
    if (literal_count == 0 && repeat_count == 0)
    {
        // Read the next run's indicator int, it could be a literal or repeated run
        // The int is encoded as a vlq-encoded value.
        uint32_t indicator_value = 0;
        bool result = bit_reader.GetVlqInt(&indicator_value);
        if (!result)
        {
            return false;
        }

        // lsb indicates if it is a literal run or repeated run
        bool is_literal = indicator_value & 1;
        if (is_literal)
        {
            literal_count = (indicator_value >> 1) * 8;
            chassert(literal_count > 0);
        }
        else
        {
            repeat_count = indicator_value >> 1;
            chassert(repeat_count > 0);
            result = bit_reader.GetAligned<T>(Ceil(bit_width, 8), reinterpret_cast<T *>(&current_value));
            chassert(result);
        }
    }
    return true;
}

template <typename T>
inline int32_t RleBatchDecoder<T>::OutputBufferedLiterals(int32_t max_to_output, T * values)
{
    int32_t num_to_output = std::min<int32_t>(max_to_output, num_buffered_literals_ - literal_buffer_pos_);
    memcpy(values, &literal_buffer_[literal_buffer_pos_], sizeof(T) * num_to_output);
    literal_buffer_pos_ += num_to_output;
    literal_count_ -= num_to_output;
    return num_to_output;
}

template <typename T>
inline void RleBatchDecoder<T>::Reset(uint8_t * buffer, int buffer_len, int bit_width)
{
    bit_reader_.reset(buffer, buffer_len);
    bit_width_ = bit_width;
    repeat_count_ = 0;
    literal_count_ = 0;
    num_buffered_literals_ = 0;
    literal_buffer_pos_ = 0;
}

template <typename T>
inline int32_t RleBatchDecoder<T>::NextNumRepeats()
{
    if (repeat_count_ > 0)
        return repeat_count_;
    if (literal_count_ == 0)
        NextCounts();
    return repeat_count_;
}

template <typename T>
inline void RleBatchDecoder<T>::NextCounts()
{
    // Read the next run's indicator int, it could be a literal or repeated run.
    // The int is encoded as a ULEB128-encoded value.
    uint32_t indicator_value = 0;
    if (unlikely(!bit_reader_.get_lleb_128<uint32_t>(&indicator_value)))
    {
        return;
    }

    // lsb indicates if it is a literal run or repeated run
    bool is_literal = indicator_value & 1;

    // Don't try to handle run lengths that don't fit in an int32_t - just fail gracefully.
    // The Parquet standard does not allow longer runs - see PARQUET-1290.
    uint32_t run_len = indicator_value >> 1;
    if (is_literal)
    {
        // Use int64_t to avoid overflowing multiplication.
        int64_t literal_count = static_cast<int64_t>(run_len) * 8;
        if (unlikely(literal_count > std::numeric_limits<int32_t>::max()))
            return;
        literal_count_ = literal_count;
    }
    else
    {
        if (unlikely(run_len == 0))
            return;
        bool result = bit_reader_.get_bytes<T>(Ceil(bit_width_, 8), &repeated_value_);
        if (unlikely(!result))
            return;
        repeat_count_ = run_len;
    }
}

template <typename T>
inline T RleBatchDecoder<T>::GetRepeatedValue(int32_t num_repeats_to_consume)
{
    repeat_count_ -= num_repeats_to_consume;
    return repeated_value_;
}

template <typename T>
inline int32_t RleBatchDecoder<T>::NextNumLiterals()
{
    if (literal_count_ > 0)
        return literal_count_;
    if (repeat_count_ == 0)
        NextCounts();
    return literal_count_;
}

template <typename T>
inline bool RleBatchDecoder<T>::GetLiteralValues(int32_t num_literals_to_consume, T * values)
{
    int32_t num_consumed = 0;
    // Copy any buffered literals left over from previous calls.
    if (HaveBufferedLiterals())
    {
        num_consumed = OutputBufferedLiterals(num_literals_to_consume, values);
    }

    int32_t num_remaining = num_literals_to_consume - num_consumed;
    // Copy literals directly to the output, bypassing 'literal_buffer_' when possible.
    // Need to round to a batch of 32 if the caller is consuming only part of the current
    // run avoid ending on a non-byte boundary.
    int32_t num_to_bypass = std::min<int32_t>(literal_count_, RoundDownToPowerOf2(num_remaining, 32));
    if (num_to_bypass > 0)
    {
        int num_read = bit_reader_.unpack_batch(bit_width_, num_to_bypass, values + num_consumed);
        // If we couldn't read the expected number, that means the input was truncated.
        if (num_read < num_to_bypass)
            return false;
        literal_count_ -= num_to_bypass;
        num_consumed += num_to_bypass;
        num_remaining = num_literals_to_consume - num_consumed;
    }

    if (num_remaining > 0)
    {
        // We weren't able to copy all the literals requested directly from the input.
        // Buffer literals and copy over the requested number.
        if (unlikely(!FillLiteralBuffer()))
            return false;
        OutputBufferedLiterals(num_remaining, values + num_consumed);
    }
    return true;
}

template <typename T>
inline bool RleBatchDecoder<T>::FillLiteralBuffer()
{
    int32_t num_to_buffer = std::min<int32_t>(LITERAL_BUFFER_LEN, literal_count_);
    num_buffered_literals_ = bit_reader_.unpack_batch(bit_width_, num_to_buffer, literal_buffer_);
    // If we couldn't read the expected number, that means the input was truncated.
    if (unlikely(num_buffered_literals_ < num_to_buffer))
        return false;
    literal_buffer_pos_ = 0;
    return true;
}


template <typename T>
int32_t RleBatchDecoder<T>::GetBatch(T * values, int32_t batch_num)
{
    int32_t num_consumed = 0;
    while (num_consumed < batch_num)
    {
        // Add RLE encoded values by repeating the current value this number of times.
        int32_t num_repeats = NextNumRepeats();
        if (num_repeats > 0)
        {
            int32_t num_repeats_to_set = std::min(num_repeats, batch_num - num_consumed);
            T repeated_value = GetRepeatedValue(num_repeats_to_set);
            for (int i = 0; i < num_repeats_to_set; ++i)
            {
                values[num_consumed + i] = repeated_value;
            }
            num_consumed += num_repeats_to_set;
            continue;
        }

        // Add remaining literal values, if any.
        int32_t num_literals = NextNumLiterals();
        if (num_literals == 0)
        {
            break;
        }
        int32_t num_literals_to_set = std::min(num_literals, batch_num - num_consumed);
        if (!GetLiteralValues(num_literals_to_set, values + num_consumed))
        {
            return 0;
        }
        num_consumed += num_literals_to_set;
    }
    return num_consumed;
}
}

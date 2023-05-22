#pragma once

#include <IO/readFloatText.h>


namespace local_engine
{


inline bool checkNumberComma(DB::ReadBuffer & buf, const DB::FormatSettings & settings)
{
    if (settings.csv.delimiter == ',' && (!settings.csv.allow_single_quotes && !settings.csv.allow_double_quotes))
    {
        return false;
    }

    /// if has ',', next ',' must has 3 digits, eg:  1,000 / 11,000,000
    /// error is 11,00 / 1,0
    if ((buf.position() + 4 <= buf.buffer().end() && isNumericASCII(buf.position()[1]) && isNumericASCII(buf.position()[2])
         && isNumericASCII(buf.position()[3]) && !isNumericASCII(buf.position()[4]))
        || (buf.position() + 3 == buf.buffer().end() && isNumericASCII(buf.position()[1]) && isNumericASCII(buf.position()[2])
            && isNumericASCII(buf.position()[3])))
    {
        ++buf.position();
        return true;
    }
    else
        return false;
}

template <size_t N, bool before_point = false, typename T>
static inline bool readUIntTextUpToNSignificantDigits(T & x, DB::ReadBuffer & buf, const DB::FormatSettings & settings)
{
    /// In optimistic case we can skip bound checking for first loop.
    if (buf.position() + N <= buf.buffer().end())
    {
        for (size_t i = 0; i < N; ++i)
        {
            if (isNumericASCII(*buf.position()))
            {
                x *= 10;
                x += *buf.position() & 0x0F;
                ++buf.position();
            }
            else if constexpr (before_point) // 10,000,000
            {
                if (*buf.position() == ',' && !checkNumberComma(buf, settings))
                    return true;
            }
            else
                return true;
        }
    }
    else
    {
        for (size_t i = 0; i < N; ++i)
        {
            if (!buf.eof() && isNumericASCII(*buf.position()))
            {
                x *= 10;
                x += *buf.position() & 0x0F;
                ++buf.position();
            }
            else if constexpr (before_point) // 10,000,000
            {
                if (*buf.position() == ',' && !checkNumberComma(buf, settings))
                    return true;
            }
            else
                return true;
        }
    }

    while (!buf.eof() && (buf.position() + 8 <= buf.buffer().end()) && DB::is_made_of_eight_digits_fast(buf.position()))
    {
        buf.position() += 8;
    }

    while (!buf.eof() && isNumericASCII(*buf.position()))
        ++buf.position();

    return true;
}


template <typename T>
inline bool readGlutenFloatTextFastImpl(T & x, DB::ReadBuffer & in, const DB::FormatSettings & settings)
{
    static_assert(std::is_same_v<T, double> || std::is_same_v<T, float>, "Argument for readFloatTextImpl must be float or double");
    static_assert('a' > '.' && 'A' > '.' && '\n' < '.' && '\t' < '.' && '\'' < '.' && '"' < '.', "Layout of char is not like ASCII");

    bool negative = false;
    x = 0;
    UInt64 before_point = 0;
    UInt64 after_point = 0;
    int after_point_exponent = 0;
    int exponent = 0;

    if (in.eof())
        return false;


    if (*in.position() == '-')
    {
        negative = true;
        ++in.position();
    }
    else if (*in.position() == '+')
        ++in.position();

    auto count_after_sign = in.count();

    constexpr int significant_digits = std::numeric_limits<UInt64>::digits10;
    if (!local_engine::readUIntTextUpToNSignificantDigits<significant_digits, true>(before_point, in, settings))
        return false;

    size_t read_digits = in.count() - count_after_sign;

    if (unlikely(read_digits > significant_digits))
    {
        int before_point_additional_exponent = static_cast<int>(read_digits) - significant_digits;
        x = static_cast<T>(shift10(before_point, before_point_additional_exponent));
    }
    else
    {
        x = before_point;

        /// Shortcut for the common case when there is an integer that fit in Int64.
        if (read_digits && (in.eof() || *in.position() < '.'))
        {
            if (negative)
                x = -x;
            return true;
        }
    }

    if (checkChar('.', in))
    {
        auto after_point_count = in.count();

        while (!in.eof() && *in.position() == '0')
            ++in.position();

        auto after_leading_zeros_count = in.count();
        int after_point_num_leading_zeros = static_cast<int>(after_leading_zeros_count - after_point_count);

        local_engine::readUIntTextUpToNSignificantDigits<significant_digits>(after_point, in, settings);
        read_digits = in.count() - after_leading_zeros_count;
        after_point_exponent
            = (read_digits > significant_digits ? -significant_digits : static_cast<int>(-read_digits)) - after_point_num_leading_zeros;
    }

    if (checkChar('e', in) || checkChar('E', in))
    {
        if (in.eof())
            return false;


        bool exponent_negative = false;
        if (*in.position() == '-')
        {
            exponent_negative = true;
            ++in.position();
        }
        else if (*in.position() == '+')
        {
            ++in.position();
        }

        local_engine::readUIntTextUpToNSignificantDigits<4>(exponent, in, settings);
        if (exponent_negative)
            exponent = -exponent;
    }

    if (after_point)
        x += static_cast<T>(shift10(after_point, after_point_exponent));

    if (exponent)
        x = static_cast<T>(shift10(x, exponent));

    if (negative)
        x = -x;

    auto num_characters_without_sign = in.count() - count_after_sign;

    /// Denormals. At most one character is read before denormal and it is '-'.
    if (num_characters_without_sign == 0)
    {
        if (in.eof())
            return false;

        if (*in.position() == '+')
        {
            ++in.position();
            if (in.eof())
                return false;
            else if (negative)
                return false;
        }

        if (*in.position() == 'i' || *in.position() == 'I')
        {
            if (assertOrParseInfinity<false>(in))
            {
                x = std::numeric_limits<T>::infinity();
                if (negative)
                    x = -x;
                return true;
            }
            return false;
        }
        else if (*in.position() == 'n' || *in.position() == 'N')
        {
            if (assertOrParseNaN<false>(in))
            {
                x = std::numeric_limits<T>::quiet_NaN();
                if (negative)
                    x = -x;
                return true;
            }
            return false;
        }
    }

    return true;
}


template <typename T>
bool readGlutenIntTextImpl(T & x, DB::ReadBuffer & buf, const DB::FormatSettings & settings)
{
    using UnsignedT = make_unsigned_t<T>;

    bool negative = false;
    UnsignedT res{};
    if (buf.eof())
        return false;

    /// '+' or '-'
    bool has_sign = false;
    bool has_number = false;
    while (!buf.eof())
    {
        if (*buf.position() == '+')
        {
            /// 123+ or +123+, just stop after 123 or +123.
            if (has_number)
                break;

            /// No digits read yet, but we already read sign, like ++, -+.
            if (has_sign)
                return false;

            has_sign = true;
            ++buf.position();
        }
        else if (*buf.position() == '-')
        {
            if (has_number)
                break;

            if (has_sign)
                return false;

            if constexpr (is_signed_v<T>)
                negative = true;
            else
                return false;

            has_sign = true;
            ++buf.position();
        }
        else if (*buf.position() == ',' && !checkNumberComma(buf, settings))
        {
            /// if 1,00010,  invalidate, fallback
            break;
        }
        else if (*buf.position() >= '0' && *buf.position() <= '9')
        {
            has_number = true;
            res *= 10;
            res += *buf.position() - '0';
            ++buf.position();
        }
        else
            break;
    }

    if (has_sign && !has_number)
        return false;

    x = res;
    if constexpr (is_signed_v<T>)
    {
        if (negative)
        {
            x = -res;
        }
    }

    return true;
}
}

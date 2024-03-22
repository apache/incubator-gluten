/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <IO/PeekableReadBuffer.h>
#include <IO/readFloatText.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_PARSE_NUMBER;
}
}

namespace local_engine
{

static String zh_cn_symbol = "¥"; // std::use_facet<std::moneypunct<char>>(std::locale("zh_cn.utf8")).curr_symbol();
static String en_us_symbol = "$"; // std::use_facet<std::moneypunct<char>>(std::locale("en_US.utf8")).curr_symbol();


inline bool checkMoneySymbol(DB::ReadBuffer & buf, String & symbol)
{
    auto pr = static_cast<DB::PeekableReadBuffer>(buf);
    DB::PeekableReadBufferCheckpoint checkpoint{pr, false};

    std::string::iterator it = symbol.begin();
    while (it != symbol.end())
    {
        if (pr.eof() || *pr.position() != *it)
        {
            pr.rollbackToCheckpoint();
            return false;
        }

        pr.position()++;
        it++;
    }

    return true;
}

inline bool checkMoneySymbol(DB::ReadBuffer & buf)
{
    return checkMoneySymbol(buf, zh_cn_symbol) || checkMoneySymbol(buf, en_us_symbol);
}

inline bool checkNumberComma(DB::ReadBuffer & buf, bool has_quote, const DB::FormatSettings & settings)
{
    if (!has_quote && settings.csv.delimiter == ',')
        return false;

    /// if has ',', next ',' must has 3 digits, eg:  1,000 / 11,000,000
    /// error is 11,00 / 1,0
    if ((buf.position() + 4 <= buf.buffer().end() && isNumericASCII(buf.position()[1]) && isNumericASCII(buf.position()[2])
         && isNumericASCII(buf.position()[3]) && !isNumericASCII(buf.position()[4]))
        || (buf.position() + 3 == buf.buffer().end() && isNumericASCII(buf.position()[1]) && isNumericASCII(buf.position()[2])
            && isNumericASCII(buf.position()[3])))
    {
        return true;
    }
    else
        return false;
}

template <size_t N, bool before_point = false, typename T>
static inline bool readUIntTextUpToNSignificantDigits(T & x, DB::ReadBuffer & buf, bool has_quote, const DB::FormatSettings & settings)
{
    bool has_values = false;
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
                has_values = true;
            }
            else if constexpr (before_point) // 10,000,000
            {
                if (!has_values)
                    return false;

                if (*buf.position() == ',' && checkNumberComma(buf, has_quote, settings))
                {
                    ++buf.position();
                    continue;
                }
                else
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
                has_values = true;
            }
            else if constexpr (before_point) // 10,000,000
            {
                if (!has_values)
                    return false;

                if (*buf.position() == ',' && checkNumberComma(buf, has_quote, settings))
                {
                    ++buf.position();
                    continue;
                }
                else
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
inline bool readExcelFloatTextFastImpl(T & x, DB::ReadBuffer & in, bool has_quote, const DB::FormatSettings & settings)
{
    static_assert(std::is_same_v<T, double> || std::is_same_v<T, float>, "Argument for readFloatTextImpl must be float or double");
    static_assert('a' > '.' && 'A' > '.' && '\n' < '.' && '\t' < '.' && '\'' < '.' && '"' < '.', "Layout of char is not like ASCII");

    const UInt8 MAX_HEAD_SKIP = 2;
    const UInt8 MAX_TAIL_SKIP = 2;
    UInt8 head_skip = 0;
    UInt8 tail_skip = 0;

    bool negative = false;
    x = 0;
    UInt64 before_point = 0;
    UInt64 after_point = 0;
    int after_point_exponent = 0;
    int exponent = 0;

    if (in.eof())
        return false;

    while (!in.eof())
    {

        if ((*in.position() < '0' || *in.position() > '9') && *in.position() != '-' && *in.position() != '+' && *in.position() != '.'
            && !checkMoneySymbol(in))
        {
            if (!((static_cast<UInt8>(*in.position()) & 0b11000000u) == 0b10000000u)) // learn from UTF8Helpers.h
            {
                head_skip++;
                if (head_skip > MAX_HEAD_SKIP)
                    return false;
            }
            ++in.position();
        }
        else
            break ;
    }


    if (*in.position() == '-')
    {
        negative = true;
        ++in.position();
    }
    else if (*in.position() == '+')
        ++in.position();

    auto count_after_sign = in.count();

    constexpr int significant_digits = std::numeric_limits<UInt64>::digits10;
    if (!local_engine::readUIntTextUpToNSignificantDigits<significant_digits, true>(before_point, in, has_quote, settings))
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
    }

    if (checkChar('.', in))
    {
        auto after_point_count = in.count();

        while (!in.eof() && *in.position() == '0')
            ++in.position();

        auto after_leading_zeros_count = in.count();
        int after_point_num_leading_zeros = static_cast<int>(after_leading_zeros_count - after_point_count);

        local_engine::readUIntTextUpToNSignificantDigits<significant_digits>(after_point, in, has_quote, settings);
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

        local_engine::readUIntTextUpToNSignificantDigits<4>(exponent, in, has_quote, settings);
        if (exponent_negative)
            exponent = -exponent;
    }


    if (!(*in.position() >= '0' && *in.position() <= '9')) // process suffix
    {
        while (!in.eof())
        {
            if(*in.position() == settings.csv.delimiter ||*in.position() == '\'' ||*in.position() == '\"'
                || *in.position() == '\n' || *in.position() == '\r')
            {
                break;
            }
            if (!((static_cast<UInt8>(*in.position()) & 0b11000000u) == 0b10000000u)) // learn from UTF8Helpers.h
            {
                tail_skip++;
                if (tail_skip>MAX_TAIL_SKIP)
                    return false;
            }
            ++in.position();
        }
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
bool readExcelIntTextImpl(T & x, DB::ReadBuffer & buf, bool has_quote, const DB::FormatSettings & settings)
{
    bool number_force = settings.try_infer_integers==1;
    const UInt8 MAX_HEAD_SKIP = 2;
    const UInt8 MAX_TAIL_SKIP = 2;
    UInt8 head_skip=0;
    UInt8 tail_skip=0;
    
    using UnsignedT = make_unsigned_t<T>;

    bool negative = false;
    UnsignedT res{};
    if (buf.eof())
        return false;

    /// '+' or '-'
    bool has_sign = false;
    bool has_number = false;
    UInt32 length = 0;
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
        else if (*buf.position() == ',')
        {
            /// invalidate like 1,00010
            if (checkNumberComma(buf, has_quote, settings))
            {
                ++buf.position();
                continue;
            }
            else
                break;
        }
        else if (*buf.position() == '.')
        {
            ++buf.position();
            if (has_number)
            {
                while (!buf.eof())
                {
                    if (!(*buf.position() >= '0' && *buf.position() <= '9'))
                    {
                        if (number_force)
                            break;
                        else
                            return false;
                    }
                    else
                    {
                        ++buf.position();
                    }
                }
            }
            else
                return false;
        }
        else if (*buf.position() >= '0' && *buf.position() <= '9')
        {
            has_number = true;
            ++length;
            if (length >= std::numeric_limits<T>::max_digits10)
            {
                if (negative)
                {
                    T signed_res = -res;
                    if (common::mulOverflow<T>(signed_res, 10, signed_res)
                        || common::subOverflow<T>(signed_res, (*buf.position() - '0'), signed_res))
                        return false;

                    res = -static_cast<UnsignedT>(signed_res);
                }
                else
                {
                    T signed_res = res;
                    if (common::mulOverflow<T>(signed_res, 10, signed_res)
                        || common::addOverflow<T>(signed_res, (*buf.position() - '0'), signed_res))
                        return false;

                    res = signed_res;
                }
            }
            else
            {
                res *= 10;
                res += *buf.position() - '0';
            }

            ++buf.position();
        }
        else if (!has_number && !has_sign && checkMoneySymbol(buf))
        {
            continue;
        }
        else if (has_number && !(*buf.position() >= '0' && *buf.position() <= '9') && number_force) // process suffix
        {
            while (!buf.eof())
            {
                if(*buf.position() == settings.csv.delimiter ||*buf.position() == '\'' ||*buf.position() == '\"'
                    || *buf.position() == '\n' || *buf.position() == '\r')
                {
                    break;
                }
                if (!((static_cast<UInt8>(*buf.position()) & 0b11000000u) == 0b10000000u)) // learn from UTF8Helpers.h
                {
                    tail_skip++;
                    if (tail_skip>MAX_TAIL_SKIP)
                        return false;
                }
                ++buf.position();
            }
            break;
        }
        else if (!has_number && !(*buf.position() >= '0' && *buf.position() <= '9') && number_force) // process prefix
        {
            if(*buf.position() == settings.csv.delimiter || *buf.position() == '\n' || *buf.position() == '\r')
            {
                break;
            }

            if (!((static_cast<UInt8>(*buf.position()) & 0b11000000u) == 0b10000000u)) // learn from UTF8Helpers.h
            {
                head_skip++;
                if (head_skip>MAX_HEAD_SKIP)
                    return false;
            }
            ++buf.position();
        }
        else
            break;
    }

    if (!has_number)
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

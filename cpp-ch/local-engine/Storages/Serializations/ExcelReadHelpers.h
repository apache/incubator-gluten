#pragma once

#include <Formats/FormatSettings.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/parseDateTimeBestEffort.h>

#include <Common/LocalDate.h>
#include <Common/StringUtils/StringUtils.h>

#include "ExcelNumberReader.h"

namespace local_engine
{

inline size_t readDigits(char * res, size_t max_chars, DB::ReadBuffer & in)
{
    size_t num_chars = 0;
    while (!in.eof() && isNumericASCII(*in.position()) && num_chars < max_chars)
    {
        res[num_chars] = *in.position() - '0';
        ++num_chars;
        ++in.position();
    }
    return num_chars;
}

template <size_t digit, size_t power_of_ten, typename T>
inline void readDecimalNumberImpl(T & res, const char * src)
{
    res += src[digit] * power_of_ten;
    if constexpr (digit > 0)
        readDecimalNumberImpl<digit - 1, power_of_ten * 10>(res, src);
}

template <size_t num_digits, typename T>
inline void readDecimalNumber(T & res, const char * src)
{
    readDecimalNumberImpl<num_digits - 1, 1>(res, src);
}

template <size_t num_digits, typename T>
inline bool readNumber(DB::ReadBuffer & buf, T & res)
{
    char digits[std::numeric_limits<UInt64>::digits10];
    size_t read_num_digits = readDigits(digits, sizeof(digits), buf);

    if (read_num_digits != num_digits)
        return false;

    readDecimalNumber<num_digits>(res, digits);
    return true;
}


bool readDatetime64TextWithExcel(
    DB::DateTime64 & datetime64,
    UInt32 scale,
    DB::ReadBuffer & buf,
    const DateLUTImpl & time_zone,
    const DB::FormatSettings::CSV & settings,
    bool quote);
void readDateTime64Text(
    DB::DateTime64 & x,
    DB::ReadBuffer & buf,
    const DB::FormatSettings & settings,
    const DateLUTImpl & time_zone,
    const DateLUTImpl & utc_time_zone,
    bool quote);

bool readDateTextWithExcel(LocalDate & date, DB::ReadBuffer & buf, bool is_us_style);
void readGlutenDateText(LocalDate & date, DB::ReadBuffer & buf, const DB::FormatSettings & settings);

template <typename T>
inline void readGlutenFloatText(T & x, DB::ReadBuffer & buf, bool has_quote, const DB::FormatSettings & settings)
{
    {
        char * pos = buf.position();
        if (local_engine::readGlutenFloatTextFastImpl(x, buf, has_quote, settings))
            return;
        else
            buf.position() = pos;

        DB::readFloatTextFast(x, buf);
    }
}


template <typename T>
inline void readGlutenIntegerText(T & x, DB::ReadBuffer & buf, bool has_quote, const DB::FormatSettings & settings)
{
    if constexpr (std::is_same_v<decltype(x), bool &>)
        readBoolText(x, buf);
    else
        readGlutenIntTextImpl(x, buf, has_quote, settings);
}

inline void readGlutenText(is_floating_point auto & x, DB::ReadBuffer & buf, bool has_quote, const DB::FormatSettings & settings)
{
    readGlutenFloatText(x, buf, has_quote, settings);
}

inline void readGlutenText(is_integer auto & x, DB::ReadBuffer & buf, bool has_quote, const DB::FormatSettings & settings)
{
    readGlutenIntegerText(x, buf, has_quote, settings);
}

inline void readGlutenText(LocalDate & x, DB::ReadBuffer & buf, bool /*has_quote*/, const DB::FormatSettings & settings)
{
    readGlutenDateText(x, buf, settings);
}

/// CSV, for numbers, dates: quotes are optional, no special escaping rules.
template <typename T>
inline void readCSVSimple(T & x, DB::ReadBuffer & buf, const DB::FormatSettings & settings)
{
    if (buf.eof())
        DB::throwReadAfterEOF();

    char maybe_quote = *buf.position();
    bool has_quote = false;
    if (maybe_quote == '\'' || maybe_quote == '\"')
    {
        has_quote = true;
        ++buf.position();
    }

    readGlutenText(x, buf, has_quote, settings);

    if (maybe_quote == '\'' || maybe_quote == '\"')
        assertChar(maybe_quote, buf);
}


template <typename T>
    requires is_arithmetic_v<T>
inline void readCSV(T & x, DB::ReadBuffer & buf, const DB::FormatSettings & settings)
{
    readCSVSimple(x, buf, settings);
}

inline void readCSV(LocalDate & x, DB::ReadBuffer & buf, const DB::FormatSettings & settings)
{
    readCSVSimple(x, buf, settings);
}

}

#pragma once

#include <Columns/ColumnDecimal.h>
#include <IO/ReadHelpers.h>
#include <IO/readDecimalText.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_PARSE_NUMBER;
}
}

namespace local_engine
{
using namespace DB;

template <typename T>
bool readExcelCSVDecimalText(ReadBuffer & buf, T & x, uint32_t precision, uint32_t & scale, const FormatSettings & format_settings)
{
    char maybe_quote = *buf.position();
    bool has_quote = false;
    if (maybe_quote == '\'' || maybe_quote == '\"')
    {
        ++buf.position();
        has_quote = true;
    }

    /// deal empty string ""
    if ((has_quote && !buf.eof() && *buf.position() == maybe_quote)
        || (!has_quote && !buf.eof()
            && (*buf.position() == format_settings.csv.delimiter || *buf.position() == '\n' || *buf.position() == '\r')))
        return false;

    bool result = readDecimalText<T, bool>(buf, x, precision, scale, false);

    if (!result)
        return false;

    if (has_quote)
        assertChar(maybe_quote, buf);

    if (!buf.eof() && (*buf.position() != format_settings.csv.delimiter && *buf.position() != '\n' && *buf.position() != '\r'))
        return false;

    return true;
}

template <typename T>
void deserializeExcelDecimalText(IColumn & column, ReadBuffer & istr, UInt32 precision, UInt32 scale, const FormatSettings & formatSettings)
{
    if (istr.eof())
        throwReadAfterEOF();

    T x;
    UInt32 unread_scale = scale;
    bool result = readExcelCSVDecimalText(istr, x, precision, unread_scale, formatSettings);
    if (!result || common::mulOverflow(x.value, DecimalUtils::scaleMultiplier<T>(unread_scale), x.value))
        throw DB::Exception(DB::ErrorCodes::INCORRECT_DATA, "Read error");
    else
        assert_cast<ColumnDecimal<T> &>(column).getData().push_back(x);
}
}

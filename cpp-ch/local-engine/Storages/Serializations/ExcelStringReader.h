#pragma once


#include <Columns/ColumnString.h>
#include <Columns/IColumn.h>
#include <Formats/FormatSettings.h>
#include <IO/ReadBuffer.h>


namespace local_engine
{
using namespace DB;


template <typename Reader>
static inline void excelRead(IColumn & column, Reader && reader)
{
    ColumnString & column_string = assert_cast<ColumnString &>(column);
    ColumnString::Chars & data = column_string.getChars();
    ColumnString::Offsets & offsets = column_string.getOffsets();
    size_t old_chars_size = data.size();
    size_t old_offsets_size = offsets.size();
    try
    {
        reader(data);
        data.push_back(0);
        offsets.push_back(data.size());
    }
    catch (...)
    {
        offsets.resize_assume_reserved(old_offsets_size);
        data.resize_assume_reserved(old_chars_size);
        throw;
    }
}


template <typename Vector, bool include_quotes = false>
void readExcelCSVStringInto(Vector & s, ReadBuffer & buf, const FormatSettings::CSV & settings, const String & escape_value);


void deserializeExcelTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, const String & escape_value);


}

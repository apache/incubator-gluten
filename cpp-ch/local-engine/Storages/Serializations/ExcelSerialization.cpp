#include "ExcelSerialization.h"
#include <Columns/ColumnsNumber.h>
#include <DataTypes/Serializations/SerializationDateTime64.h>
#include <DataTypes/Serializations/SerializationNumber.h>
#include <DataTypes/Serializations/SerializationString.h>
#include "ExcelReadHelpers.h"
#include "ExcelStringReader.h"


namespace local_engine
{


void ExcelSerialization::serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings & settings) const
{
    nested_ptr->serializeBinary(field, ostr, settings);
}

void ExcelSerialization::deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings & settings) const
{
    nested_ptr->deserializeBinary(field, istr, settings);
}

void ExcelSerialization::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    nested_ptr->serializeBinary(column, row_num, ostr, settings);
}

void ExcelSerialization::deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    nested_ptr->deserializeBinary(column, istr, settings);
}

void ExcelSerialization::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    nested_ptr->serializeText(column, row_num, ostr, settings);
}

void ExcelSerialization::deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    nested_ptr->deserializeWholeText(column, istr, settings);
}

void ExcelSerialization::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    nested_ptr->deserializeTextEscaped(column, istr, settings);
}

void ExcelSerialization::serializeTextEscaped(
    const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    nested_ptr->serializeTextEscaped(column, row_num, ostr, settings);
}

void ExcelSerialization::serializeTextQuoted(
    const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    nested_ptr->serializeTextQuoted(column, row_num, ostr, settings);
}

void ExcelSerialization::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    nested_ptr->deserializeTextQuoted(column, istr, settings);
}

void ExcelSerialization::serializeTextJSON(
    const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    nested_ptr->serializeTextJSON(column, row_num, ostr, settings);
}

void ExcelSerialization::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    nested_ptr->deserializeTextJSON(column, istr, settings);
}

void ExcelSerialization::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    nested_ptr->serializeTextCSV(column, row_num, ostr, settings);
}

void ExcelSerialization::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    if (typeid_cast<const DB::SerializationDate32 *>(nested_ptr.get()))
    {
        deserializeDate32TextCSV(column, istr, settings);
    }
    else if (const auto * datetime64 = typeid_cast<const DB::SerializationDateTime64 *>(nested_ptr.get()))
    {
        deserializeDatetimeTextCSV<DB::SerializationDateTime64::ColumnType>(
            column, istr, settings, datetime64->getTimeZone(), DateLUT::instance("UTC"));
    }
    else if (typeid_cast<const SerializationNumber<DB::UInt8> *>(nested_ptr.get()))
    {
        deserializeNumberTextCSV<DB::UInt8>(column, istr, settings);
    }
    else if (typeid_cast<const SerializationNumber<DB::UInt16> *>(nested_ptr.get()))
    {
        deserializeNumberTextCSV<DB::UInt16>(column, istr, settings);
    }
    else if (typeid_cast<const SerializationNumber<DB::UInt32> *>(nested_ptr.get()))
    {
        deserializeNumberTextCSV<DB::UInt32>(column, istr, settings);
    }
    else if (typeid_cast<const SerializationNumber<DB::UInt64> *>(nested_ptr.get()))
    {
        deserializeNumberTextCSV<DB::UInt64>(column, istr, settings);
    }
    else if (typeid_cast<const SerializationNumber<DB::Int8> *>(nested_ptr.get()))
    {
        deserializeNumberTextCSV<DB::Int8>(column, istr, settings);
    }
    else if (typeid_cast<const SerializationNumber<DB::Int16> *>(nested_ptr.get()))
    {
        deserializeNumberTextCSV<DB::Int16>(column, istr, settings);
    }
    else if (typeid_cast<const SerializationNumber<DB::Int32> *>(nested_ptr.get()))
    {
        deserializeNumberTextCSV<DB::Int32>(column, istr, settings);
    }
    else if (typeid_cast<const SerializationNumber<DB::Int64> *>(nested_ptr.get()))
    {
        deserializeNumberTextCSV<DB::Int64>(column, istr, settings);
    }
    else if (typeid_cast<const SerializationNumber<DB::Float32> *>(nested_ptr.get()))
    {
        deserializeNumberTextCSV<DB::Float32>(column, istr, settings);
    }
    else if (typeid_cast<const SerializationNumber<DB::Float64> *>(nested_ptr.get()))
    {
        deserializeNumberTextCSV<DB::Float64>(column, istr, settings);
    }
    else if (typeid_cast<const SerializationString *>(nested_ptr.get()))
    {
        deserializeGlutenTextCSV(column, istr, settings, escape);
    }
    else
    {
        nested_ptr->deserializeTextCSV(column, istr, settings);
    }
}

template <typename T>
    requires is_arithmetic_v<T>
void ExcelSerialization::deserializeNumberTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    T x;
    local_engine::readCSV(x, istr, settings);
    assert_cast<ColumnVector<T> &>(column).getData().push_back(x);
}

void ExcelSerialization::deserializeDate32TextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    LocalDate value;
    local_engine::readCSV(value, istr, settings);
    assert_cast<ColumnInt32 &>(column).getData().push_back(value.getExtenedDayNum());
}

template <typename ColumnType>
void ExcelSerialization::deserializeDatetimeTextCSV(
    IColumn & column, ReadBuffer & istr, const FormatSettings & settings, const DateLUTImpl & time_zone, const DateLUTImpl & utc_time_zone)
    const
{
    DateTime64 x = 0;

    if (istr.eof())
        throwReadAfterEOF();

    char maybe_quote = *istr.position();
    bool quote = false;
    if (maybe_quote == '\'' || maybe_quote == '\"')
    {
        quote = true;
        ++istr.position();
    }

    local_engine::readDateTime64Text(x, istr, settings, time_zone, utc_time_zone, quote);

    if (maybe_quote == '\'' || maybe_quote == '\"')
        assertChar(maybe_quote, istr);

    assert_cast<ColumnType &>(column).getData().push_back(x);
}
}

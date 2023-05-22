#pragma once

#include <DataTypes/Serializations/SerializationDate32.h>
#include <DataTypes/Serializations/SerializationNumber.h>
#include <Common/DateLUTImpl.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ATTEMPT_TO_READ_AFTER_EOF;
}
}

namespace local_engine
{
using namespace DB;


class ExcelSerialization final : public DB::ISerialization
{
public:
    explicit ExcelSerialization(const SerializationPtr & nested_) : nested_ptr(nested_){};

    void serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings &) const override;
    void serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    void serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    void serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    void serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    void serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override;

private:
    void deserializeDate32TextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const;

    template <typename T>
        requires is_arithmetic_v<T>
    void deserializeNumberTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings &) const;

    template <typename T>
    void deserializeDatetimeTextCSV(
        IColumn & column,
        ReadBuffer & istr,
        const FormatSettings & settings,
        const DateLUTImpl & time_zone,
        const DateLUTImpl & utc_time_zone) const;

private:
    SerializationPtr nested_ptr;
};
}

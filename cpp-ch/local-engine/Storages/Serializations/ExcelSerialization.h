#pragma once

#include <DataTypes/Serializations/ISerialization.h>
#include <base/extended_types.h>
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
    explicit ExcelSerialization(const SerializationPtr & nested_, String escape_) : nested_ptr(nested_), escape(escape_){};

    void serializeBinary(const Field &, WriteBuffer &, const FormatSettings &) const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented for excel serialization");
    };
    void deserializeBinary(Field &, ReadBuffer &, const FormatSettings &) const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented for excel serialization");
    };
    void serializeBinary(const IColumn &, size_t, WriteBuffer &, const FormatSettings &) const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented for excel serialization");
    };
    void deserializeBinary(IColumn &, ReadBuffer &, const FormatSettings &) const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented for excel serialization");
    };
    void serializeText(const IColumn &, size_t, WriteBuffer &, const FormatSettings &) const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented for excel serialization");
    };
    void deserializeWholeText(IColumn &, ReadBuffer &, const FormatSettings &) const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented for excel serialization");
    };
    void serializeTextEscaped(const IColumn &, size_t, WriteBuffer &, const FormatSettings &) const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented for excel serialization");
    };
    void deserializeTextEscaped(IColumn &, ReadBuffer &, const FormatSettings &) const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented for excel serialization");
    };
    void serializeTextQuoted(const IColumn &, size_t, WriteBuffer &, const FormatSettings &) const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented for excel serialization");
    };
    void deserializeTextQuoted(IColumn &, ReadBuffer &, const FormatSettings &) const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented for excel serialization");
    };
    void serializeTextJSON(const IColumn &, size_t, WriteBuffer &, const FormatSettings &) const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented for excel serialization");
    };
    void deserializeTextJSON(IColumn &, ReadBuffer &, const FormatSettings &) const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented for excel serialization");
    };
    void serializeTextCSV(const IColumn &, size_t, WriteBuffer &, const FormatSettings &) const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented for excel serialization");
    };
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
    String escape;
};
}

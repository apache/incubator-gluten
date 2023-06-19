#pragma once

#include <DataTypes/Serializations/SerializationDate32.h>
#include <DataTypes/Serializations/SerializationNumber.h>
#include <Common/DateLUTImpl.h>

namespace local_engine
{
using namespace DB;

template <is_decimal T>
class ExcelDecimalSerialization final : public DB::ISerialization
{
public:
    explicit ExcelDecimalSerialization(const SerializationPtr & nested_, UInt32 precision_, UInt32 scale_)
        : nested_ptr(nested_), precision(precision_), scale(scale_){};

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

    void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;


private:
    SerializationPtr nested_ptr;
    const UInt32 precision;
    const UInt32 scale;
};
}

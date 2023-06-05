#pragma once

#include <Columns/IColumn.h>
#include <Core/AccurateComparison.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/Field.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int TYPE_MISMATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}
}

namespace local_engine
{
using namespace DB;

template <typename From, typename To>
Field convertNumericTypeImpl(const Field & from)
{
    To result;
    if (!accurate::convertNumeric(from.get<From>(), result))
        return {};
    return result;
}

template <typename To>
Field convertNumericType(const Field & from)
{
    if (from.getType() == Field::Types::UInt64)
        return convertNumericTypeImpl<UInt64, To>(from);
    if (from.getType() == Field::Types::Int64)
        return convertNumericTypeImpl<Int64, To>(from);
    if (from.getType() == Field::Types::UInt128)
        return convertNumericTypeImpl<UInt128, To>(from);
    if (from.getType() == Field::Types::Int128)
        return convertNumericTypeImpl<Int128, To>(from);
    if (from.getType() == Field::Types::UInt256)
        return convertNumericTypeImpl<UInt256, To>(from);
    if (from.getType() == Field::Types::Int256)
        return convertNumericTypeImpl<Int256, To>(from);

    throw Exception(ErrorCodes::TYPE_MISMATCH, "Type mismatch. Expected: Integer. Got: {}", from.getType());
}

inline UInt32 extractArgument(const ColumnWithTypeAndName & named_column)
{
    if (!isColumnConst(*named_column.column.get()))
    {
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument, column type must be const", named_column.type->getName());
    }

    Field from;
    named_column.column->get(0, from);
    Field to = convertNumericType<UInt32>(from);
    if (to.isNull())
    {
        throw Exception(
            ErrorCodes::DECIMAL_OVERFLOW, "{} convert overflow, precision/scale value must in UInt32", named_column.type->getName());
    }
    return static_cast<UInt32>(to.get<UInt32>());
}

}

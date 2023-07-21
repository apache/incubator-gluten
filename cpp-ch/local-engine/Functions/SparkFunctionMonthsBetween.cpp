#include "SparkFunctionMonthsBetween.h"
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeNullable.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionFactory.h>
#include <Functions/TransformDateTime64.h>
#include <Poco/Logger.h>
#include <Common/DateLUT.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include "base/Decimal.h"
#include "base/types.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NOT_IMPLEMENTED;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}
}

namespace local_engine
{
using namespace DB;
DB::DataTypePtr SparkFunctionMonthsBetween::getReturnTypeImpl(const DB::DataTypes & arguments) const
{
    if (arguments.size() != 3 && arguments.size() != 4)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Number of arguments for function {} doesn't match: passed {}, should be 3 or 4",
            getName(), arguments.size());

    if (!isDate(arguments[0]) && !isDate32(arguments[0]) && !isDateTime(arguments[0]) && !isDateTime64(arguments[0]))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "First argument for function {} must be Date, Date32, DateTime or DateTime64",
            getName()
            );

    if (!isDate(arguments[1]) && !isDate32(arguments[1]) && !isDateTime(arguments[1]) && !isDateTime64(arguments[1]))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Second argument for function {} must be Date, Date32, DateTime or DateTime64",
            getName());

    if (arguments.size() == 4 && !isString(arguments[3]))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Fourth argument for function {} (timezone) must be String",
            getName());

    return DB::makeNullableSafe(std::make_shared<DataTypeFloat64>());
}

ALWAYS_INLINE Float64 roundTo8IfNeed(bool round, Float64 res)
{
    return round ? std::round(res * 1e8) / 1e8 : res;
}

Float64 monthsBetween(DateTime64 x, DateTime64 y, const DateLUTImpl & timezone_x, const DateLUTImpl & timezone_y, bool round)
{
    // we know that spark use microseconds, maybe round to 8 digits after point
    x /= 1000000;
    y /= 1000000;
    int x_year = timezone_x.toYear(x);
    int y_year = timezone_y.toYear(y);
    auto x_month = timezone_x.toMonth(x);
    auto y_month = timezone_y.toMonth(y);
    auto x_day = timezone_x.toDayOfMonth(x);
    auto y_day = timezone_y.toDayOfMonth(y);
    auto month_diff = static_cast<Float64>(x_year * 12 + x_month - y_year * 12 - y_month);
    if (x_day == y_day)
        return roundTo8IfNeed(round, month_diff);

    int x_to_month_end = timezone_x.daysInMonth(x);
    x_to_month_end -= x_day;
    int y_to_month_end = timezone_y.daysInMonth(y);
    y_to_month_end -= y_day;
    if (x_to_month_end == 0 && y_to_month_end == 0)
        return roundTo8IfNeed(round, month_diff);

    int day_diff = static_cast<int>(x_day) - y_day;
    auto x_seconds_in_day= x - timezone_x.makeDate(x_year, x_month, x_day);
    auto y_seconds_in_day= y - timezone_y.makeDate(y_year, y_month, y_day);
    auto seconds_diff = x_seconds_in_day - y_seconds_in_day;
    auto res = static_cast<Float64>(day_diff * 86400 + seconds_diff)/2678400.0 + month_diff;
    return roundTo8IfNeed(round, res);
}

DB::ColumnPtr SparkFunctionMonthsBetween::executeImpl(
    const DB::ColumnsWithTypeAndName & arguments, const DB::DataTypePtr & result_type, size_t input_rows_count) const
{
    const IColumn & x = *arguments[0].column;
    const IColumn & y = *arguments[1].column;
    const IColumn & round_off = *arguments[2].column;

    size_t rows = input_rows_count;
    auto res = result_type->createColumn();
    res->reserve(rows);

    const auto & timezone_x = extractTimeZoneFromFunctionArguments(arguments, 3, 0);
    const auto & timezone_y = extractTimeZoneFromFunctionArguments(arguments, 3, 1);

    for (size_t i = 0; i < rows; ++i)
    {
        DB::Field x_value;
        DB::Field y_value;
        DB::Field round_value;
        x.get(i, x_value);
        y.get(i, y_value);
        round_off.get(i, round_value);
        if (x_value.isNull() || y_value.isNull()) [[unlikely]]
            res->insertDefault();
        else
            res->insert(monthsBetween(
                static_cast<DateTime64>(x_value.safeGet<DateTime64>()),
                static_cast<DateTime64>(y_value.safeGet<DateTime64>()),
                timezone_x,
                timezone_y,
                static_cast<bool>(round_value.safeGet<DB::UInt8>())));
    }
    return res;
}

REGISTER_FUNCTION(SparkFunctionMonthsBetween)
{
    factory.registerFunction<SparkFunctionMonthsBetween>();
}
}

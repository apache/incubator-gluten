#include <Columns/ColumnConst.h>
#include <Columns/ColumnsDateTime.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Common/DateLUT.h>
#include <Common/LocalDateTime.h>
#include <Core/DecimalFunctions.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeNullable.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

}

namespace local_engine
{
    template <typename Name>
    class UTCTimestampSparkFunctionBase : public DB::IFunction
    {
    public:
        static constexpr auto name = Name::name;

        String getName() const override { return name; }

        size_t getNumberOfArguments() const override { return 2; }

        bool useDefaultImplementationForConstants() const override { return true; }

        bool isSuitableForShortCircuitArgumentsExecution(const DB::DataTypesWithConstInfo & /*arguments*/) const override { return true; }

        static DB::FunctionPtr create(DB::ContextPtr) { return std::make_shared<UTCTimestampSparkFunctionBase>(); }

        DB::DataTypePtr getReturnTypeImpl(const DB::DataTypes & arguments) const override
        {
            if (arguments.size() < 2)
                throw DB::Exception(DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {}'s arguments number must be 2.", name);

            DB::DataTypePtr first_arg_type = arguments[0];
            DB::DataTypePtr second_arg_type = arguments[1];

            if (first_arg_type->isNullable())
            {
                const DB::DataTypeNullable * null_type = static_cast<const DB::DataTypeNullable *>(first_arg_type.get());
                first_arg_type = null_type->getNestedType();
            }
            
            if (second_arg_type->isNullable())
            {
                const DB::DataTypeNullable * null_type = static_cast<const DB::DataTypeNullable *>(second_arg_type.get());
                second_arg_type = null_type->getNestedType();
            }

            DB::WhichDataType which_type_first(arguments[0]);
            DB::WhichDataType which_type_second(arguments[1]);
            if (!which_type_first.isDateTime() && !which_type_first.isDateTime64())
                throw DB::Exception(DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function {}'s 1st argument type must be datetime.", name);
            
            if (!which_type_second.isString())
                throw DB::Exception(DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function {}'s 2nd argument type must be string.", name);
            
            DB::DataTypePtr date_time_type;
            if (which_type_first.isDateTime())
                date_time_type = std::make_shared<DB::DataTypeDateTime>();
            else
            {
                const DB::DataTypeDateTime64 * date_time_64 = static_cast<const DB::DataTypeDateTime64 *>(first_arg_type.get());
                date_time_type = std::make_shared<DB::DataTypeDateTime64>(date_time_64->getScale());
            }
            return std::make_shared<DB::DataTypeNullable>(date_time_type);
        }

        DB::ColumnPtr executeImpl(const DB::ColumnsWithTypeAndName & arguments, const DB::DataTypePtr & result_type, size_t input_rows_count) const override
        {
            if (arguments.size() < 2)
                throw DB::Exception(DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {}'s arguments number must be 2.", name);
            DB::ColumnWithTypeAndName arg1 = arguments[0];
            DB::ColumnWithTypeAndName arg2 = arguments[1];
            
            if (arg1.column->size() != input_rows_count || arg2.column->size() != input_rows_count)
                throw DB::Exception(
                    DB::ErrorCodes::ILLEGAL_COLUMN, 
                    "The argument column size {} is not match input rows count {}.",
                    arg1.column->size() != input_rows_count ? arg1.column->size() : arg2.column->size(),
                    input_rows_count);
            
            const auto *  time_zone_col = DB::checkAndGetColumn<DB::ColumnString>(arg2.column.get());
            if (!time_zone_col)
            {
                const auto * time_zone_const_col = DB::checkAndGetColumn<DB::ColumnConst>(arg2.column.get());
                if (!time_zone_const_col)
                    throw DB::Exception(DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function {}'s 2nd argument is not string type or const string type.", name);
                time_zone_col = DB::checkAndGetColumn<DB::ColumnString>(time_zone_const_col->getDataColumn());
                if (!time_zone_col)
                    throw DB::Exception(DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function {}'s 2nd argument must be string type or const string type.", name);
            }

            auto column = result_type->createColumn();
            if (DB::WhichDataType(arg1.type).isDateTime())
            {
                const auto * date_time_col = DB::checkAndGetColumn<DB::ColumnDateTime>(arg1.column.get());
                if (!date_time_col)
                {
                    const auto * date_time_const_col = DB::checkAndGetColumn<DB::ColumnConst>(arg1.column.get());
                    if (!date_time_const_col)
                        throw DB::Exception(DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function {}'s 1st argument is not datatime type or const type.", name);
                    date_time_col = DB::checkAndGetColumn<DB::ColumnDateTime>(date_time_const_col->getDataColumn());
                    if (!date_time_col)
                        throw DB::Exception(DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function {}'s 1st argument must be datetime type", name);
                }
                for (size_t i = 0; i < input_rows_count; ++i)
                {
                    DB::Field date_time_val;
                    DB::Field time_zone_val;
                    date_time_col->get(i, date_time_val);
                    time_zone_col->get(i, time_zone_val);
                    LocalDateTime date_time(date_time_val.get<UInt32>(), Name::to ? DateLUT::instance("UTC") : DateLUT::instance(time_zone_val.get<String>()));
                    time_t utc_time_val = date_time.to_time_t(Name::from ? DateLUT::instance("UTC") : DateLUT::instance(time_zone_val.get<String>()));
                    column->insert(DB::Field(utc_time_val));
                }
            }
            else if (DB::WhichDataType(arg1.type).isDateTime64())
            {
                const auto * date_time_col = DB::checkAndGetColumn<DB::ColumnDateTime64>(arg1.column.get());
                if (!date_time_col)
                {
                    const auto * date_time_const_col = DB::checkAndGetColumn<DB::ColumnConst>(arg1.column.get());
                    if (!date_time_const_col)
                        throw DB::Exception(DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function {}'s 1st argument is not datatime64 type or const type.", name);
                    date_time_col = DB::checkAndGetColumn<DB::ColumnDateTime64>(date_time_const_col->getDataColumn());
                    if (!date_time_col)
                        throw DB::Exception(DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function {}'s 1st argument must be datetime64 type", name);
                }
                const DB::DataTypeDateTime64 * date_time_type = static_cast<const DB::DataTypeDateTime64 *>(arg1.type.get());
                for (size_t i = 0; i < input_rows_count; ++i)
                {
                    DB::Field date_time_val;
                    DB::Field time_zone_val;
                    date_time_col->get(i, date_time_val);
                    time_zone_col->get(i, time_zone_val);
                    DB::DateTime64 date_time_64 = date_time_val.get<DB::DateTime64>();
                    Int64 scale_multiplier = DB::DecimalUtils::scaleMultiplier<Int64>(date_time_type->getScale());
                    Int64 seconds = date_time_64.value / scale_multiplier;
                    Int64 micros = date_time_64.value % scale_multiplier;
                    LocalDateTime date_time(seconds, Name::to ? DateLUT::instance("UTC") : DateLUT::instance(time_zone_val.get<String>()));
                    time_t utc_time_val = date_time.to_time_t(Name::from ? DateLUT::instance("UTC") : DateLUT::instance(time_zone_val.get<String>()));
                    DB::DateTime64 utc_time(utc_time_val * scale_multiplier + micros);
                    column->insert(DB::Field(utc_time));
                }
            }
            else
                throw DB::Exception(DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function {}'s 1st argument can only be datetime/datatime64. ", name);
            return column;
        }

    };

    struct NameToUTCTimestamp
    {
        static constexpr auto name = "ToUTCTimestamp";
        static constexpr auto from = false;
        static constexpr auto to = true;
    };

    struct NameFromUTCTimestamp
    {
        static constexpr auto name = "FromUTCTimestamp";
        static constexpr auto from = true;
        static constexpr auto to = false;
    };

    using ToUTCTimestampSparkFunction = UTCTimestampSparkFunctionBase<NameToUTCTimestamp>;
    using FromUTCTimestampSparkFunction = UTCTimestampSparkFunctionBase<NameFromUTCTimestamp>;

    REGISTER_FUNCTION(UTCTimestampSparkFunctionBase)
    {
        factory.registerFunction<ToUTCTimestampSparkFunction>();
        factory.registerFunction<FromUTCTimestampSparkFunction>();
    }
}




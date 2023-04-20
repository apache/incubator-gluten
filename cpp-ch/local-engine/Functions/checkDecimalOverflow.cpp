#include <Columns/ColumnConst.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Core/AccurateComparison.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
    extern const int DECIMAL_OVERFLOW;
    extern const int TYPE_MISMATCH;
}
}


namespace local_engine
{
using namespace DB;

struct CheckDecimalOverflow
{
    static constexpr auto name = "checkDecimalOverflow";
};
struct CheckDecimalOverflowOrNull
{
    static constexpr auto name = "checkDecimalOverflowOrNull";
};

enum class CheckExceptionMode
{
    Throw, /// Throw exception if value cannot be parsed.
    Null /// Return ColumnNullable with NULLs when value cannot be parsed.
};

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
    Field from;
    named_column.column->get(0, from);
    Field to = convertNumericType<UInt32>(from);
    if (to.isNull())
    {
        throw Exception(ErrorCodes::DECIMAL_OVERFLOW, "{} convert overflow, precision value must in UInt32", named_column.type->getName());
    }
    return static_cast<UInt32>(to.get<UInt32>());
}

namespace
{
    /// Returns received decimal value if and Decimal value has less digits then it's Precision allow, 0 otherwise.
    /// Precision could be set as second argument or omitted. If omitted function uses Decimal precision of the first argument.
    template <typename Name, CheckExceptionMode mode>
    class FunctionCheckDecimalOverflow : public IFunction
    {
    public:
        static constexpr auto name = Name::name;
        static constexpr auto exception_mode = mode;

        static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionCheckDecimalOverflow>(); }

        String getName() const override { return name; }
        size_t getNumberOfArguments() const override { return 3; }
        bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

        DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
        {
            if (arguments.size() != 3)
                throw Exception(
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                    "Number of arguments for function {} doesn't match: passed {}, should be 3.",
                    getName(),
                    arguments.size());

            if (!isDecimalOrNullableDecimal(arguments[0]) || !isInteger(arguments[1]) || !isInteger(arguments[2]))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} {} {} of argument of function {}",
                    arguments[0]->getName(),
                    arguments[1]->getName(),
                    arguments[2]->getName(),
                    getName());

            if constexpr (exception_mode == CheckExceptionMode::Null)
            {
                if (!arguments[0]->isNullable())
                    return std::make_shared<DataTypeNullable>(arguments[0]);
            }

            return arguments[0];
        }

        ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
        {
            if (!arguments[0].column || !arguments[1].column || !arguments[2].column)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal column while execute function {}", getName());

            const auto & src_column = arguments[0];
            UInt32 precision = extractArgument(arguments[1]);
            UInt32 scale = extractArgument(arguments[2]);

            ColumnPtr result_column;

            auto call = [&](const auto & types) -> bool
            {
                using Types = std::decay_t<decltype(types)>;
                using Type = typename Types::RightType;
                using ColVecType = ColumnDecimal<Type>;

                if (const ColumnConst * const_column = checkAndGetColumnConst<ColVecType>(src_column.column.get()))
                {
                    execute<Type>(
                        *checkAndGetColumn<ColVecType>(&const_column->getDataColumn()), result_column, input_rows_count, precision, scale);
                    result_column = ColumnConst::create(result_column, result_column->size());
                    return true;
                }
                else if (const ColVecType * col_vec = checkAndGetColumn<ColVecType>(src_column.column.get()))
                {
                    execute<Type>(*col_vec, result_column, input_rows_count, precision, scale);
                    return true;
                }

                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal column while execute function {}", getName());
            };

            callOnBasicType<void, false, false, true, false>(src_column.type->getTypeId(), call);
            if (!result_column)
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Wrong call for {} with {}", getName(), src_column.type->getName());

            return result_column;
        }

    private:
        template <typename T>
        static void
        execute(const ColumnDecimal<T> & col_source, ColumnPtr & result_column, size_t input_rows_count, UInt32 precision, UInt32 scale_to)
        {
            ColumnUInt8::MutablePtr col_null_map_to;
            ColumnUInt8::Container * vec_null_map_to [[maybe_unused]] = nullptr;
            auto scale_from = col_source.getScale();

            if constexpr (exception_mode == CheckExceptionMode::Null)
            {
                col_null_map_to = ColumnUInt8::create(input_rows_count, false);
                vec_null_map_to = &col_null_map_to->getData();
            }

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                bool overflow = outOfDigits<T>(col_source.getData()[i], precision, scale_from, scale_to);

                if (overflow)
                {
                    if constexpr (exception_mode == CheckExceptionMode::Null)
                        (*vec_null_map_to)[i] = overflow;
                    else
                        throw Exception(ErrorCodes::DECIMAL_OVERFLOW, "Decimal value is overflow.");
                }
            }

            typename ColumnDecimal<T>::MutablePtr col_to = ColumnDecimal<T>::create(std::move(col_source));
            if constexpr (exception_mode == CheckExceptionMode::Null)
                result_column = ColumnNullable::create(std::move(col_to), std::move(col_null_map_to));
            else
                result_column = std::move(col_to);
        }

        template <is_decimal T>
        static bool outOfDigits(T decimal, UInt32 precision_to, UInt32 scale_from, UInt32 scale_to)
        {
            using NativeT = typename T::NativeType;

            NativeT converted_value;
            if (scale_to > scale_from)
            {
                converted_value = DecimalUtils::scaleMultiplier<T>(scale_to - scale_from);
                if (common::mulOverflow(static_cast<NativeT>(decimal.value), converted_value, converted_value))
                {
                    if constexpr (exception_mode == CheckExceptionMode::Null)
                        return false;
                    else
                        throw Exception(ErrorCodes::DECIMAL_OVERFLOW, "Decimal value is overflow.");
                }
            }
            else
                converted_value = decimal.value / DecimalUtils::scaleMultiplier<NativeT>(scale_from - scale_to);

            NativeT pow10 = intExp10OfSize<NativeT>(precision_to);
            if (converted_value < 0)
                return converted_value <= -pow10;
            return converted_value >= pow10;
        }
    };

    using FunctionCheckDecimalOverflowThrow = FunctionCheckDecimalOverflow<CheckDecimalOverflow, CheckExceptionMode::Throw>;
    using FunctionCheckDecimalOverflowOrNull = FunctionCheckDecimalOverflow<CheckDecimalOverflowOrNull, CheckExceptionMode::Null>;
}


void registerFunctionCheckDecimalOverflow(FunctionFactory & factory)
{
    factory.registerFunction<FunctionCheckDecimalOverflowThrow>({
        R"(
Check decimal precision is overflow. If overflow throws exception.
)",
        Documentation::Examples{{"CheckDecimalOverflow", "SELECT checkDecimalOverflow(toDecimal32(123.456, 3), 6)"}},
        Documentation::Categories{"OtherFunctions"}});

    factory.registerFunction<FunctionCheckDecimalOverflowOrNull>({
        R"(
Check decimal precision is overflow. If overflow return `NULL`.
)",
        Documentation::Examples{{"CheckDecimalOverflowOrNull", "SELECT checkDecimalOverflowOrNull(toDecimal32(123.456, 3), 6)"}},
        Documentation::Categories{"OtherFunctions"}});
}
}

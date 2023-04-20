#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>

namespace DB
{
namespace ErrorCodes
{

    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}
}


namespace local_engine
{
using namespace DB;

struct NameUnscaleValue
{
    static constexpr auto name = "unscaleValue";
};

template <typename T>
    requires(is_decimal<T>)
static DataTypePtr createNativeDataType()
{
    return std::make_shared<DataTypeNumber<typename T::NativeType>>();
}

namespace
{
    /// Return decimal nest datatype value.
    /// Required 1 argument must be decimal type and return type decide by nest datatype.
    template <typename Name>
    class FunctionUnscaleValue : public IFunction
    {
    public:
        static constexpr auto name = Name::name;

        static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionUnscaleValue>(); }

        String getName() const override { return name; }
        size_t getNumberOfArguments() const override { return 1; }
        bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

        DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
        {
            if (arguments.size() != 1)
                throw Exception(
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                    "Number of arguments for function {} doesn't match: passed {}, should be 1.",
                    getName(),
                    arguments.size());

            if (!isDecimal(arguments[0].type))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of argument of function {}, expected Decimal",
                    arguments[0].type->getName(),
                    getName());

            WhichDataType which(arguments[0].type);

            if (which.isDecimal32())
                return createNativeDataType<Decimal32>();
            else if (which.isDecimal64())
                return createNativeDataType<Decimal64>();
            else if (which.isDecimal128())
                return createNativeDataType<Decimal128>();
            else
                return createNativeDataType<Decimal256>();
        }

        ColumnPtr
        executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
        {
            const auto & src_column = arguments[0];
            if (!src_column.column)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal column while execute function {}", getName());

            ColumnPtr result_column;
            auto call = [&](const auto & types) -> bool
            {
                using Types = std::decay_t<decltype(types)>;
                using Type = typename Types::RightType;
                using ColVecType = ColumnDecimal<Type>;
                using NativeT = typename Type::NativeType;

                if (const ColumnConst * const_column = checkAndGetColumnConst<ColVecType>(src_column.column.get()))
                {
                    result_column = unscaleValue<NativeT>(
                        *checkAndGetColumn<ColVecType>(&const_column->getDataColumn()), result_type, input_rows_count);
                    result_column = ColumnConst::create(result_column, result_column->size());
                    return true;
                }
                else if (const ColVecType * col_vec = checkAndGetColumn<ColVecType>(src_column.column.get()))
                {
                    result_column = unscaleValue<NativeT>(*col_vec, result_type, input_rows_count);
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
        static auto unscaleValue(const ColumnDecimal<Decimal<T>> & columns, const DataTypePtr & result_type, size_t input_rows_count)
        {
            auto col_to = result_type->createColumn();
            col_to->reserve(input_rows_count);

            for (size_t i = 0; i < input_rows_count; ++i)
                col_to->insert(columns.getElement(i).value);

            return col_to;
        }
    };

}

void registerFunctionUnscaleValue(FunctionFactory & factory)
{
    factory.registerFunction<FunctionUnscaleValue<NameUnscaleValue>>({
        R"(
Get decimal nested Integer value.
)",
        Documentation::Examples{{"unscaleValue", "SELECT unscaleValue(makeDecimal(10987654321, 40, 3));"}},
        Documentation::Categories{"OtherFunctions"}});
}

}

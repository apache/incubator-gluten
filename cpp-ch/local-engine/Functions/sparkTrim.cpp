#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <IO/WriteHelpers.h>

#include <memory>
#include <string>

using namespace DB;


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}
}


namespace local_engine
{

struct TrimModeLeft
{
    static constexpr auto name = "sparkTrimLeft";
    static constexpr bool trim_left = true;
    static constexpr bool trim_right = false;
};

struct TrimModeRight
{
    static constexpr auto name = "sparkTrimRigth";
    static constexpr bool trim_left = false;
    static constexpr bool trim_right = true;
};

struct TrimModeBoth
{
    static constexpr auto name = "sparkTrimBoth";
    static constexpr bool trim_left = true;
    static constexpr bool trim_right = true;
};


namespace
{

    template <typename Name>
    class SparkTrimFunction : public IFunction
    {
    public:
        static constexpr auto name = Name::name;

        static FunctionPtr create(ContextPtr) { return std::make_shared<SparkTrimFunction>(); }

        String getName() const override { return name; }

        bool isVariadic() const override { return true; }

        bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

        size_t getNumberOfArguments() const override { return 2; }

        ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

        DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
        {
            if (arguments.size() != 2)
                throw Exception(
                    "Number of arguments for function " + getName() + " doesn't match: passed " + toString(arguments.size())
                        + ", should be  2.",
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
            return std::make_shared<DataTypeString>();
        }

        ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &,  size_t /*input_rows_count*/) const override
        {
            const ColumnPtr & trimStrPtr = arguments[1].column;
            const ColumnConst * trimStrConst = typeid_cast<const ColumnConst *>(&*trimStrPtr);

            if (!trimStrConst) {
                throw Exception("Second argument of function " + getName() + " must be constant string", ErrorCodes::ILLEGAL_COLUMN);
            }
            String trimStr = trimStrConst->getValue<String>();
            auto col_res = ColumnString::create();
            const ColumnPtr column = arguments[0].column;
            if (const ColumnString * col = checkAndGetColumn<ColumnString>(column.get()))
            {
                executeVector(col->getChars(), col->getOffsets(), col_res->getChars(), col_res->getOffsets(), trimStr);
                return col_res;
            }

            return col_res;
        }

    private:
        void executeVector(
            const ColumnString::Chars & data,
            const ColumnString::Offsets & offsets,
            ColumnString::Chars & res_data,
            ColumnString::Offsets & res_offsets,
            const DB::String & trimStr) const
        {
            size_t size = offsets.size();
            res_offsets.resize(size);
            res_data.reserve(data.size());

            size_t prev_offset = 0;
            size_t res_offset = 0;

            const UInt8 * start;
            size_t length;

            for (size_t i = 0; i < size; ++i)
            {
                trimInternal(reinterpret_cast<const UInt8 *>(&data[prev_offset]), offsets[i] - prev_offset - 1, start, length, trimStr);
                res_data.resize(res_data.size() + length + 1);
                memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], start, length);
                res_offset += length + 1;
                res_data[res_offset - 1] = '\0';
                res_offsets[i] = res_offset;
                prev_offset = offsets[i];
            }
        }

        void trimInternal(const UInt8 * data, size_t size, const UInt8 *& res_data, size_t & res_size, const DB::String & trimStr) const
        {
            const char * char_data = reinterpret_cast<const char *>(data);
            const char * char_end = char_data + size;
            if constexpr (Name::trim_left)
            {
                for (size_t i = 0; i < size; i++)
                {
                    char c = * (char_data + i);
                    if (trimStr.find(c) == std::string::npos) {
                        char_data += i;
                        break;
                    }
                }
                res_data = reinterpret_cast<const UInt8 *>(char_data);

            }
            if constexpr (Name::trim_right)
            {
                while(char_end != char_data) {
                    char c = *(char_end -1);
                    if (trimStr.find(c) == std::string::npos) {
                        break;
                    }
                    char_end -= 1;
                }
            }
            res_size = char_end - char_data;
        }
    };

    using FunctionSparkTrimBoth = SparkTrimFunction<TrimModeBoth>;
    using FunctionSparkTrimLeft = SparkTrimFunction<TrimModeLeft>;
    using FunctionSparkTrimRight = SparkTrimFunction<TrimModeRight>;
}

void registerFunctionSparkTrim(FunctionFactory & factory)
{
    factory.registerFunction<FunctionSparkTrimBoth>();
    factory.registerFunction<FunctionSparkTrimLeft>();
    factory.registerFunction<FunctionSparkTrimRight>();
}
}

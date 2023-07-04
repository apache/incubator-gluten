#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/GatherUtils/GatherUtils.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Common/typeid_cast.h>
#include <IO/WriteHelpers.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ZERO_ARRAY_OR_TUPLE_INDEX;
}
}

namespace local_engine
{
using namespace DB;
/**
  * copied from src/Functions/array/arraySlice.cpp
  * but have some differences:
  *   1. the number of arguments should be 3 instead of 2 or 3
  *   2. useDefaultImplementationForNulls return true
  *   3. check whether array indices are 1-based, if not, throw exception
  */
class FunctionArraySliceSpark : public IFunction
{
public:
    static constexpr auto name = "arraySliceSpark";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArraySliceSpark>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }
    size_t getNumberOfArguments() const override { return 3; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool useDefaultImplementationForNulls() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const size_t number_of_arguments = arguments.size();

        if (number_of_arguments != 3)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Number of arguments for function {} doesn't match: passed {}, should be 3",
                            getName(), number_of_arguments);

        const auto * array_type = typeid_cast<const DataTypeArray *>(arguments[0].get());
        if (!array_type)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "First argument for function {} must be an array but it has type {}.",
                            getName(), arguments[0]->getName());

        for (size_t i = 1; i < number_of_arguments; ++i)
        {
            if (!isInteger(arguments[i]))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                                "Argument {} for function {} must be integer but it has type {}.",
                                i, getName(), arguments[i]->getName());
        }

        return arguments[0];
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /* input_rows_count */) const override
    {
        auto array_column = arguments[0].column;
        const auto & offset_column = arguments[1].column;
        const auto & length_column = arguments[2].column;

        std::unique_ptr<GatherUtils::IArraySource> source;

        size_t size = array_column->size();
        bool is_const = false;

        if (const auto * const_array_column = typeid_cast<const ColumnConst *>(array_column.get()))
        {
            is_const = true;
            array_column = const_array_column->getDataColumnPtr();
        }

        if (const auto * argument_column_array = typeid_cast<const ColumnArray *>(array_column.get()))
            source = GatherUtils::createArraySource(*argument_column_array, is_const, size);
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "First arguments for function {} must be array.", getName());

        ColumnArray::MutablePtr sink;

        if (isColumnConst(*offset_column))
        {
            ssize_t offset = offset_column->getUInt(0);
            if (!offset)
                throw Exception(ErrorCodes::ZERO_ARRAY_OR_TUPLE_INDEX, "Array indices are 1-based for function {}", getName());

            if (isColumnConst(*length_column))
            {
                ssize_t length = length_column->getInt(0);
                if (offset > 0)
                    sink = GatherUtils::sliceFromLeftConstantOffsetBounded(*source, static_cast<size_t>(offset - 1), length);
                else
                    sink = GatherUtils::sliceFromRightConstantOffsetBounded(*source, -static_cast<size_t>(offset), length);
            }
            else
                sink = GatherUtils::sliceDynamicOffsetBounded(*source, *offset_column, *length_column);
        }
        else [[unlikely]]
        {
            for (size_t i = 0; i < size; ++i)
            {
                if (offset_column->isDefaultAt(i))
                    throw Exception(ErrorCodes::ZERO_ARRAY_OR_TUPLE_INDEX, "Array indices are 1-based for function {}", getName());
            }
            sink = GatherUtils::sliceDynamicOffsetBounded(*source, *offset_column, *length_column);
        }

        return sink;
    }
};


REGISTER_FUNCTION(ArraySliceSpark)
{
    factory.registerFunction<FunctionArraySliceSpark>();
}

}

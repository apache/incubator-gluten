#pragma once
#include <Columns/IColumn.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/IDataType.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>

namespace local_engine
{
class SparkFunctionConv: public DB::IFunction
{
public:
    static constexpr auto name = "sparkConv";
    static DB::FunctionPtr create(DB::ContextPtr) { return std::make_shared<SparkFunctionMonthsBetween>(); }
    SparkFunctionConv() = default;
    ~SparkFunctionConv() override = default;

    DB::String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 3; }
    bool isVariadic() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DB::DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DB::DataTypePtr getReturnTypeImpl(const DB::DataTypes & /*arguments*/) const override;

    DB::ColumnPtr executeImpl(
        const DB::ColumnsWithTypeAndName & arguments, const DB::DataTypePtr & result_type, size_t /*input_rows_count*/) const override;

    bool useDefaultImplementationForConstants() const override { return true; }
private:
    static DB::DataTypePtr getNestedResultType(DB::DataTypePtr from_arg_type)
    {
        return DB::removeNullable(from_arg_type);
    }
};
}

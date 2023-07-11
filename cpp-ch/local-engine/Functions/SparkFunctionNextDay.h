#pragma once
#include <Columns/IColumn.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/IDataType.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <memory>
namespace local_engine
{
class SparkFunctionNextDay : public DB::IFunction
{
public:
    static constexpr auto name = "spark_next_day";
    static DB::FunctionPtr create(DB::ContextPtr) { return std::make_shared<SparkFunctionNextDay>(); }
    SparkFunctionNextDay() = default;
    ~SparkFunctionNextDay() override = default;

    DB::String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }
    bool isVariadic() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DB::DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DB::DataTypePtr getReturnTypeImpl(const DB::ColumnsWithTypeAndName & arguments) const override;

    DB::ColumnPtr executeImpl(
        const DB::ColumnsWithTypeAndName & arguments, const DB::DataTypePtr & result_type, size_t /*input_rows_count*/) const override;

    static Int8 getDayOfWeek(const String & abbr);
private:
    DB::ColumnPtr executeConst(const DB::ColumnsWithTypeAndName & arguments, const DB::DataTypePtr & result_type) const;
    DB::ColumnPtr executeGeneral(const DB::ColumnsWithTypeAndName & arguments, const DB::DataTypePtr & result_type) const;


    static DB::DataTypePtr getNestedResultType(DB::DataTypePtr from_arg_type)
    {
        return DB::removeNullable(from_arg_type);
    }
};
}

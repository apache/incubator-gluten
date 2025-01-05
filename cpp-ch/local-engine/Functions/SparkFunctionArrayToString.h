/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once
#include <memory>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnStringHelpers.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeArray.h>
#include <Formats/FormatFactory.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <IO/WriteHelpers.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}
}

namespace local_eingine
{
class SparkFunctionArrayToString : public DB::IFunction
{
public:
    static constexpr auto name = "sparkCastArrayToString";

    static DB::FunctionPtr create(DB::ContextPtr context) { return std::make_shared<SparkFunctionArrayToString>(context); }

    explicit SparkFunctionArrayToString(DB::ContextPtr context_) : context(context_) {}

    ~SparkFunctionArrayToString() override = default;

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    bool isSuitableForShortCircuitArgumentsExecution(const DB::DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    bool useDefaultImplementationForNulls() const override { return false; }

    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

    DB::DataTypePtr getReturnTypeImpl(const DB::ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 1)
            throw DB::Exception(DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} argument size must be 1", name);

        auto arg_type = DB::removeNullable(arguments[0].type);
        if (!DB::WhichDataType(arg_type).isArray())
            throw DB::Exception(
                DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument[0] of function {}",
                arguments[0].type->getName(), getName());

        if (arguments[0].type->isNullable())
            return makeNullable(std::make_shared<DB::DataTypeString>());
        else
            return std::make_shared<DB::DataTypeString>();
    }

    DB::ColumnPtr executeImpl(const DB::ColumnsWithTypeAndName & arguments, const DB::DataTypePtr & result_type, size_t input_rows_count) const override
    {
        DB::ColumnUInt8::MutablePtr null_map = nullptr;
        if (const auto * col_nullable = checkAndGetColumn<DB::ColumnNullable>(arguments[0].column.get()))
        {
            null_map = DB::ColumnUInt8::create();
            null_map->insertRangeFrom(col_nullable->getNullMapColumn(), 0, col_nullable->size());
        }

        const auto & nested_col_with_type_and_name = columnGetNested(arguments[0]);

        if (const auto * col_const = typeid_cast<const DB::ColumnConst *>(nested_col_with_type_and_name.column.get()))
        {
            DB::ColumnsWithTypeAndName new_arguments {1};
            new_arguments[0] = {col_const->getDataColumnPtr(), nested_col_with_type_and_name.type, nested_col_with_type_and_name.name};
            auto col = executeImpl(new_arguments, result_type, 1);
            return DB::ColumnConst::create(std::move(col), input_rows_count);
        }

        const DB::IColumn & col_from = *nested_col_with_type_and_name.column;
        size_t size = col_from.size();
        auto col_to = removeNullable(result_type)->createColumn();

        DB::FormatSettings format_settings = context ? DB::getFormatSettings(context) : DB::FormatSettings{};
        format_settings.pretty.charset = DB::FormatSettings::Pretty::Charset::ASCII; /// Use ASCII for pretty output.

        DB::ColumnStringHelpers::WriteHelper write_helper(
                assert_cast<DB::ColumnString &>(*col_to),
                size);

        auto & write_buffer = write_helper.getWriteBuffer();

        const auto * array_type = checkAndGetDataType<DB::DataTypeArray>(nested_col_with_type_and_name.type.get());
        if (!array_type)
            throw DB::Exception(DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument #1 for function {} must be an array, not {}",
                name, arguments[0].type->getName());

        DB::DataTypePtr value_type = array_type->getNestedType();
        auto value_serializer = value_type->getDefaultSerialization();

        for (size_t row = 0; row < size; ++row)
        {
            serializeInSparkStyle(col_from,row,write_buffer,format_settings,    value_serializer);
            write_helper.rowWritten();
        }

        write_helper.finalize();

        if (result_type->isNullable() && null_map)
            return DB::ColumnNullable::create(std::move(col_to), std::move(null_map));
        return col_to;
    }

private:
    DB::ContextPtr context;

    void serializeInSparkStyle(
        const DB::IColumn & column,
        size_t row_num,
        DB::WriteBuffer & ostr,
        const DB::FormatSettings & settings,
        const DB::SerializationPtr & value_serializer) const
    {
        const auto & column_array = assert_cast<const DB::ColumnArray &>(column);

        const auto & nested_column= column_array.getData();
        const DB::ColumnArray::Offsets & offsets = column_array.getOffsets();

        size_t offset = offsets[row_num - 1];
        size_t next_offset = offsets[row_num];

        writeChar('[', ostr);
        if (offset != next_offset)
        {
            value_serializer->serializeText(nested_column, offset, ostr, settings);
            for (size_t i = offset + 1; i < next_offset; ++i)
            {
                writeString(std::string_view(", "), ostr);
                value_serializer->serializeText(nested_column, i, ostr, settings);
            }
        }
        writeChar(']', ostr);
    }
};

}

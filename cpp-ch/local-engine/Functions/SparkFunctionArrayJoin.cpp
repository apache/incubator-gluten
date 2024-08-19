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
#include <base/StringRef.h>
#include <Interpreters/Context_fwd.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnNullable.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeString.h>
#include <iostream>

using namespace DB;

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}
}

namespace local_engine
{
class SparkFunctionArrayJoin : public IFunction
{
public:
    static constexpr auto name = "sparkArrayJoin";
    static FunctionPtr create(ContextPtr) { return std::make_shared<SparkFunctionArrayJoin>(); }
    SparkFunctionArrayJoin() = default;
    ~SparkFunctionArrayJoin() override = default;
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    bool useDefaultImplementationForNulls() const override { return false; }

    DB::DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName &) const override
    {
        auto data_type = std::make_shared<DataTypeString>();
        return makeNullable(data_type);
    }

     ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
     {
        if (arguments.size() != 2 && arguments.size() != 3)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} must have 2 or 3 arguments", getName());
        auto res_col = ColumnString::create();
        auto null_col = ColumnUInt8::create(input_rows_count, 0);
        PaddedPODArray<UInt8> & null_result = null_col->getData();
        if (input_rows_count == 0)
            return ColumnNullable::create(std::move(res_col), std::move(null_col));
        
        const auto * arg_const_col = checkAndGetColumn<ColumnConst>(arguments[0].column.get());
        const ColumnArray * array_col = nullptr;
        const ColumnNullable * arg_null_col = nullptr;
        if (arg_const_col)
        {
            if (arg_const_col->onlyNull())
            {
                null_result[0] = 1;
                return ColumnNullable::create(std::move(res_col), std::move(null_col));
            }
            array_col = checkAndGetColumn<ColumnArray>(arg_const_col->getDataColumnPtr().get());
        }
        else
        {
            arg_null_col = checkAndGetColumn<ColumnNullable>(arguments[0].column.get());
            if (!arg_null_col)
                array_col = checkAndGetColumn<ColumnArray>(arguments[0].column.get());
            else
                array_col = checkAndGetColumn<ColumnArray>(arg_null_col->getNestedColumnPtr().get());
        }
        if (!array_col)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function {} 1st argument must be array type", getName());

        std::pair<bool, StringRef> delim_p, null_replacement_p;
        bool return_result = false;
        auto checkAndGetConstString = [&](const ColumnPtr & col) -> std::pair<bool, StringRef>
        {
            StringRef res;
            const auto * str_null_col = checkAndGetColumnConstData<ColumnNullable>(col.get());
            if (str_null_col)
            {
                if (str_null_col->isNullAt(0))
                {
                    for (size_t i = 0; i < array_col->size(); ++i)
                    {
                        res_col->insertDefault();
                        null_result[i] = 1;
                    }
                    return_result = true;
                    return std::pair<bool, StringRef>(false, res);
                }
            }
            else
            {
                const auto * string_col = checkAndGetColumnConstData<ColumnString>(col.get());
                if (!string_col)
                    return std::pair<bool, StringRef>(false, res);
                else
                    return std::pair<bool, StringRef>(true, string_col->getDataAt(0));
            }
        };
        delim_p = checkAndGetConstString(arguments[1].column);
        if (return_result)
            return ColumnNullable::create(std::move(res_col), std::move(null_col));
        
        if (arguments.size() == 3)
        {
            null_replacement_p = checkAndGetConstString(arguments[2].column);
            if (return_result)
                return ColumnNullable::create(std::move(res_col), std::move(null_col));
        }
        const ColumnNullable * array_nested_col = checkAndGetColumn<ColumnNullable>(&array_col->getData());
        const ColumnString * string_col;
        if (array_nested_col)
            string_col = checkAndGetColumn<ColumnString>(array_nested_col->getNestedColumnPtr().get());
        else
            string_col = checkAndGetColumn<ColumnString>(&array_col->getData());
        const ColumnArray::Offsets & array_offsets = array_col->getOffsets();
        const ColumnString::Offsets & string_offsets = string_col->getOffsets();
        const ColumnString::Chars & string_data = string_col->getChars();
        const ColumnNullable * delim_col = checkAndGetColumn<ColumnNullable>(arguments[1].column.get());
        const ColumnNullable * null_replacement_col = arguments.size() == 3 ? checkAndGetColumn<ColumnNullable>(arguments[2].column.get()) : nullptr;
        size_t current_offset = 0, array_pos = 0;
        for (size_t i = 0; i < array_col->size(); ++i)
        {
            String res;
            auto setResultNull = [&]() -> void
            {
                res_col->insertDefault();
                null_result[i] = 1;
                current_offset = array_offsets[i];
            };
            auto getDelimiterOrNullReplacement = [&](const std::pair<bool, StringRef> & s, const ColumnNullable * col) -> StringRef
            {
                if (s.first)
                    return s.second;
                else
                {
                    if (col->isNullAt(i))
                        return StringRef(nullptr, 0);
                    else
                    {
                        const ColumnString * col_string = checkAndGetColumn<ColumnString>(col->getNestedColumnPtr().get());
                        return col_string->getDataAt(i);
                    }
                }
            };
            if (arg_null_col && arg_null_col->isNullAt(i))
            {
                setResultNull();
                continue;
            }
            const StringRef delim = getDelimiterOrNullReplacement(delim_p, delim_col);
            if (!delim.data)
            {
                setResultNull();
                continue;
            }
            StringRef null_replacement;
            if (arguments.size() == 3)
            {
                null_replacement = getDelimiterOrNullReplacement(null_replacement_p, null_replacement_col);
                if (!null_replacement.data)
                {
                    setResultNull();
                    continue;
                }
            }
            size_t array_size = array_offsets[i] - current_offset;
            size_t data_pos = array_pos == 0 ? 0 : string_offsets[array_pos - 1];
            size_t last_not_null_pos = 0;
            for (size_t j = 0; j < array_size; ++j)
            {
                if (array_nested_col && array_nested_col->isNullAt(j + array_pos))
                {
                    if (null_replacement.data)
                    {
                        res += null_replacement.toString();
                        if (j != array_size - 1)
                            res += delim.toString();
                    }
                    else if (j == array_size - 1)
                        res = res.substr(0, last_not_null_pos);
                }
                else
                {
                    const StringRef s(&string_data[data_pos], string_offsets[j + array_pos] - data_pos - 1);
                    res += s.toString();
                    last_not_null_pos = res.size();
                    if (j != array_size - 1)
                        res += delim.toString();
                }
                data_pos = string_offsets[j + array_pos];
            }
            array_pos += array_size;
            res_col->insertData(res.data(), res.length());
            current_offset = array_offsets[i];
        }
        return ColumnNullable::create(std::move(res_col), std::move(null_col));
     }
};

REGISTER_FUNCTION(SparkArrayJoin)
{
    factory.registerFunction<SparkFunctionArrayJoin>();
}
}
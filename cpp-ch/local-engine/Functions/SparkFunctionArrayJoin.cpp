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

     ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const override
     {
        if (arguments.size() != 2 && arguments.size() != 3)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} must have 2 or 3 arguments", getName());
        
        const auto * arg_null_col = checkAndGetColumn<ColumnNullable>(arguments[0].column.get());
        const auto * array_col = checkAndGetColumn<ColumnArray>(arg_null_col->getNestedColumnPtr().get());
        if (!array_col)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function {} 1st argument must be array type", getName());
        
        auto res_col = ColumnString::create();
        auto null_col = ColumnUInt8::create(array_col->size(), 0);
        PaddedPODArray<UInt8> & null_result = null_col->getData();
        StringRef delim, null_replacement;
        
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
                        res_col->insertData("", 0);
                        null_result[i] = 1;
                    }
                    return std::pair<bool, StringRef>(false, res);
                }
            }
            else
            {
                const auto * string_col = checkAndGetColumnConstData<ColumnString>(col.get());
                if (!string_col)
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function {} 2nd/3rd argument type must be literal string", getName());
                res = string_col->getDataAt(0);
                return std::pair<bool, StringRef>(true, res);
            }
        };
        std::pair<bool, StringRef> delim_res = checkAndGetConstString(arguments[1].column);
        if (!delim_res.first)
            return ColumnNullable::create(std::move(res_col), std::move(null_col));
        delim = delim_res.second;

        if (arguments.size() == 3)
        {
            std::pair<bool, StringRef> null_replacement_res = checkAndGetConstString(arguments[2].column);
            if (!null_replacement_res.first)
                return ColumnNullable::create(std::move(res_col), std::move(null_col));
            null_replacement = null_replacement_res.second;
        }
        
        const ColumnNullable * array_nested_col = checkAndGetColumn<ColumnNullable>(&array_col->getData());
        const ColumnString * string_col = checkAndGetColumn<ColumnString>(array_nested_col->getNestedColumnPtr().get());
        const ColumnArray::Offsets & array_offsets = array_col->getOffsets();
        const ColumnString::Offsets & string_offsets = string_col->getOffsets();
        const ColumnString::Chars & string_data = string_col->getChars();
        size_t current_offset = 0;
        for (size_t i = 0; i < array_col->size(); ++i)
        {
            String res;
            if (arg_null_col->isNullAt(i))
            {
                null_result[i] = 1;
                continue;
            }
            size_t array_size = array_offsets[i] - current_offset;
            size_t data_pos = 0;
            for (size_t j = 0; j < array_size - 1; ++j)
            {
                if (array_nested_col->isNullAt(j))
                {
                    if (null_replacement.data)
                    {
                        res += null_replacement.toString();
                        res += delim.toString();
                    }
                }
                else
                {
                    const StringRef s(&string_data[data_pos], string_offsets[j] - data_pos);
                    res += s.toString();
                    res += delim.toString();
                }
                data_pos = string_offsets[j];
            }
            if (array_size > 0)
            {
                const StringRef s = array_nested_col->isNullAt(array_size - 1) ? null_replacement : StringRef(&string_data[data_pos], string_offsets[array_size - 1] - data_pos);
                res += s.toString();
            }
            res_col->insertData(res.data(), res.size());
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

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
#include <Common/StringUtils/StringUtils.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <Functions/FunctionFactory.h>

using namespace DB;

namespace local_engine
{
class SparkFunctionExtractYear : public IFunction
{
public:
    static constexpr auto name = "sparkExtractYear";
    static DB::FunctionPtr create(DB::ContextPtr) { return std::make_shared<SparkFunctionExtractYear>(); }
    SparkFunctionExtractYear() = default;
    ~SparkFunctionExtractYear() override = default;
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }
    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForConstants() const override { return true; }
    String getName() const override { return name; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName &) const override
    {
        return makeNullable(std::make_shared<DataTypeInt32>());
    }
    DB::ColumnPtr executeImpl(const DB::ColumnsWithTypeAndName & arguments, const DB::DataTypePtr &, size_t) const override
    {
        if (arguments.size() != 1)
            throw Exception(-1, "Function {}'s argument size must be 1", name);
        
        const ColumnString * col_str = checkAndGetColumn<ColumnString>(arguments[0].column.get());
        if (!col_str)
            throw Exception(-1, "Function {}'s 1st argument type must be string", name);
        
        auto res_col = ColumnInt32::create(col_str->size(), 0);
        auto null_map_col = ColumnUInt8::create(col_str->size(), 0);
        executeInternal(*col_str, res_col->getData(), null_map_col->getData());
        return ColumnNullable::create(std::move(res_col), std::move(null_map_col));
    }

    void executeInternal(const ColumnString & src, PaddedPODArray<Int32> & result_data, PaddedPODArray<UInt8> & null_map) const
    {
        for (size_t i = 0; i < src.size(); ++i)
        {
            if (!extractYear(src.getDataAt(i), result_data[i]))
                null_map[i] = 1;
        }
    }

    bool extractYear(const StringRef & s, Int32 & year) const
    {
        size_t i = 0;
        for (; i < s.size; ++i)
        {
            char ch = *(s.data + i);
            if (!isNumericASCII(ch))
            {
                if (ch == '-') break;
                else return false;
            }
        }
        if (i != 4) return false;
        year += (*(s.data + 0) - '0') * 1000 + (*(s.data + 1) - '0') * 100 + (*(s.data + 2) - '0') * 10 + (*(s.data + 3) - '0'); 
        return true;
    }
};

REGISTER_FUNCTION(SparkExtractYear)
{
    factory.registerFunction<SparkFunctionExtractYear>();
}
}
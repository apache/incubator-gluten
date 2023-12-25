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

#include <Common/DateLUTImpl.h>
#include <Functions/FunctionsConversion.h>
#include <Functions/FunctionFactory.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}
}

using namespace DB;

namespace local_eingine
{

class SparkFunctionUnixTimestamp : public FunctionToUnixTimestamp
{
public:
    static constexpr auto name = "sparkToUnixTimestamp";
    static FunctionPtr create(ContextPtr) { return std::make_shared<SparkFunctionUnixTimestamp>(); }
    SparkFunctionUnixTimestamp() = default;
    ~SparkFunctionUnixTimestamp() override = default;
    String getName() const override { return name; }

    ColumnPtr executeImpl(const DB::ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows) const override
    {
        if (arguments.size() != 1 && arguments.size() != 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} argument size must be 1 or 2", name);
        
        ColumnWithTypeAndName first_arg = arguments[0];
        const DateLUTImpl * date_lut = &DateLUT::instance("UTC");
        if (!isDateOrDate32(first_arg.type))
        {
            return FunctionToUnixTimestamp::executeImpl(arguments, result_type, input_rows);
        }
        else if (isDate(first_arg.type))
            return executeInternal<UInt16>(first_arg.column, date_lut);
        else
            return executeInternal<Int32>(first_arg.column, date_lut);
    }

    template<typename T>
    static ColumnPtr executeInternal(const ColumnPtr & col, const DateLUTImpl * date_lut)
    {
        const ColumnVector<T> * col_src = checkAndGetColumn<ColumnVector<T>>(col.get());
        MutableColumnPtr res = ColumnVector<UInt32>::create(col->size());
        PaddedPODArray<UInt32> & data = assert_cast<ColumnVector<UInt32> *>(res.get())->getData();
        for (size_t i = 0; i < col->size(); ++i)
        {
            const T t = col_src->getElement(i);
            LocalDateTime date_time(static_cast<UInt32>(t * DATE_SECONDS_PER_DAY), *date_lut);
            data[i] = date_time.to_time_t();
        }
        return res;
    }
};

}

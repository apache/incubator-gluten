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
#include <Common/LocalDate.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeDate32.h>
#include <Functions/FunctionsConversion.h>
#include <Functions/FunctionFactory.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>

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
class SparkFunctionConvertToDate : public DB::FunctionToDate32OrNull
{
public:
    static constexpr auto name = "spark_to_date";
    static DB::FunctionPtr create(DB::ContextPtr) { return std::make_shared<SparkFunctionConvertToDate>(); }
    SparkFunctionConvertToDate() = default;
    ~SparkFunctionConvertToDate() override = default;
    DB::String getName() const override { return name; }

    bool checkDateFormat(DB::ReadBuffer & buf) const
    {
        auto checkNumbericASCII = [&](DB::ReadBuffer & rb, size_t start, size_t length) -> bool
        {
            for (size_t i = start; i < start + length; ++i)
            {
                if (!isNumericASCII(*(rb.position() + i)))
                    return false;
            }
            return true;
        };
        auto checkDelimiter = [&](DB::ReadBuffer & rb, size_t pos) -> bool
        {
            if (*(rb.position() + pos) != '-')
                return false;
            else
                return true;
        };
        auto checkMonthDay = [&](DB::ReadBuffer &rb) -> bool
        {
            int month = (*(rb.position() + 5) - '0') * 10 + (*(rb.position() + 6) - '0');
            if (month > 12 || month < 0)
                return false;
            int day = (*(rb.position() + 8) - '0') * 10 + (*(rb.position() + 9) - '0');
            if (day < 0 || day > 31)
                return false;
            else if (day == 31 && (month == 2 || month == 4 || month == 6 || month == 9 || month == 11))
                return false;
            else if (day == 30 && month == 2)
                return false;
            else if (day == 29 && month == 2)
            {
                int year = (*(rb.position() + 0) - '0') * 1000 + 
                    (*(rb.position() + 1) - '0') * 100 + 
                    (*(rb.position() + 2) - '0') * 10 + 
                    (*(rb.position() + 3) - '0');
                if (year % 4 != 0)
                    return false;
                else
                    return true;
            }
            else
                return true;

        };
        if (!checkNumbericASCII(buf, 0, 4))
            return false;
        if (!checkDelimiter(buf, 4))
            return false;
        if (!checkNumbericASCII(buf, 5, 2))
            return false;
        if (!checkDelimiter(buf, 7))
            return false;
        if (!checkNumbericASCII(buf, 8, 2))
            return false;
        if (!checkMonthDay(buf))
            return false;
        return true;
    }

    DB::ColumnPtr executeImpl(const DB::ColumnsWithTypeAndName & arguments, const DB::DataTypePtr & result_type, size_t) const override
    {
        auto result_column = result_type->createColumn();
        if (arguments.size() != 1)
            throw DB::Exception(DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {}'s arguments number must be 1.", name);
        
        const DB::ColumnWithTypeAndName arg1 = arguments[0];
        const auto * src_col = checkAndGetColumn<DB::ColumnString>(arg1.column.get());
        for (size_t i = 0; i < src_col->size(); ++i)
        {
            auto str = src_col->getDataAt(i);
            if (str.size < 10)
            {
                result_column->insert(DB::Field());
            }
            else
            {
                DB::ReadBufferFromMemory buf(str.data, str.size);
                while(!buf.eof() && *buf.position() == ' ')
                {
                    buf.position() ++;
                }
                if(buf.buffer().end() - buf.position() < 10)
                {
                    result_column->insert(DB::Field());
                    continue;
                }
                if (!checkDateFormat(buf))
                {
                    result_column->insert(DB::Field());
                    continue;
                }
                LocalDate date;
                if (readDateTextImpl<bool>(date, buf))
                    result_column->insert(date.getDayNum());
                else
                    result_column->insert(DB::Field());
            }
        }
        return result_column;
    }
};

REGISTER_FUNCTION(SparkToDate)
{
    factory.registerFunction<SparkFunctionConvertToDate>();
}

}

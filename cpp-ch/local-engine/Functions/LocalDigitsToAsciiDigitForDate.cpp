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


#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <boost/iostreams/detail/select.hpp>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include "base/types.h"

namespace DB
{
namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}
}

namespace local_engine
{
// Since spark 3.3, unix_timestamp support arabic number input, e.g., "٢٠٢١-٠٧-٠١ ١٢:٠٠:٠٠".
// We implement a function to translate arabic indic digits to ascii digits here.
class LocalDigitsToAsciiDigitForDateFunction : public DB::IFunction
{
public:
    static constexpr auto name = "local_digit_to_ascii_digit_for_date";

    static DB::FunctionPtr create(DB::ContextPtr) { return std::make_shared<LocalDigitsToAsciiDigitForDateFunction>(); }

    String getName() const override { return name; }

    bool isSuitableForShortCircuitArgumentsExecution(const DB::DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    size_t getNumberOfArguments() const override { return 1; }

    DB::DataTypePtr getReturnTypeImpl(const DB::DataTypes & arguments) const override
    {
        auto nested_type = DB::removeNullable(arguments[0]);
        if (!DB::WhichDataType(nested_type).isString())
            throw DB::Exception(
                DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Argument for function {} must be String, but got {}",
                getName(),
                arguments[0]->getName());
        return arguments[0];
    }

    DB::ColumnPtr executeImpl(const DB::ColumnsWithTypeAndName & arguments, const DB::DataTypePtr &, size_t input_rows_count) const override
    {
        auto data_col = arguments[0].column;
        const DB::ColumnString * col_str = nullptr;
        const DB::ColumnNullable * col_nullable = nullptr;
        const DB::NullMap * null_map = nullptr;
        if (data_col->isConst())
        {
            if (data_col->isNullAt(0))
            {
                return data_col;
            }
            const DB::ColumnConst * col_const = DB::checkAndGetColumn<DB::ColumnConst>(data_col.get());
            data_col = col_const->getDataColumnPtr();
            if (data_col->isNullable())
            {
                col_nullable = DB::checkAndGetColumn<DB::ColumnNullable>(data_col.get());
                null_map = &(col_nullable->getNullMapData());
                col_str = DB::checkAndGetColumn<DB::ColumnString>(&(col_nullable->getNestedColumn()));
            }
            else
            {
                col_str = DB::checkAndGetColumn<DB::ColumnString>(data_col.get());
            }
            if (!col_str)
                throw DB::Exception(
                    DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Argument for function {} must be String, but got {}",
                    getName(),
                    data_col->getName());
            auto date_str = col_str->getDataAt(0);
            auto new_str = convertLocalDigit(date_str);
            auto new_data_col = data_col->cloneEmpty();
            new_data_col->insertData(new_str.c_str(), new_str.size());
            return DB::ColumnConst::create(std::move(new_data_col), input_rows_count);
        }

        if (data_col->isNullable())
        {
            col_nullable = DB::checkAndGetColumn<DB::ColumnNullable>(data_col.get());
            null_map = &(col_nullable->getNullMapData());
            col_str = DB::checkAndGetColumn<DB::ColumnString>(&(col_nullable->getNestedColumn()));
        }
        else
        {
            col_str = DB::checkAndGetColumn<DB::ColumnString>(data_col.get());
        }
        if (!col_str)
            throw DB::Exception(
                DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Argument for function {} must be String, but got {}",
                getName(),
                data_col->getName());

        auto nested_data_col = DB::removeNullable(arguments[0].column);
        bool has_local_digit = false;
        size_t row_index = 0;
        for (row_index = 0; row_index < input_rows_count; ++row_index)
        {
            if (null_map && (*null_map)[row_index])
            {
                continue;
            }
            auto str = col_str->getDataAt(row_index);
            if (hasLocalDigit(str))
            {
                has_local_digit = true;
                break;
            }
        }

        if (!has_local_digit)
        {
            // No local language digits found, return the original column
            return arguments[0].column;
        }

        auto res_col = data_col->cloneEmpty();
        if (row_index)
        {
            res_col->insertManyFrom(*data_col, 0, row_index);
        }
        for (; row_index < input_rows_count; ++row_index)
        {
            if (null_map && (*null_map)[row_index])
            {
                res_col->insertDefault();
                continue;
            }
            auto str = convertLocalDigit(col_str->getDataAt(row_index));
            LOG_ERROR(getLogger("LocalDigitsToAsciiDigitForDateFunction"), "Converted local digit string {} to ascii digit string: {}", col_str->getDataAt(row_index).toString(), str);
            res_col->insertData(str.c_str(), str.size());
        }
        return res_col;
    }

private:
    bool hasLocalDigit(StringRef str) const
    {
        // In most cases, the first byte is a digit.
        char c = reinterpret_cast<char>(str.data[0]);
        if ('0' <= c && c <= '9')
        {
            return false;
        }
        return true;
    }

    char toAsciiDigit(char32_t c) const {
        // In Thai and Persian, dates typically do not use the Gregorian calendar.
        // This may cause failures in unix_timestamp parsing.
        if (c >= 0x0660 && c <= 0x0669)
            return static_cast<char>(c - 0x0660 + '0');
        else if (c >= 0x06F0 && c <= 0x06F9)
            return static_cast<char>(c - 0x06F0 + '0');
        else if (c >= 0x0966 && c <= 0x096F)
            return static_cast<char>(c - 0x0966 + '0');
        else if (c >= 0x0E50 && c <= 0x0E59)
            return static_cast<char>(c - 0x0E50 + '0');
        else if (c >= 0x17E0 && c <= 0x17E9)
            return static_cast<char>(c - 0x17E0 + '0');
        else if (c >= 0x09E6 && c <= 0x09EF)
            return static_cast<char>(c - 0x09E6 + '0');
        else
            return 0;
    }

    String convertLocalDigit(const StringRef & str) const
    {
        std::string result;
        result.reserve(str.size);
        for (size_t i = 0; i < str.size;)
        {
            unsigned char c = str.data[i];
            char32_t cp = 0;
            if ((c & 0x80) == 0) // 1-byte
            {
                cp = c;
                i += 1;
            }
            else if ((c & 0xE0) == 0xC0) // 2-byte
            {
                cp = ((c & 0x1F) << 6) | (str.data[i + 1] & 0x3F);
                i += 2;
            }
            else if ((c & 0xF0) == 0xE0) // 3-byte
            {
                cp = ((c & 0x0F) << 12) | ((str.data[i + 1] & 0x3F) << 6) | (str.data[i + 2] & 0x3F);
                i += 3;
            }
            else if ((c & 0xF8) == 0xF0) // 4-byte
            {
                cp = ((c & 0x07) << 18) | ((str.data[i + 1] & 0x3F) << 12) | ((str.data[i + 2] & 0x3F) << 6) | (str.data[i + 3] & 0x3F);
                i += 4;
            }
            auto local_digit = toAsciiDigit(cp);
            if (local_digit)
                result.push_back(local_digit);
            else
                result.push_back(cp);
        }
        return result;
    }
};

using namespace DB;
REGISTER_FUNCTION(LocalDigitToAsciiDigitForDate)
{
    factory.registerFunction<LocalDigitsToAsciiDigitForDateFunction>();
}
}

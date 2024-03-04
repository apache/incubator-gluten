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
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <IO/WriteHelpers.h>

#include <memory>
#include <string>

using namespace DB;

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}
}

namespace local_engine
{
struct TrimModeLeft
{
    static constexpr auto name = "trimLeftSpark";
    static constexpr bool trim_left = true;
    static constexpr bool trim_right = false;
};

struct TrimModeRight
{
    static constexpr auto name = "trimRightSpark";
    static constexpr bool trim_left = false;
    static constexpr bool trim_right = true;
};

struct TrimModeBoth
{
    static constexpr auto name = "trimBothSpark";
    static constexpr bool trim_left = true;
    static constexpr bool trim_right = true;
};


namespace
{
    template <typename TrimMode>
    class TrimSparkFunction : public IFunction
    {
    public:
        static constexpr auto name = TrimMode::name;

        static FunctionPtr create(ContextPtr) { return std::make_shared<TrimSparkFunction>(); }

        String getName() const override { return name; }

        bool useDefaultImplementationForConstants() const override { return true; }

        size_t getNumberOfArguments() const override { return 2; }

        bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

        ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

        DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
        {
            if (arguments.size() != 2)
                throw Exception(
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                    "Number of arguments for function {} doesn't match: passed {}, should be  2.",
                    getName(),
                    toString(arguments.size()));

            if (!isString(arguments[0]))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type ({}) of first argument of function {}. Should be String.",
                    arguments[0]->getName(),
                    getName());

            if (!isString(arguments[1]))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type ({}) of second argument of function {}. Should be String.",
                    arguments[0]->getName(),
                    getName());

            return std::make_shared<DataTypeString>();
        }

        ColumnPtr
        executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
        {
            const ColumnString * src_str_col = checkAndGetColumn<ColumnString>(arguments[0].column.get());
            if (!src_str_col)
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "First argument of function {} must be String", getName());

            const ColumnConst * trim_str_col = checkAndGetColumnConst<ColumnString>(arguments[1].column.get());
            if (!trim_str_col)
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Second argument of function {} must be Const String", getName());

            String trim_str = trim_str_col->getValue<String>();
            if (trim_str.empty())
                return src_str_col->cloneResized(input_rows_count);

            auto res_col = ColumnString::create();
            res_col->reserve(input_rows_count);

            executeVector(src_str_col->getChars(), src_str_col->getOffsets(), res_col->getChars(), res_col->getOffsets(), trim_str);
            return std::move(res_col);
        }

    private:
        void executeVector(
            const ColumnString::Chars & data,
            const ColumnString::Offsets & offsets,
            ColumnString::Chars & res_data,
            ColumnString::Offsets & res_offsets,
            const String & trim_str) const
        {
            res_data.reserve_exact(data.size());

            size_t rows = offsets.size();
            res_offsets.resize_exact(rows);

            size_t prev_offset = 0;
            size_t res_offset = 0;

            const UInt8 * start;
            size_t length;
            std::unordered_set<char> trim_set(trim_str.begin(), trim_str.end());
            for (size_t i = 0; i < rows; ++i)
            {
                trim(reinterpret_cast<const UInt8 *>(&data[prev_offset]), offsets[i] - prev_offset - 1, start, length, trim_set);
                res_data.resize_exact(res_data.size() + length + 1);
                memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], start, length);
                res_offset += length + 1;
                res_data[res_offset - 1] = '\0';

                res_offsets[i] = res_offset;
                prev_offset = offsets[i];
            }
        }

        void
        trim(const UInt8 * data, size_t size, const UInt8 *& res_data, size_t & res_size, const std::unordered_set<char> & trim_set) const
        {
            const char * char_data = reinterpret_cast<const char *>(data);
            const char * char_end = char_data + size;

            if constexpr (TrimMode::trim_left)
                while (char_data < char_end && trim_set.contains(*char_data))
                    ++char_data;

            if constexpr (TrimMode::trim_right)
                while (char_data < char_end && trim_set.contains(*(char_end - 1)))
                    --char_end;

            res_data = reinterpret_cast<const UInt8 *>(char_data);
            res_size = char_end - char_data;
        }
    };

    using FunctionTrimBothSpark = TrimSparkFunction<TrimModeBoth>;
    using FunctionTrimLeftSpark = TrimSparkFunction<TrimModeLeft>;
    using FunctionTrimRightSpark = TrimSparkFunction<TrimModeRight>;
}

REGISTER_FUNCTION(TrimSpark)
{
    factory.registerFunction<FunctionTrimBothSpark>();
    factory.registerFunction<FunctionTrimLeftSpark>();
    factory.registerFunction<FunctionTrimRightSpark>();
}

}

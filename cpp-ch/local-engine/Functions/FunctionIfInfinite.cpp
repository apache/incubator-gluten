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
#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/getLeastSupertype.h>
#include <Core/ColumnNumbers.h>


namespace local_engine
{
using namespace DB;
namespace
{

    /// IfInfinite(x, y) is equivalent to isInfinite(x) ? y : x.
    class FunctionIfInfinite : public IFunction
    {
    public:
        static constexpr auto name = "ifInfinite";

        explicit FunctionIfInfinite(ContextPtr context_) : context(context_) {}

        static FunctionPtr create(ContextPtr context)
        {
            return std::make_shared<FunctionIfInfinite>(context);
        }

        std::string getName() const override
        {
            return name;
        }

        size_t getNumberOfArguments() const override { return 2; }
        bool useDefaultImplementationForNulls() const override { return false; }
        bool useDefaultImplementationForConstants() const override { return true; }
        bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
        ColumnNumbers getArgumentsThatDontImplyNullableReturnType(size_t /*number_of_arguments*/) const override { return {0}; }

        DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
        {
            auto is_infinite_type = FunctionFactory::instance().get("isInfinite", context)->build({arguments[0]})->getResultType();
            auto if_type = FunctionFactory::instance().get("if", context)->build({{nullptr, is_infinite_type, ""}, arguments[0], arguments[1]})->getResultType();
            return if_type;
        }

        ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
        {
            ColumnsWithTypeAndName is_infinite_columns{arguments[0]};
            auto is_infinite = FunctionFactory::instance().get("isInfinite", context)->build(is_infinite_columns);
            auto res = is_infinite->execute(is_infinite_columns, is_infinite->getResultType(), input_rows_count);

            ColumnsWithTypeAndName if_columns
                {
                    {res, is_infinite->getResultType(), ""},
                    arguments[1],
                    arguments[0],
                };

            auto func_if = FunctionFactory::instance().get("if", context)->build(if_columns);
            return func_if->execute(if_columns, result_type, input_rows_count);
        }

    private:
        ContextPtr context;
    };

}

REGISTER_FUNCTION(IfInfinite)
{
    factory.registerFunction<FunctionIfInfinite>();
}

}

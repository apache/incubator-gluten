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
#include <string>
#include <Columns/DecodedJSONColumn.h>
#include <DataTypes/DataTypeNullable.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include "config.h"
#include <Poco/Logger.h>
#include <Common/logger_useful.h>


namespace DB
{
namespace ErrorCodes
{}
}

namespace local_engine
{
class DecodeJSONStringFunction : public DB::IFunction
{
public:
    static constexpr auto name = "decodeJSONString";
    static DB::FunctionPtr create(const DB::ContextPtr & context)
    {
        return std::make_shared<DecodeJSONStringFunction>(context);
    }

    explicit DecodeJSONStringFunction(DB::ContextPtr context_) : context(context_) {}
    ~DecodeJSONStringFunction() override = default;

    DB::String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }
    bool isVariadic() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DB::DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DB::DataTypePtr getReturnTypeImpl(const DB::ColumnsWithTypeAndName & arguments) const override
    {
        return arguments[0].type;
    }

    DB::ColumnPtr executeImpl(
        const DB::ColumnsWithTypeAndName & arguments, const DB::DataTypePtr & /*result_type*/, size_t /*input_rows_count*/) const override
    {
#if USE_SIMDJSON
        if (context->getSettingsRef().allow_simdjson)
        {
            return SimdJSONColumn::create(arguments[0].column, std::make_shared<SimdJSONParserWrapper>());
        }
#endif
        auto decoder = std::make_shared<DummyJSONParserWrapper>();
        return DummyJSONColumn::create(arguments[0].column, decoder);
    }

private:
    DB::ContextPtr context;
};

REGISTER_FUNCTION(DecodeJSONStringFunction)
{
    factory.registerFunction<DecodeJSONStringFunction>();
}
}

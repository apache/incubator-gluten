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
#include <AggregateFunctions/AggregateFunctionPartialMerge.h>
#include <AggregateFunctions/Combinators/AggregateFunctionCombinatorFactory.h>
#include <DataTypes/DataTypeAggregateFunction.h>


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
namespace
{
class AggregateFunctionCombinatorPartialMerge final : public IAggregateFunctionCombinator
{
public:
    String getName() const override { return "PartialMerge"; }

    DataTypes transformArguments(const DataTypes & arguments) const override
    {
        if (arguments.size() != 1)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Incorrect number of arguments for aggregate function with {} suffix",
                getName());

        const DataTypePtr & argument = arguments[0];

        const DataTypeAggregateFunction * function = typeid_cast<const DataTypeAggregateFunction *>(argument.get());
        if (!function)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument for aggregate function with {} suffix  must be AggregateFunction(...)",
                argument->getName(),
                getName());

        const DataTypeAggregateFunction * function2
            = typeid_cast<const DataTypeAggregateFunction *>(function->getArgumentsDataTypes()[0].get());
        if (function2)
            return transformArguments(function->getArgumentsDataTypes());
        return function->getArgumentsDataTypes();
    }

    AggregateFunctionPtr transformAggregateFunction(
        const AggregateFunctionPtr & nested_function,
        const AggregateFunctionProperties &,
        const DataTypes & arguments,
        const Array & params) const override
    {
        DataTypePtr & argument = const_cast<DataTypePtr &>(arguments[0]);

        const DataTypeAggregateFunction * function = typeid_cast<const DataTypeAggregateFunction *>(argument.get());
        if (!function)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument for aggregate function with {} suffix must be AggregateFunction(...)",
                argument->getName(),
                getName());

        while (nested_function->getName() != function->getFunctionName())
        {
            argument = function->getArgumentsDataTypes()[0];
            function = typeid_cast<const DataTypeAggregateFunction *>(function->getArgumentsDataTypes()[0].get());
            if (!function)
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of argument for aggregate function with {} suffix must be AggregateFunction(...)",
                    argument->getName(),
                    getName());
        }

        if (nested_function->getName() != function->getFunctionName())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument for aggregate function with {} suffix, because it corresponds to different aggregate "
                "function: {} instead of {}",
                argument->getName(),
                getName(),
                function->getFunctionName(),
                nested_function->getName());

        return std::make_shared<AggregateFunctionPartialMerge>(nested_function, argument, params);
    }
};

}

void registerAggregateFunctionCombinatorPartialMerge(AggregateFunctionCombinatorFactory & factory)
{
    factory.registerCombinator(std::make_shared<AggregateFunctionCombinatorPartialMerge>());
}

}

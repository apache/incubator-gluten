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
#include <Parser/FunctionParser.h>
#include <Common/CHUtil.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/IDataType.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}
}

namespace local_engine
{

class FunctionParserConcat : public FunctionParser
{
public:
    explicit FunctionParserConcat(SerializedPlanParser * plan_parser_) : FunctionParser(plan_parser_) {}
    ~FunctionParserConcat() override = default;

    static constexpr auto name = "concat";

    String getName() const override { return name; }
    String getCHFunctionName(const substrait::Expression_ScalarFunction &) const override { return name; }

    const ActionsDAG::Node * parse(
        const substrait::Expression_ScalarFunction & substrait_func,
        ActionsDAG & actions_dag) const override
    {
        /*
          parse concat(args) as:
            1. if output type is array, return arrayConcat(args)
            2. otherwise:
                1) if args is empty, return empty string
                2) if args have size 1, return identity(args[0])
                3) otherwise return concat(args)
        */
        auto args = parseFunctionArguments(substrait_func, actions_dag);
        const auto & output_type = substrait_func.output_type();
        const ActionsDAG::Node * result_node = nullptr;
        if (output_type.has_list())
        {
             result_node = toFunctionNode(actions_dag, "arrayConcat", args);
        }
        else
        {
            if (args.empty())
                result_node = addColumnToActionsDAG(actions_dag, std::make_shared<DataTypeString>(), "");
            else if (args.size() == 1)
                result_node = toFunctionNode(actions_dag, "identity", args);
            else
                result_node = toFunctionNode(actions_dag, "concat", args);
        }
        return convertNodeTypeIfNeeded(substrait_func, result_node, actions_dag);
    }
};

static FunctionParserRegister<FunctionParserConcat> register_concat;
}

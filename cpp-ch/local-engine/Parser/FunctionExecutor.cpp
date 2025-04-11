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
#include "FunctionExecutor.h"

#include <Builder/SerializedPlanBuilder.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Common/BlockTypeUtils.h>
#include <Parser/SubstraitParserUtils.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_COLUMNS_DOESNT_MATCH;
}
}

namespace local_engine
{

using namespace dbms;
using namespace DB;

void FunctionExecutor::buildExpressionParser()
{
    std::unordered_map<String, String> function_mapping;
    function_mapping["0"] = name;
    auto parser_context = ParserContext::build(context, function_mapping);
    expression_parser = std::make_unique<ExpressionParser>(parser_context);
}

void FunctionExecutor::buildExpression()
{
    auto * scalar_function = expression.mutable_scalar_function();
    scalar_function->set_function_reference(0);

    auto type = SerializedPlanBuilder::buildType(output_type);
    *scalar_function->mutable_output_type() = *type;

    Int32 field = 0;
    auto * arguments = scalar_function->mutable_arguments();

    std::for_each(input_types.cbegin(), input_types.cend(),
      [&](const auto & ) {
          substrait::FunctionArgument argument;
          auto * value = argument.mutable_value();
          value->CopyFrom(SubstraitParserUtils::buildStructFieldExpression(field++));

          arguments->Add(std::move(argument));
    });
}

void FunctionExecutor::buildHeader()
{
    size_t i = 0;
    for (const auto & input_type : input_types)
        header.insert(ColumnWithTypeAndName{nullptr, input_type, "col_" + std::to_string(i++)});
}

void FunctionExecutor::parseExpression()
{
    DB::ActionsDAG actions_dag{blockToRowType(header)};
    /// Notice keep_result must be true, because result_node of current function must be output node in actions_dag
    const auto * node = expression_parser->parseFunction(expression.scalar_function(), actions_dag, true);
    result_name = node->result_name;

    expression_actions = std::make_unique<ExpressionActions>(std::move(actions_dag));
}

Block FunctionExecutor::getHeader() const
{
    return header;
}

String FunctionExecutor::getResultName() const
{
    return result_name;
}

void FunctionExecutor::execute(Block & block)
{
    expression_actions->execute(block);
}

bool FunctionExecutor::executeAndCompare(const std::vector<FunctionExecutor::TestCase> & cases)
{
    Block block = header.cloneEmpty();
    auto columns = block.mutateColumns();

    for (const auto & test_case : cases)
    {
        if (test_case.inputs.size() != input_types.size())
            throw Exception(ErrorCodes::NUMBER_OF_COLUMNS_DOESNT_MATCH, "Wrong number of inputs");

        for (size_t i = 0; i < test_case.inputs.size(); ++i)
            columns[i]->insert(test_case.inputs[i]);
    }
    block.setColumns(std::move(columns));

    execute(block);

    const auto & result_column = block.getByName(result_name).column;
    for (size_t i = 0; i < cases.size(); ++i)
    {
        Field output;
        result_column->get(i, output);

        if (output != cases[i].expect_output)
            return false;
    }
    return true;
}


}

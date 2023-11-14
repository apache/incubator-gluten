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

void FunctionExecutor::buildExtensions()
{
    auto * extension = extensions.Add();
    auto * extension_function = extension->mutable_extension_function();
    extension_function->set_function_anchor(0);
    extension_function->set_name(name);
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
          auto * selection = value->mutable_selection();
          auto * direct_reference = selection->mutable_direct_reference();
          auto * struct_field = direct_reference->mutable_struct_field();
          struct_field->set_field(field++);

          arguments->Add(std::move(argument));
    });
}

void FunctionExecutor::buildHeader()
{
    size_t i = 0;
    for (const auto & input_type : input_types)
        header.insert(ColumnWithTypeAndName{nullptr, input_type, "col_" + std::to_string(i++)});
}

void FunctionExecutor::parseExtensions()
{
    plan_parser.parseExtensions(extensions);
}

void FunctionExecutor::parseExpression()
{
    /// Notice keep_result must be true, because result_node of current function must be output node in actions_dag
    auto actions_dag = plan_parser.parseFunction(header, expression, result_name, nullptr, true);
    // std::cout << "actions_dag:" << std::endl;
    // std::cout << actions_dag->dumpDAG() << std::endl;

    expression_actions = std::make_unique<ExpressionActions>(actions_dag);
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

    // std::cout << "input block:" << block.dumpStructure() << std::endl;
    execute(block);
    // std::cout << "output block:" << block.dumpStructure() << std::endl;

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

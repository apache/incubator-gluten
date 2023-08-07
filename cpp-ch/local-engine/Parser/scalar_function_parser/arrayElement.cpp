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
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeArray.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}
}

namespace local_engine
{

class FunctionParserArrayElement : public FunctionParser
{
public:
    explicit FunctionParserArrayElement(SerializedPlanParser * plan_parser_) : FunctionParser(plan_parser_) {}
    ~FunctionParserArrayElement() override = default;

    const ActionsDAG::Node * parse(const substrait::Expression_ScalarFunction & substrait_func, ActionsDAGPtr & actions_dag) const override
    {
        auto parsed_args = parseFunctionArguments(substrait_func, "arrayElement", actions_dag);
        if (parsed_args.size() != 2)
            throw Exception(DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires exactly two arguments", getName());

        const DB::ActionsDAG::Node * arr_arg = parsed_args[0];
        const DB::ActionsDAG::Node * index_arg = parsed_args[1];

        const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(removeNullable(arr_arg->result_type).get());
        if (!array_type)
            throw Exception(DB::ErrorCodes::BAD_ARGUMENTS, "First argument for function {} must be an array", getName());
        
        const DB::DataTypePtr nested_type = removeNullable(array_type->getNestedType());
        DB::WhichDataType type_which(nested_type);
        if (type_which.isTuple())
        {
            DB::DataTypePtr array_type_non_null = std::make_shared<DB::DataTypeArray>(nested_type);
            const DB::ActionsDAG::Node * arr_not_null_node = ActionsDAGUtil::convertNodeType(actions_dag, arr_arg, makeNullable(array_type_non_null)->getName());
            return toFunctionNode(actions_dag, "arrayElement", {arr_not_null_node, index_arg});
        }
        else
            return toFunctionNode(actions_dag, "arrayElement", {arr_arg, index_arg});
    }

protected:
    String getCHFunctionName(const substrait::Expression_ScalarFunction & /*substrait_func*/) const override
    {
        return "arrayElement";
    }
};

class FunctionParserElementAt : public FunctionParserArrayElement
{
public:
    explicit FunctionParserElementAt(SerializedPlanParser * plan_parser_) : FunctionParserArrayElement(plan_parser_) { }
    ~FunctionParserElementAt() override = default;

    static constexpr auto name = "element_at";
    String getName() const override { return name; }
};

class FunctionParserGetArrayItem : public FunctionParserArrayElement
{
public:
    explicit FunctionParserGetArrayItem(SerializedPlanParser * plan_parser_) : FunctionParserArrayElement(plan_parser_) { }
    ~FunctionParserGetArrayItem() override = default;
    
    static constexpr auto name = "get_array_item";
    String getName() const override { return name; }
};

class FunctionParserGetMapValue : public FunctionParserArrayElement
{
public:
    explicit FunctionParserGetMapValue(SerializedPlanParser * plan_parser_) : FunctionParserArrayElement(plan_parser_) { }
    ~FunctionParserGetMapValue() override = default;
    
    static constexpr auto name = "get_map_value";
    String getName() const override { return name; }
};

static FunctionParserRegister<FunctionParserElementAt> register_array_element_at;
static FunctionParserRegister<FunctionParserGetArrayItem> register_array_get_item;
static FunctionParserRegister<FunctionParserGetMapValue> register_map_get_value;
}

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
#include <DataTypes/IDataType.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}
}

namespace local_engine
{

class FunctionParserArraySlice : public FunctionParser
{
public:
    explicit FunctionParserArraySlice(SerializedPlanParser * plan_parser_) : FunctionParser(plan_parser_) { }

    static constexpr auto name = "slice";

    String getName() const override { return name; }

    const ActionsDAG::Node * parse(
        const substrait::Expression_ScalarFunction & substrait_func,
        ActionsDAGPtr & actions_dag) const override
    {
        /**
            parse slice(arr, start, length) as
            if (isNull(arr))
                null
            elif (isNull(start))
                null
            elif (if(isNull(length)))
                null
            else
                slice(assumeNotNull(arr), if (start=0) then throwIf(start=0) else start, if (length<0) then throwIf(length<0) else length)

            Main differences between CH arraySlice and Spark slice
            1. Spark slice throws exception if start = 0 or length < 0
            2. Spark slice returns null if any of the argument is null
        */

        auto parsed_args = parseFunctionArguments(substrait_func, "", actions_dag);
        if (parsed_args.size() != 3)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function {} requires exactly three arguments", getName());

        const auto * arr_arg = parsed_args[0];
        const auto * start_arg = parsed_args[1];
        const auto * length_arg = parsed_args[2];

        auto is_arr_nullable = arr_arg->result_type->isNullable();
        auto is_start_nullable = start_arg->result_type->isNullable();
        auto is_length_nullable = length_arg->result_type->isNullable();

        const auto * zero_const_node = addColumnToActionsDAG(actions_dag, std::make_shared<DataTypeInt32>(), 0);
        const auto * start_if_node = makeStartIfNode(actions_dag, start_arg, zero_const_node);
        const auto * length_if_node = makeLengthIfNode(actions_dag, length_arg, zero_const_node);

        if (!is_arr_nullable && !is_start_nullable && !is_length_nullable)
        {
            // slice(arr, if (start=0) then throwIf(start=0) else start, if (length<0) then throwIf(length<0) else length)
            return toFunctionNode(actions_dag, "arraySlice", {arr_arg, start_if_node, length_if_node});
        }

        // There is at least one nullable argument, should return nullable result
        const auto * arr_not_null_node = is_arr_nullable ? toFunctionNode(actions_dag, "assumeNotNull", {arr_arg}) : arr_arg;
        const auto * slice_node = toFunctionNode(actions_dag, "arraySlice", {arr_not_null_node, start_if_node, length_if_node});
        DataTypePtr wrap_arr_nullable_type = wrapNullableType(true, slice_node->result_type);

        const auto * wrap_slice_node = ActionsDAGUtil::convertNodeType(
            actions_dag, slice_node, wrap_arr_nullable_type->getName(), slice_node->result_name);
        const auto * null_const_node = addColumnToActionsDAG(actions_dag, wrap_arr_nullable_type, Field{});

        const auto * arr_is_null_node = toFunctionNode(actions_dag, "isNull", {arr_arg});
        const auto * start_is_null_node = toFunctionNode(actions_dag, "isNull", {start_arg});
        const auto * length_is_null_node = toFunctionNode(actions_dag, "isNull", {length_arg});
        const auto * or_condition_node = toFunctionNode(actions_dag, "or", {arr_is_null_node, start_is_null_node, length_is_null_node});

        const auto * if_node = toFunctionNode(actions_dag, "if", {or_condition_node, null_const_node, wrap_slice_node });
        return convertNodeTypeIfNeeded(substrait_func, if_node, actions_dag);
    }

private:
    // if (start=0) then throwIf(start=0) else start
    const ActionsDAG::Node * makeStartIfNode(
        ActionsDAGPtr & actions_dag,
        const ActionsDAG::Node * start_arg,
        const ActionsDAG::Node * zero_const_node) const
    {
        const auto * start_equal_zero_node = toFunctionNode(actions_dag, "equals", {start_arg, zero_const_node});
        const auto * start_msg_const_node = addColumnToActionsDAG(actions_dag, std::make_shared<DataTypeString>(), "Unexpected value for start");
        const auto * start_throw_if_node = toFunctionNode(actions_dag, "throwIf", {start_equal_zero_node, start_msg_const_node});
        return toFunctionNode(actions_dag, "if", {start_equal_zero_node, start_throw_if_node, start_arg});
    }

     // if (length<0) then throwIf(length<0) else length
    const ActionsDAG::Node * makeLengthIfNode(
        ActionsDAGPtr & actions_dag,
        const ActionsDAG::Node * length_arg,
        const ActionsDAG::Node * zero_const_node) const
    {
        const auto * length_less_zero_node = toFunctionNode(actions_dag, "less", {length_arg, zero_const_node});
        const auto * length_msg_const_node = addColumnToActionsDAG(actions_dag, std::make_shared<DataTypeString>(), "Unexpected value for length");
        const auto * length_throw_if_node = toFunctionNode(actions_dag, "throwIf", {length_less_zero_node, length_msg_const_node});
        return toFunctionNode(actions_dag, "if", {length_less_zero_node, length_throw_if_node, length_arg});
    }
};

static FunctionParserRegister<FunctionParserArraySlice> register_array_slice;
}

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
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeArray.h>
#include <Common/CHUtil.h>
#include "DataTypes/DataTypeMap.h"
#include <Core/Field.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}
}

namespace local_engine
{

/// map_from_arrays_unaligned is a inner substrait function, which is not exposed to users.
/// It is only used insides posexplode to avoid issue: https://github.com/oap-project/gluten/issues/2492
class FunctionMapFromArraysUnaligned : public FunctionParser
{
public:
    explicit FunctionMapFromArraysUnaligned(SerializedPlanParser * plan_parser_) : FunctionParser(plan_parser_) { }
    ~FunctionMapFromArraysUnaligned() override = default;

    static constexpr auto name = "map_from_arrays_unaligned";

    static constexpr auto type_hint_map = "type_hint:map";
    static constexpr auto type_hint_array= "type_hint:array";

    String getName() const override { return name; }

    const ActionsDAG::Node * parse(
        const substrait::Expression_ScalarFunction & substrait_func,
        ActionsDAGPtr & actions_dag) const override
    {
        /**
            parse mapFromArraysUnaligned(keys, values) as
            mapFromArrays(assumeNotNull(keys), arrayResize(assumeNotNull(values), length(keys)))
        */

        auto parsed_args = parseFunctionArguments(substrait_func, "", actions_dag);
        if (parsed_args.size() != 2)
            throw Exception(DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires two arguments", getName());

        const auto * keys_arg = parsed_args[0];
        const auto * values_arg = parsed_args[1];

        const auto * keys_not_null_node = toFunctionNode(actions_dag, "assumeNotNull", {keys_arg});
        const auto * values_not_null_node = toFunctionNode(actions_dag, "assumeNotNull", {values_arg});

        const auto * array_values_node = values_not_null_node;
        String type_hint = type_hint_array;
        if (isMap(values_not_null_node->result_type))
        {
            const auto * map_type = static_cast<const DataTypeMap *>(values_not_null_node->result_type.get());
            const auto & nested_type = map_type->getNestedType();
            String result_name = "cast(" + values_not_null_node->result_name + " as " + nested_type->getName() + ")";
            array_values_node = ActionsDAGUtil::convertNodeType(
                actions_dag,
                values_not_null_node,
                nested_type->getName(),
                result_name);
            type_hint = type_hint_map;
        }

        const auto * length_keys_node = toFunctionNode(actions_dag, "length", {keys_not_null_node});
        const auto * array_resize_node = toFunctionNode(actions_dag, "arrayResize", {array_values_node, length_keys_node});
        const auto * map_from_arrays_node = toFunctionNode(actions_dag, "mapFromArrays", {keys_not_null_node, array_resize_node});
        auto * result_node = const_cast<DB::ActionsDAG::Node *>(convertNodeTypeIfNeeded(substrait_func, map_from_arrays_node, actions_dag));
        result_node->result_name += " " + type_hint;
        return result_node;
    }
};

static FunctionParserRegister<FunctionMapFromArraysUnaligned> register_map_from_arrays_unaligned;
}

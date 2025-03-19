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

#include <unordered_set>
#include <Parser/AdvancedParametersParseUtil.h>
#include <Parser/SubstraitParserUtils.h>
#include <Rewriter/AnyJoinRewriter.h>
#include <Common/logger_useful.h>

namespace local_engine
{

bool AnyJoinRewriter::visitJoin(substrait::Rel & join_rel)
{
    auto & join = *join_rel.mutable_join();
    auto & right_rel = *join.mutable_right();
    bool has_changed = false;
    auto join_type = join.type();

    do
    {
        if (join_type != substrait::JoinRel_JoinType_JOIN_TYPE_LEFT)
            break;

        // We expect the left side is a read node. It reads from java iter.
        if (join.left().rel_type_case() != substrait::Rel::RelTypeCase::kRead)
            break;

        size_t left_columns_num = join.left().read().base_schema().struct_().types_size();

        if (right_rel.rel_type_case() == substrait::Rel::RelTypeCase::kAggregate && isDeduplicationAggregate(right_rel.aggregate()))
        {
            const auto & aggregate_rel = right_rel.aggregate();
            size_t right_columns_num = aggregate_rel.groupings(0).grouping_expressions_size();
            std::vector<const substrait::Expression *> equal_expressions;
            if (!checkAllRightColumnsAreGroupingKeys(left_columns_num, right_columns_num, join.expression()))
                break;

            substrait::Rel aggregate_input = aggregate_rel.input();
            right_rel.CopyFrom(aggregate_input);
            has_changed = true;
        }
        else if (
            right_rel.rel_type_case() == substrait::Rel::RelTypeCase::kProject
            && right_rel.project().input().rel_type_case() == substrait::Rel::RelTypeCase::kAggregate
            && isDeduplicationAggregate(right_rel.project().input().aggregate()))
        {
            const auto & project_rel = right_rel.project();
            const auto & aggregate_rel = project_rel.input().aggregate();
            size_t right_columns_num = project_rel.expressions_size();

            // ensure that this project only rerange the positions of the columns.
            std::unordered_set<size_t> used_fields;
            bool are_all_select_exprs = true;
            for (const auto & expr : project_rel.expressions())
            {
                auto field_index = SubstraitParserUtils::getStructFieldIndex(expr);
                if (!field_index)
                {
                    are_all_select_exprs = false;
                    break;
                }
                used_fields.insert(*field_index);
            }
            if (!are_all_select_exprs || used_fields.size() != aggregate_rel.groupings(0).grouping_expressions_size())
                break;


            std::vector<const substrait::Expression *> equal_expressions;
            if (!checkAllRightColumnsAreGroupingKeys(left_columns_num, right_columns_num, join.expression()))
                break;

            // ensure all the attibutes from aggregate are used as join keys
            if (equal_expressions.size() != aggregate_rel.groupings(0).grouping_expressions_size())
                break;


            substrait::Rel aggregate_input = aggregate_rel.input();
            join.mutable_right()->mutable_project()->mutable_input()->CopyFrom(aggregate_input);
            has_changed = true;
        }
    } while (0);

    if (has_changed)
    {
        google::protobuf::StringValue optimization_info;
        optimization_info.ParseFromString(join.advanced_extension().optimization().value());
        auto join_opt_info = convertToKVs(optimization_info.value());
        join_opt_info["JoinParameters"]["isAnyJoin"] = "true";
        auto new_opt_info = serializeKVs(join_opt_info);
        optimization_info.set_value(new_opt_info);
        join.mutable_advanced_extension()->mutable_optimization()->set_value(optimization_info.SerializeAsString());
    }
    has_changed |= visitRel(*(join.mutable_left()));
    has_changed |= visitRel(*(join.mutable_right()));
    return has_changed;
}

bool AnyJoinRewriter::isDeduplicationAggregate(const substrait::AggregateRel & aggregate_rel)
{
    return aggregate_rel.measures_size() == 0 && aggregate_rel.groupings_size() == 1;
}

bool AnyJoinRewriter::collectOnJoinEqualConditions(
    const substrait::Expression & e, std::vector<const substrait::Expression *> & equal_expressions) const
{
    if (e.has_scalar_function())
    {
        const auto & scalar_function = e.scalar_function();
        auto function_name = parser_context->getFunctionNameInSignature(scalar_function);
        if (!function_name)
        {
            return false;
        }
        if (function_name == "equal")
        {
            equal_expressions.push_back(&e);
            return true;
        }
        else if (function_name == "and")
        {
            for (const auto & arg : scalar_function.arguments())
            {
                if (!collectOnJoinEqualConditions(arg.value(), equal_expressions))
                    return false;
            }
            return true;
        }
        else
        {
            return false;
        }
    }
    else
    {
        return false;
    }
}

bool AnyJoinRewriter::checkAllRightColumnsAreGroupingKeys(size_t left_columns_num, size_t right_columns_num, const substrait::Expression & join_expression) const
{
    std::vector<const substrait::Expression *> equal_expressions;
    if (!collectOnJoinEqualConditions(join_expression, equal_expressions))
        return false;
    std::vector<size_t> left_keys;
    std::vector<size_t> right_keys;
    for (const auto * equal_expr : equal_expressions)
    {
        const auto & equal_func = equal_expr->scalar_function();
        auto left_op_index = SubstraitParserUtils::getStructFieldIndex(equal_func.arguments(0).value());
        auto right_op_index = SubstraitParserUtils::getStructFieldIndex(equal_func.arguments(1).value());
        if (!left_op_index || !right_op_index)
            return false;
        if (*left_op_index < left_columns_num)
            left_keys.push_back(*left_op_index);
        else if (*left_op_index >= left_columns_num)
            right_keys.push_back(*left_op_index);

        if (*right_op_index >= left_columns_num)
            right_keys.push_back(*right_op_index);
        else if (*right_op_index < left_columns_num)
            left_keys.push_back(*right_op_index);
    }
    return  right_keys.size() == left_keys.size() && right_keys.size() == equal_expressions.size();
}
}

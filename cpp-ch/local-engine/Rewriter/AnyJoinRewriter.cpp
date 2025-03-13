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
    auto join_type = join.join_type();
    if (join_type != substrait::JoinRel_JoinType_JOIN_TYPE_LEFT && join_type != substrait::JoinRel_JoinType_JOIN_TYPE_RIGHT)
        return false;

    do
    {
        if (right_rel.rel_type_case() == substrait::Rel::RelTypeCase::kAggregate && isDeduplicationAggregate(right_rel.aggregate()))
        {
            const auto & aggregate_rel = right_rel.aggregate();
            std::vector<const substrait::Expression *> equal_expressions;
            if (!collectOnJoinEqualConditions(join.expression(), equal_expressions))
                break;

            // ensure all the attibutes from aggregate are used as join keys
            if (equal_expressions.size() != aggregate_rel.groupings(0).grouping_expressions_size())
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
            LOG_ERROR(getLogger("AnyJoinRewriter"), "xxx project + aggregate");
            const auto & project_rel = right_rel.project();
            const auto & aggregate_rel = project_rel.input().aggregate();

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
            LOG_ERROR(
                getLogger("AnyJoinRewriter"),
                "xxx are_all_select_exprs: {}, used_fields: {}, {}",
                are_all_select_exprs,
                used_fields.size(),
                aggregate_rel.groupings(0).grouping_expressions_size());
            if (!are_all_select_exprs || used_fields.size() != aggregate_rel.groupings(0).grouping_expressions_size())
                break;


            std::vector<const substrait::Expression *> equal_expressions;
            if (!collectOnJoinEqualConditions(join.expression(), equal_expressions))
            {
                LOG_ERROR(getLogger("AnyJoinRewriter"), "xxx invalid join expression:{}", join.expression().DebugString());
                break;
            }
            LOG_ERROR(getLogger("AnyJoinRewriter"), "xxx equal_expressions: {}", equal_expressions.size());

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
        LOG_ERROR(getLogger("AnyJoinRewriter"), "xxx has_changed");
        google::protobuf::StringValue optimization_info;
        optimization_info.ParseFromString(join.advanced_extension().optimization().value());
        auto join_opt_info = convertToKVs(optimization_info.value());
        join_opt_info["JoinParameters"]["isAnyJoin"] = "true";
        auto new_opt_info = serializeKVs(join_opt_info);
        optimization_info.set_value(new_opt_info);
        join.mutable_advanced_extension()->mutable_optimization()->set_value(optimization_info.SerializeAsString());
        LOG_ERROR(getLogger("AnyJoinRewriter"), "xxx new_opt_info:{}", join.advanced_extension().optimization().DebugString());
    }
    has_changed |= visitRel(*(join.mutable_left()));
    has_changed |= visitRel(*(join.mutable_right()));
    LOG_ERROR(getLogger("AnyJoinRewriter"), "xxx 2 visit left/right\n{}", join_rel.DebugString());
    return has_changed;
}

bool AnyJoinRewriter::isDeduplicationAggregate(const substrait::AggregateRel & aggregate_rel)
{
    return aggregate_rel.measures_size() == 0 && aggregate_rel.groupings_size() == 1;
}

bool AnyJoinRewriter::collectOnJoinEqualConditions(
    const substrait::Expression & e, std::vector<const substrait::Expression *> & equal_expressions)
{
    if (e.has_scalar_function())
    {
        const auto & scalar_function = e.scalar_function();
        auto function_name = parser_context->getFunctionNameInSignature(scalar_function);
        if (!function_name)
        {
            LOG_ERROR(getLogger("AnyJoinRewriter"), "Unknow scalar function: {}", scalar_function.DebugString());
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
            LOG_ERROR(getLogger("AnyJoinRewriter"), "Unknow function: {}", *function_name);
            return false;
        }
    }
    else
    {
        LOG_ERROR(getLogger("AnyJoinRewriter"), "Unknow expression: {}", e.DebugString());
        return false;
    }
}
}
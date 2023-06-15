#include "Filters.h"
#include <DataTypes/IDataType.h>
#include <Functions/FunctionsLogical.h>
#include <Functions/IFunction.h>
#include <Functions/IFunctionAdaptors.h>
#include <Interpreters/ActionsDAG.h>

namespace DB
{
ColumnPtr PushDownFilter::execute(Block & input)
{
    auto rows = input.rows();
    expr->execute(input, rows, false);
    auto filter = input.getByName(filter_name).column;
    if (filter->isNullable())
    {
        return checkAndGetColumn<ColumnNullable>(*filter)->getNestedColumnPtr();
    }
    return filter;
}
ColumnPtr PageFilter::execute(Block & input)
{
    auto bool_type = std::make_shared<DataTypeUInt8>();
    if (condition_map.empty())
    {
        return bool_type->createColumnConst(1, 1);
    }
    ExpressionActionsPtr condition_expr;
    DB::String filter_name;
    std::vector<ColumnConditionPtr> all_conditions;
    for (const auto & name : input.getNames())
    {
        if (!condition_map.contains(name))
            continue;
        all_conditions.insert(all_conditions.end(), condition_map[name].begin(), condition_map[name].end());
    }
    auto dag = ColumnCondition::mergeConditionsWithAnd(all_conditions);
    filter_name = dag->getOutputs().front()->result_name;
    condition_expr = std::make_shared<ExpressionActions>(dag);
    auto rows = input.rows();
    condition_expr->execute(input, rows, false);
    return input.getByName(filter_name).column;
}
Block PageFilter::getArguments()
{
    Block block;
    for (const auto & item : condition_map)
    {
        block.insert({item.second.front()->getColumnType(), item.second.front()->getColumnName()});
    }
    return block;
}
const String & ColumnCondition::getColumnName() const
{
    return column_name;
}
const String & ColumnCondition::getFilterName() const
{
    return filter_name;
}
ColumnCondition::ColumnCondition(const String & columnName, const String & filterName, const ActionsDAGPtr & expr_)
    : column_name(columnName), filter_name(filterName), expr(expr_)
{
    chassert(expr->getInputs().size() == 1);
    chassert(expr->getOutputs().size() == 1);
}
void ColumnCondition::addLogicalNot()
{
    FunctionOverloadResolverPtr func_builder_not = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionNot>());
    const auto & new_node = expr->addFunction(func_builder_not, {&expr->findInOutputs(filter_name)}, "");
    expr->addOrReplaceInOutputs(new_node);
    expr->removeUnusedResult(filter_name);
    filter_name = new_node.result_name;
}

bool ColumnCondition::supportMinMaxStats() const
{
    WhichDataType which(removeNullable(expr->getInputs().front()->result_type));
    return which.isNativeInt() || which.isFloat() || which.isDateOrDate32();
}

inline bool isFunctionEqualOrNotEqual(const ActionsDAG::Node & node)
{
    chassert(node.type == ActionsDAG::ActionType::FUNCTION);
    return node.function_base->getName() == "equals" || node.function_base->getName() == "notEquals";
}

bool ColumnCondition::supportDictFilter() const
{
    WhichDataType which(removeNullable(expr->getInputs().front()->result_type));
    if (which.isString())
    {
        for (const auto & item : expr->getNodes())
        {
            if (item.type == ActionsDAG::ActionType::FUNCTION && isFunctionEqualOrNotEqual(item))
                return true;
        }
    }
    return false;
}

ActionsDAGPtr ColumnCondition::mergeConditionsWithAnd(std::vector<std::shared_ptr<ColumnCondition>> conditions)
{
    if (conditions.empty())
        return nullptr;
    if (conditions.size() == 1)
    {
        return conditions.front()->expr;
    }
    ActionsDAGPtr dag = std::make_shared<ActionsDAG>();
    FunctionOverloadResolverPtr func_builder_and = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionAnd>());
    bool first = true;
    for (const auto & item : conditions)
    {
        auto clone_expr = item->expr->clone();
        auto left_name = dag->getOutputs().front()->result_name;
        auto right_name = item->expr->getOutputs().front()->result_name;
        dag->mergeNodes(std::move(*clone_expr));
        if (!first)
        {
            const auto & node = dag->addFunction(func_builder_and, dag->getOutputs(), "");
            dag->addOrReplaceInOutputs(node);
            dag->removeUnusedResult(left_name);
            dag->removeUnusedResult(right_name);
        }
        first = false;
    }
    return dag;
}
const DataTypePtr & ColumnCondition::getColumnType() const
{
    return expr->getInputs().front()->result_type;
}

inline bool isFunctionCompare(const ActionsDAG::Node & node)
{
    chassert(node.type == ActionsDAG::ActionType::FUNCTION);
    static std::unordered_set<DB::String> functions = {"equals", "notEquals", "less", "greater", "lessOrEquals", "greaterOrEquals"};
    return functions.contains(node.function_base->getName());
}

inline bool isFunctionAnd(const ActionsDAG::Node & node)
{
    chassert(node.type == ActionsDAG::ActionType::FUNCTION);
    return node.function_base->getName() == "and";
}

inline bool isFunctionOr(const ActionsDAG::Node & node)
{
    chassert(node.type == ActionsDAG::ActionType::FUNCTION);
    return node.function_base->getName() == "or";
}

inline bool isFunctionNot(const ActionsDAG::Node & node)
{
    chassert(node.type == ActionsDAG::ActionType::FUNCTION);
    return node.function_base->getName() == "not";
}

const ActionsDAG::Node * addNodeToActionsDAG(const ActionsDAG::Node & node, ActionsDAGPtr dag)
{
    if (node.type == ActionsDAG::ActionType::INPUT)
    {
        return &dag->addInput(node.result_name, node.result_type);
    }
    else if (node.type == ActionsDAG::ActionType::COLUMN)
    {
        return &dag->addColumn({node.column, node.result_type, node.result_name});
    }
    else if (node.type == ActionsDAG::ActionType::FUNCTION)
    {
        ActionsDAG::NodeRawConstPtrs children;
        for (const auto & item : node.children)
        {
            children.emplace_back(addNodeToActionsDAG(*item, dag));
        }
        return &dag->addFunction(node.function_base, children, node.result_name);
    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "unimplemented node type");
    }
}

std::vector<ColumnConditionPtr> getConditionsFromNode(const ActionsDAG::Node & node)
{
    std::vector<ColumnConditionPtr> result;
    if (isFunctionAnd(node))
    {
        auto left = getConditionsFromNode(*node.children.at(0));
        auto right = getConditionsFromNode(*node.children.at(1));
        result.insert(result.end(), left.begin(), left.end());
        result.insert(result.end(), left.begin(), left.end());
    }
    else if (isFunctionOr(node))
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "or function are not supported");
    }
    else if (isFunctionNot(node))
    {
        auto conditions = getConditionsFromNode(*node.children.at(0));
        for (auto & item : conditions)
        {
            item->addLogicalNot();
        }
        result.insert(result.end(), conditions.begin(), conditions.end());
    }
    else if (isFunctionCompare(node))
    {
        ActionsDAGPtr dag = std::make_shared<ActionsDAG>();
        const auto * new_node = addNodeToActionsDAG(node, dag);
        dag->addOrReplaceInOutputs(*new_node);
        ColumnConditionPtr condition = std::make_shared<ColumnCondition>(dag->getInputs().at(0)->result_name, new_node->result_name, dag);
        result.emplace_back(condition);
    }
    return result;
}

/// extract function node (exclude logical function)
std::vector<ColumnConditionPtr> splitColumnConditions(ActionsDAGPtr expr)
{
    std::vector<ColumnConditionPtr> result;
    for (const auto & item : expr->getNodes())
    {
        if (item.type != ActionsDAG::ActionType::FUNCTION)
            continue;
        auto conditions = getConditionsFromNode(item);
        result.insert(result.end(), conditions.begin(), conditions.end());
    }
    return result;
}

RowGroupFilterPtr PushDownFilter::getRowGroupFilter()
{
    auto conditions = splitColumnConditions(dag);
    return std::make_shared<RowGroupFilter>(conditions);
}
PageFilterPtr PushDownFilter::getPageFilter()
{
    auto conditions = splitColumnConditions(dag);
    return std::make_shared<PageFilter>(conditions);
}

Block RowGroupFilter::getArguments()
{
    Block block;
    for (const auto & item : condition_map)
    {
        block.insert({item.second.front()->getColumnType(), item.second.front()->getColumnName()});
    }
    return block;
}

ColumnPtr RowGroupFilter::execute(Block & input)
{
    auto bool_type = std::make_shared<DataTypeUInt8>();
    if (condition_map.empty())
    {
        return bool_type->createColumnConst(1, 1);
    }
    if (!condition_expr)
    {
        std::vector<ColumnConditionPtr> all_conditions;
        for (const auto & name : input.getNames())
        {
            if (!condition_map.contains(name))
                continue;
            all_conditions.insert(all_conditions.end(), condition_map[name].begin(), condition_map[name].end());
        }
        auto dag = ColumnCondition::mergeConditionsWithAnd(all_conditions);
        filter_name = dag->getOutputs().front()->result_name;
        condition_expr = std::make_shared<ExpressionActions>(dag);
    }
    condition_expr->execute(input);
    return input.getByName(filter_name).column;
}
}

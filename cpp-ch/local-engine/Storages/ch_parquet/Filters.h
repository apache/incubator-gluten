#pragma once
#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ExpressionActions.h>

namespace DB
{
class ColumnCondition
{
public:
    static ActionsDAGPtr mergeConditionsWithAnd(std::vector<std::shared_ptr<ColumnCondition>> conditions);

    ColumnCondition(const String & columnName, const String & filterName, const ActionsDAGPtr & expr);

    const String & getColumnName() const;
    const DataTypePtr & getColumnType() const;
    const String & getFilterName() const;
    void addLogicalNot();
    bool supportMinMaxStats() const;
    bool supportDictFilter() const;


private:
    DB::String column_name;
    DB::String filter_name;
    /// only one expression
    ActionsDAGPtr expr;
};

using ColumnConditionPtr = std::shared_ptr<ColumnCondition>;

class IFilter
{
public:
    virtual ~IFilter() = default;
    virtual ColumnPtr execute(Block & input) = 0;
    virtual Block getArguments() = 0;
};

class RowGroupFilter;
using RowGroupFilterPtr = std::shared_ptr<RowGroupFilter>;
class PageFilter;
using PageFilterPtr = std::shared_ptr<PageFilter>;

class PushDownFilter : public IFilter
{
public:
    explicit PushDownFilter(ActionsDAGPtr dag_) : dag(dag_)
    {
        filter_name = dag->getOutputs().back()->result_name;
        Names names = {filter_name};
        dag->removeUnusedActions(names);
        for (const auto & item : dag->getInputs())
        {
            condition_columns.insert(item->result_name);
        }
        dag->projectInput(true);
        for (const auto & item : dag->getInputs())
        {
            dag->tryRestoreColumn(item->result_name);
        }
        expr = std::make_shared<ExpressionActions>(dag);
    }
    ~PushDownFilter() override { }

    const std::unordered_set<DB::String> & getConditionColumns() { return condition_columns; }
    RowGroupFilterPtr getRowGroupFilter();
    PageFilterPtr getPageFilter();
    ColumnPtr execute(Block & input) override;
    Block getArguments() override { return {}; }

private:
    ActionsDAGPtr dag;
    std::unordered_set<DB::String> condition_columns;
    ExpressionActionsPtr expr;
    DB::String filter_name;
};

class RowGroupFilter : public IFilter
{
public:
    explicit RowGroupFilter(std::vector<ColumnConditionPtr> conditions_)
    {
        for (const auto & item : conditions_)
        {
            if (item->supportMinMaxStats())
            {
                condition_map[item->getColumnName()].emplace_back(item);
            }
        }
    }
    ~RowGroupFilter() override { }
    Block getArguments() override;
    ColumnPtr execute(Block & input) override;

private:
    std::unordered_map<DB::String, std::vector<ColumnConditionPtr>> condition_map;
    ExpressionActionsPtr condition_expr;
    DB::String filter_name;
};

class PageFilter : public IFilter
{
public:
    explicit PageFilter(std::vector<ColumnConditionPtr> conditions_)
    {
        for (const auto & item : conditions_)
        {
            if (item->supportMinMaxStats())
            {
                condition_map[item->getColumnName()].emplace_back(item);
                condition_columns.insert(item->getColumnName());
            }
        }
    }
    ~PageFilter() override { }

    ColumnPtr execute(Block & input) override;
    Block getArguments() override;
    const std::unordered_set<DB::String> & getAllConditions() const { return condition_columns; }

private:
    std::unordered_set<DB::String> condition_columns;
    std::unordered_map<DB::String, std::vector<ColumnConditionPtr>> condition_map;
};

class PageDictFilter : public PageFilter
{
};


}

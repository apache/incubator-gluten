#include <Storages/SubstraitSource/SubstraitFileSourceStep.h>
#include <Storages/SubstraitSource/SubstraitFileSource.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace local_engine
{

SubstraitFileSourceStep::SubstraitFileSourceStep(DB::ContextPtr context_, DB::Pipe pipe_, String)
    : SourceStepWithFilter(DB::DataStream{.header = pipe_.getHeader()}), pipe(std::move(pipe_)), context(context_) {}


void SubstraitFileSourceStep::initializePipeline(DB::QueryPipelineBuilder & pipeline, const DB::BuildQueryPipelineSettings &)
{
    for (const auto & processor : pipe.getProcessors())
        processors.emplace_back(processor);

    pipeline.init(std::move(pipe));
}

void SubstraitFileSourceStep::applyFilters()
{
    if (filter_dags.size() == 0 || filter_nodes.nodes.size() == 0)
        return;
    std::vector<DB::KeyCondition> filters;
    const DB::Block header = pipe.getHeader();
    const DB::ColumnsWithTypeAndName columns = header.getColumnsWithTypeAndName();

    std::unordered_map<std::string, DB::ColumnWithTypeAndName> node_name_to_input_column;
    for (size_t i=0; i < columns.size(); ++i)
    {
        DB::ColumnWithTypeAndName column = columns[i];
        node_name_to_input_column.insert({column.name, column});
    }
    std::shared_ptr<DB::ExpressionActions> filter_expr = std::make_shared<DB::ExpressionActions>(filter_dags[0], DB::ExpressionActionsSettings::fromContext(context));
    std::vector<std::string> filter_columns = { "id", "name", "sex" };
    if (filter_columns.size() != 0)
    {
        DB::ActionsDAGPtr filter_actions_dag = DB::ActionsDAG::buildFilterActionsDAG(filter_nodes.nodes, node_name_to_input_column, context);
        DB::KeyCondition filter_condition(filter_actions_dag, context, filter_columns, filter_expr, DB::NameSet{});
        filters.push_back(filter_condition);
    }
    
    DB::Processors processors = pipe.getProcessors();
    for (size_t i = 0; i < processors.size(); ++i)
    {
        DB::ProcessorPtr processor = processors[i];
        const SubstraitFileSource * source = static_cast<const SubstraitFileSource *>(processor.get());
        if (source)
            source->applyFilters(filters);
    }
}

}

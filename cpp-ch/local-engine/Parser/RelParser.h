#pragma once
#include <map>
#include <optional>
#include <unordered_map>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Core/Field.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Parser/SerializedPlanParser.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <base/types.h>
#include <google/protobuf/repeated_field.h>
#include <substrait/plan.pb.h>
namespace local_engine
{
/// parse a single substrait relation
class RelParser
{
public:
    explicit RelParser(SerializedPlanParser * plan_parser_) : plan_parser(plan_parser_) { }

    virtual ~RelParser() = default;
    virtual DB::QueryPlanPtr
    parse(DB::QueryPlanPtr current_plan_, const substrait::Rel & rel, std::list<const substrait::Rel *> & rel_stack_)
        = 0;

    static AggregateFunctionPtr getAggregateFunction(
        DB::String & name, DB::DataTypes arg_types, DB::AggregateFunctionProperties & properties, const DB::Array & parameters = {});

    static DB::DataTypePtr parseType(const substrait::Type & type) { return SerializedPlanParser::parseType(type); }

protected:
    inline ContextPtr getContext() { return plan_parser->context; }

    inline String getUniqueName(const std::string & name) { return plan_parser->getUniqueName(name); }

    inline const std::unordered_map<std::string, std::string> & getFunctionMapping() { return plan_parser->function_mapping; }
    // Get function signature name.
    std::optional<String> parseSignatureFunctionName(UInt32 function_ref);
    // Get coresponding function name in ClickHouse.
    std::optional<String> parseFunctionName(UInt32 function_ref, const substrait::Expression_ScalarFunction & function);

    const DB::ActionsDAG::Node * parseArgument(ActionsDAGPtr action_dag, const substrait::Expression & rel)
    {
        return plan_parser->parseExpression(action_dag, rel);
    }
    std::pair<DataTypePtr, Field> parseLiteral(const substrait::Expression_Literal & literal) { return plan_parser->parseLiteral(literal); }

    const ActionsDAG::Node *
    buildFunctionNode(ActionsDAGPtr action_dag, const String & function, const DB::ActionsDAG::NodeRawConstPtrs & args)
    {
        return plan_parser->toFunctionNode(action_dag, function, args);
    }

    DB::AggregateFunctionPtr getAggregateFunction(const std::string & name, DB::DataTypes arg_types)
    {
        auto & factory = DB::AggregateFunctionFactory::instance();
        DB::AggregateFunctionProperties properties;
        return factory.get(name, arg_types, DB::Array{}, properties);
    }

private:
    SerializedPlanParser * plan_parser;
};

class RelParserFactory
{
protected:
    RelParserFactory() = default;

public:
    using RelParserBuilder = std::function<std::shared_ptr<RelParser>(SerializedPlanParser *)>;
    static RelParserFactory & instance();
    void registerBuilder(UInt32 k, RelParserBuilder builder);
    RelParserBuilder getBuilder(DB::UInt32 k);

private:
    std::map<UInt32, RelParserBuilder> builders;
};

void registerRelParsers();
}

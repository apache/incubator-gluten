#pragma once
#include <map>
#include <optional>
#include <unordered_map>
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
    explicit RelParser(SerializedPlanParser * plan_parser_)
        :plan_parser(plan_parser_)
    {}

    virtual ~RelParser() = default;
    virtual DB::QueryPlanPtr parse(DB::QueryPlanPtr current_plan_, const substrait::Rel & rel, std::list<const substrait::Rel *> & rel_stack_) = 0;

    static AggregateFunctionPtr getAggregateFunction(
        DB::String & name,
        DB::DataTypes arg_types,
        DB::AggregateFunctionProperties & properties,
        const DB::Array & parameters = {});

public:
    static DB::DataTypePtr parseType(const substrait::Type & type) { return SerializedPlanParser::parseType(type); }
protected:
    inline ContextPtr getContext() { return plan_parser->context; }
    inline String getUniqueName(const std::string & name) { return plan_parser->getUniqueName(name); }
    inline const std::unordered_map<std::string, std::string> & getFunctionMapping() { return plan_parser->function_mapping; }
    std::optional<String> parseFunctionName(UInt32 function_ref);
    static DB::DataTypes parseFunctionArgumentTypes(const Block & header, const google::protobuf::RepeatedPtrField<substrait::FunctionArgument> & func_args);
    static DB::DataTypePtr parseExpressionType(const Block & header, const substrait::Expression & expr);
    static DB::Names parseFunctionArgumentNames(const Block & header, const google::protobuf::RepeatedPtrField<substrait::FunctionArgument> & func_args);
    const DB::ActionsDAG::Node * parseArgument(ActionsDAGPtr action_dag, const substrait::Expression & rel)
    {
        return plan_parser->parseArgument(action_dag, rel);
    }
    std::pair<DataTypePtr, Field> parseLiteral(const substrait::Expression_Literal & literal)
    {
        return plan_parser->parseLiteral(literal);
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

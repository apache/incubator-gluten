#include "RelParser.h"
#include <string>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <DataTypes/IDataType.h>
#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}
}

namespace local_engine
{
AggregateFunctionPtr RelParser::getAggregateFunction(
    DB::String & name, DB::DataTypes arg_types, DB::AggregateFunctionProperties & properties, const DB::Array & parameters)
{
    auto & factory = AggregateFunctionFactory::instance();
    return factory.get(name, arg_types, parameters, properties);
}

std::optional<String> RelParser::parseFunctionName(UInt32 function_ref)
{
    const auto & function_mapping = getFunctionMapping();
    auto it = function_mapping.find(std::to_string(function_ref));
    if (it == function_mapping.end())
    {
        return {};
    }
    auto function_signature = it->second;
    auto function_name = function_signature.substr(0, function_signature.find(':'));
    return function_name;
}

DB::DataTypes RelParser::parseFunctionArgumentTypes(
    const Block & header, const google::protobuf::RepeatedPtrField<substrait::FunctionArgument> & func_args)
{
    DB::DataTypes res;
    for (const auto & arg : func_args)
    {
        if (!arg.has_value())
        {
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Expect a FunctionArgument with value field");
        }
        res.emplace_back(parseExpressionType(header, arg.value()));
    }
    return res;
}

DB::DataTypePtr RelParser::parseExpressionType(const Block & header, const substrait::Expression & expr)
{
    DB::DataTypePtr res;
    if (expr.has_selection())
    {
        auto pos = expr.selection().direct_reference().struct_field().field();
        res = header.getByPosition(pos).type;
    }
    else if (expr.has_literal())
    {
        auto [data_type, _] = SerializedPlanParser::parseLiteral(expr.literal());
        res = data_type;
    }
    else
    {
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Unknow FunctionArgument: {}", expr.DebugString());
    }
    return res;
}


DB::Names RelParser::parseFunctionArgumentNames(
    const Block & header, const google::protobuf::RepeatedPtrField<substrait::FunctionArgument> & func_args)
{
    DB::Names res;
    for (const auto & arg : func_args)
    {
        if (!arg.has_value())
        {
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Expect a FunctionArgument with value field");
        }
        const auto & value = arg.value();
        if (value.has_selection())
        {
            auto pos = value.selection().direct_reference().struct_field().field();
            res.push_back(header.getByPosition(pos).name);
        }
        else if (value.has_literal())
        {
            auto [_, field] = SerializedPlanParser::parseLiteral(value.literal());
            res.push_back(field.dump());
        }
        else
        {
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Unknow FunctionArgument: {}", arg.DebugString());
        }
    }
    return res;
}

RelParserFactory & RelParserFactory::instance()
{
    static RelParserFactory factory;
    return factory;
}

void RelParserFactory::registerBuilder(UInt32 k, RelParserBuilder builder)
{
    auto it = builders.find(k);
    if (it != builders.end())
    {
        throw Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Duplicated builder key:{}", k);
    }
    builders[k] = builder;
}

RelParserFactory::RelParserBuilder RelParserFactory::getBuilder(DB::UInt32 k)
{
    auto it = builders.find(k);
    if (it == builders.end())
    {
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Not found builder for key:{}", k);
    }
    return it->second;
}

void registerWindowRelParser(RelParserFactory & factory);
void registerSortRelParser(RelParserFactory & factory);
void registerExpandRelParser(RelParserFactory & factory);

void registerRelParsers()
{
    auto & factory = RelParserFactory::instance();
    registerWindowRelParser(factory);
    registerSortRelParser(factory);
    registerExpandRelParser(factory);
}
}

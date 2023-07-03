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

std::optional<String> RelParser::parseSignatureFunctionName(UInt32 function_ref)
{
    const auto & function_mapping = getFunctionMapping();
    auto it = function_mapping.find(std::to_string(function_ref));
    if (it == function_mapping.end())
        return {};

    auto function_signature = it->second;
    auto function_name = function_signature.substr(0, function_signature.find(':'));
    return function_name;
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
        throw Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Duplicated builder key:{}", k);

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
void registerAggregateParser(RelParserFactory & factory);

void registerRelParsers()
{
    auto & factory = RelParserFactory::instance();
    registerWindowRelParser(factory);
    registerSortRelParser(factory);
    registerExpandRelParser(factory);
    registerAggregateParser(factory);
}
}

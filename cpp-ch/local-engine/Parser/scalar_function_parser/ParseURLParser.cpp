#include "ParseURLParser.h"
#include <iterator>
#include <Common/Exception.h>
#include <DataTypes/IDataType.h>
#include <unordered_map>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int NOT_IMPLEMENTED;
}
}

namespace local_engine
{
String ParseURLParser::getCHFunctionName(const CommonFunctionInfo & func_info) const
{
    if (func_info.arguments.size() < 2)
    {
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "parse_url() expects at least 2 arguments");
    }
    return selectCHFunctionName(func_info);
}

String ParseURLParser::getCHFunctionName(const DB::DataTypes &) const
{
    throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "Not implemented");
}

const DB::ActionsDAG::Node * ParseURLParser::parse(const CommonFunctionInfo & func_info, DB::ActionsDAGPtr & actions_dag) const
{
    return FunctionParser::parse(func_info, actions_dag);
}

String ParseURLParser::getQueryPartName(const substrait::Expression & expr) const
{
    if (!expr.has_literal())
    {
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "parse_url() expects a string literal as the 2nd argument");
    }

    auto [data_type, field] = parseLiteral(expr.literal());
    DB::WhichDataType ty_which(data_type);
    if (!ty_which.isString())
    {
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "parse_url() 2nd argument must be a string literal");
    }

    return field.safeGet<String>();
}

const static String CH_URL_PROTOL_FUNCTION = "protocol";
const static String CH_URL_PATH_FUNCTION = "spark_parse_url_path";
const static String CH_URL_REF_FUNCTION = "spark_parse_url_ref";
const static String CH_URL_USERINFO_FUNCTION = "spark_parse_url_userinfo";
const static String CH_URL_FILE_FUNCTION = "spark_parse_url_file";
const static String CH_URL_AUTHORITY_FUNCTION = "spark_parse_url_authority";
const static String CH_URL_HOST_FUNCTION = "spark_parse_url_host";
const static String CH_URL_PARAMS_FUNCTION = "spark_parse_url_query";
const static String CH_URL_ONE_PARAM_FUNCTION = "spark_parse_url_one_query";
const static String CH_URL_INVALID_FUNCTION = "spark_parse_url_invalid";
String ParseURLParser::selectCHFunctionName(const CommonFunctionInfo & func_info) const
{
    auto query_part_name = getQueryPartName(func_info.arguments[1].value());
    if (query_part_name == "QUERY")
    {
        if (func_info.arguments.size() == 2)
            return CH_URL_PARAMS_FUNCTION;
        else
            return CH_URL_ONE_PARAM_FUNCTION;
    }
    else if (query_part_name == "PROTOCOL")
        return CH_URL_PROTOL_FUNCTION;
    else if (query_part_name == "PATH")
        return CH_URL_PATH_FUNCTION;
    else if (query_part_name == "HOST")
        return CH_URL_HOST_FUNCTION;
    else if (query_part_name == "REF")
        return CH_URL_REF_FUNCTION;
    else if (query_part_name == "FILE")
        return CH_URL_FILE_FUNCTION;
    else if (query_part_name == "AUTHORITY")
        return CH_URL_AUTHORITY_FUNCTION;
    else if (query_part_name == "USERINFO")
        return CH_URL_USERINFO_FUNCTION;
    else
    {
        // throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Unknown query part name {} ", query_part_name);
        return CH_URL_INVALID_FUNCTION;
    }
}

DB::ActionsDAG::NodeRawConstPtrs ParseURLParser::parseFunctionArguments(
    const CommonFunctionInfo & func_info, const String & /*ch_func_name*/, DB::ActionsDAGPtr & actions_dag) const
{
    DB::ActionsDAG::NodeRawConstPtrs arg_nodes;
    arg_nodes.push_back(parseExpression(actions_dag, func_info.arguments[0].value()));
    for (Int32 i = 2; i < func_info.arguments.size(); ++i)
    {
        arg_nodes.push_back(parseExpression(actions_dag, func_info.arguments[i].value()));
    }
    return arg_nodes;
}

const DB::ActionsDAG::Node * ParseURLParser::convertNodeTypeIfNeeded(
    const CommonFunctionInfo & func_info, const DB::ActionsDAG::Node * func_node, DB::ActionsDAGPtr & actions_dag) const
{
    auto ch_function_name = getCHFunctionName(func_info);
    if (ch_function_name != CH_URL_PROTOL_FUNCTION)
    {
        return func_node;
    }
    // Empty string is converted to NULL.
    auto str_type = std::make_shared<DB::DataTypeString>();
    const auto * empty_str_node = &actions_dag->addColumn(ColumnWithTypeAndName(str_type->createColumnConst(1, DB::Field("")), str_type, getUniqueName("")));
    return toFunctionNode(actions_dag, "nullIf", {func_node, empty_str_node});
}

FunctionParserRegister<ParseURLParser> register_scalar_function_parser_parse_url;
}

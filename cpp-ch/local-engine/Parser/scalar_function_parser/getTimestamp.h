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

#include <DataTypes/DataTypeNullable.h>
#include <Parser/FunctionParser.h>
#include <Core/Settings.h>
#include <Core/Field.h>
#include <Common/CHUtil.h>
#include <boost/algorithm/string/case_conv.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}
}


namespace local_engine
{
class FunctionParserGetTimestamp : public FunctionParser
{
public:
    explicit FunctionParserGetTimestamp(ParserContextPtr parser_context_) : FunctionParser(parser_context_) {}
    ~FunctionParserGetTimestamp() override = default;

    static constexpr auto name = "get_timestamp";
    String getName() const override { return name; }

    const ActionsDAG::Node * parse(
        const substrait::Expression_ScalarFunction & substrait_func,
        ActionsDAG & actions_dag) const override
    {
        /*
        spark function: get_timestamp(expr, fmt)
        1. If timeParserPolicy is LEGACY
            1) fmt has 0 'S', ch function = parseDateTime64InJodaSyntaxOrNull(substr(expr,1,length(fmt)), fmt);
            2) fmt has 'S' more than 0, make the fmt has 3 'S', ch function =  parseDateTime64InJodaSyntaxOrNull(expr, fmt)
        2. Else ch function = parseDateTime64InJodaSyntaxOrNull(expr, fmt)
        */
        auto parsed_args = parseFunctionArguments(substrait_func, actions_dag);
        if (parsed_args.size() != 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires exactly two arguments", getName());
        
        const auto & args = substrait_func.arguments();
        String fmt_str;
        UInt32 scale = 0;
        if(args[1].value().has_literal())
        {
            const auto & literal_expr = args[1].value().literal();
            if (literal_expr.has_string())
            {
                fmt_str = literal_expr.string();
                scale = std::count(fmt_str.begin(), fmt_str.end(), 'S');
            }
            else
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "The second of function {} must be const String.", name);
        }
        else
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "The second of function {} must be const String.", name);

        const auto * expr_arg = parsed_args[0];
        const auto * fmt_arg = parsed_args[1];
        const DateLUTImpl * date_lut = &DateLUT::instance();
        const auto * time_zone_node = addColumnToActionsDAG(actions_dag, std::make_shared<DataTypeString>(), date_lut->getTimeZone());
        const auto * scale_node = addColumnToActionsDAG(actions_dag, std::make_shared<DataTypeUInt8>(), scale);
        String timeParserPolicy;
        const ContextPtr context = getContext();
        if (context->getSettingsRef().has(TIMER_PARSER_POLICY))
            timeParserPolicy = toString(context->getSettingsRef().get(TIMER_PARSER_POLICY));
        boost::to_lower(timeParserPolicy);
        if (timeParserPolicy == "legacy")
        {
            if (scale < 3)
            {
                if (scale == 0)
                {
                    const auto * index_begin_node = addColumnToActionsDAG(actions_dag, std::make_shared<DataTypeUInt64>(), 1);
                    const auto * index_end_node = addColumnToActionsDAG(actions_dag, std::make_shared<DataTypeUInt64>(), fmt_str.size());
                    const auto * substr_node = toFunctionNode(actions_dag, "substringUTF8", {expr_arg, index_begin_node, index_end_node});
                    const auto * fmt_node = addColumnToActionsDAG(actions_dag, std::make_shared<DataTypeString>(), fmt_str);
                    const auto * result_node = toFunctionNode(actions_dag, "parseDateTime64InJodaSyntaxOrNull", {substr_node, scale_node, fmt_node, time_zone_node});
                    return convertNodeTypeIfNeeded(substrait_func, result_node, actions_dag);
                }
                else
                    for (size_t i = 0; i < 3 - scale; ++i)
                        fmt_str += 'S';
            }
            else
                fmt_str = fmt_str.substr(0, fmt_str.size() - (scale - 3));
            
            const auto * fmt_node = addColumnToActionsDAG(actions_dag, std::make_shared<DataTypeString>(), fmt_str);
            const auto * new_scale_node = addColumnToActionsDAG(actions_dag, std::make_shared<DataTypeUInt8>(), 3);
            const auto * result_node = toFunctionNode(actions_dag, "parseDateTime64InJodaSyntaxOrNull", {expr_arg, new_scale_node, fmt_node, time_zone_node});
            return convertNodeTypeIfNeeded(substrait_func, result_node, actions_dag);
        }
        else
        {
            const auto * result_node = toFunctionNode(actions_dag, "parseDateTime64InJodaSyntaxOrNull", {expr_arg, scale_node, fmt_arg, time_zone_node});
            return convertNodeTypeIfNeeded(substrait_func, result_node, actions_dag);
        }
    }
};
}

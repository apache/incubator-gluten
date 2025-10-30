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
#pragma once

#include <Core/Field.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Parser/FunctionParser.h>
#include <boost/algorithm/string/case_conv.hpp>
#include <Common/CHUtil.h>

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

    const DB::ActionsDAG::Node * parse(
        const substrait::Expression_ScalarFunction & substrait_func,
        DB::ActionsDAG & actions_dag) const override
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
            throw DB::Exception(DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires exactly two arguments", getName());
        const auto * expr_arg = parsed_args[0];
        const auto * fmt_arg = parsed_args[1];
        
        const auto & args = substrait_func.arguments();
        bool fmt_string_literal = args[1].value().has_literal();
        String fmt;
        if (fmt_string_literal)
        {
            const auto & literal_fmt_expr = args[1].value().literal();
            fmt_string_literal = literal_fmt_expr.has_string();
            fmt = fmt_string_literal ? literal_fmt_expr.string() : "";
        }
        if (!fmt_string_literal)
            throw DB::Exception(DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "The second argument of function {} must be const String.", name);

        UInt32 s_count = std::count(fmt.begin(), fmt.end(), 'S');
        String time_parser_policy = getContext()->getSettingsRef().has(TIMER_PARSER_POLICY) ? toString(getContext()->getSettingsRef().get(TIMER_PARSER_POLICY)) : "";
        boost::to_lower(time_parser_policy);
        if (time_parser_policy == "legacy" && checkFormat(fmt))
        {
            if (s_count == 0)
            {
                // If fmt == "yyyy-MM-dd" or fmt == "yyyy-MM-dd HH" or fmt == "yyyy-MM-dd HH:mm" or fmt == "yyyy-MM-dd HH:mm:ss", parse the timestamp by using the following logic.
                const auto * index_begin_node = addColumnToActionsDAG(actions_dag, std::make_shared<DB::DataTypeUInt64>(), 1);
                const auto * index_end_node = addColumnToActionsDAG(actions_dag, std::make_shared<DB::DataTypeUInt64>(), fmt.size());
                const auto * substr_node = toFunctionNode(actions_dag, "substringUTF8", {expr_arg, index_begin_node, index_end_node});
                const auto * fmt_node = addColumnToActionsDAG(actions_dag, std::make_shared<DB::DataTypeString>(), fmt);
                const auto * result_node = toFunctionNode(actions_dag, "parseDateTime64InJodaSyntaxOrNull", {substr_node, fmt_node});
                return convertNodeTypeIfNeeded(substrait_func, result_node, actions_dag);
            }
            else if (s_count < 3)
                fmt += String(3 - s_count, 'S');
            else
                fmt = fmt.substr(0, fmt.size() - (s_count - 3));
            const auto * fmt_node = addColumnToActionsDAG(actions_dag, std::make_shared<DB::DataTypeString>(), fmt);
            const auto * result_node = toFunctionNode(actions_dag, "parseDateTime64InJodaSyntaxOrNull", {expr_arg, fmt_node});
            return convertNodeTypeIfNeeded(substrait_func, result_node, actions_dag);
        }
        else
        {
            const auto * result_node = toFunctionNode(actions_dag, "parseDateTime64InJodaSyntaxOrNull", {expr_arg, fmt_arg});
            return convertNodeTypeIfNeeded(substrait_func, result_node, actions_dag);
        }
    }

private:
    bool checkFormat(const String& fmt) const
    {
        if (fmt.size() < 10)
        {
            return false;
        }
        else
        {
            const String yearFmt = fmt.substr(0, 4);
            const String monthFmt = fmt.substr(5, 2);
            const String dayFmt = fmt.substr(8, 2);
            bool yearMonthDayValid = yearFmt == "yyyy" && monthFmt == "MM" && dayFmt == "dd";
            if (fmt.size() == 10)
            {
                return yearMonthDayValid;
            }
            else if (yearMonthDayValid)
            {
                const String splitChar = fmt.size() >= 11 ? fmt.substr(10, 1) : "";
                if (splitChar != " " && splitChar != "T")
                {
                    return false;
                }
                const String hourFmt = fmt.size() >= 13 ? fmt.substr(11, 2) : "";
                const String minuteFmt = fmt.size() >= 15 ? fmt.substr(14, 2) : "";
                const String secondFmt = fmt.size() >= 17 ? fmt.substr(17, 2) : "";
                bool hourValid = hourFmt == "HH";
                bool minuteValid = minuteFmt == "mm";
                bool secondValid = secondFmt == "ss";
                if ((fmt.size() == 13 && hourValid) || (fmt.size() == 16 && hourValid && minuteValid) || (fmt.size() == 19 && hourValid && minuteValid && secondValid)) 
                {
                    return true;
                }
                if (fmt.size() > 19 && fmt.substr(19, 1) == ".") 
                {
                    return true;
                }
            }
            return false;
        }
    }
};
}

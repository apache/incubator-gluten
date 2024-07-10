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

#include <stack>

#include <Parser/FunctionParser.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}
}

using namespace DB;

namespace local_engine
{
class FunctionParserRegexpExtract : public FunctionParser
{
public:
    explicit FunctionParserRegexpExtract(SerializedPlanParser * plan_parser_) : FunctionParser(plan_parser_) {}
    ~FunctionParserRegexpExtract() override = default;

    static constexpr auto name = "regexp_extract";
    String getName() const override { return name; }

    const ActionsDAG::Node * parse(
        const substrait::Expression_ScalarFunction & substrait_func,
        ActionsDAGPtr & actions_dag) const override
    {
        const auto & args = substrait_func.arguments();
        if (args.size() != 3)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires 3 arguments", getName());
        
        if(args[1].value().has_literal())
        {
            const auto & literal_expr = args[1].value().literal();
            if (literal_expr.has_string())
            {
                std::string expr_str = literal_expr.string();
                size_t expr_size = expr_str.size();
                if (expr_str.data()[expr_size - 1] == '$')
                    expr_str.replace(expr_str.find_last_of("$"), 1, "(?:(\n)*)$");

                String sparkRegexp = adjustSparkRegexpRule(expr_str);
                const auto * regex_expr_node = addColumnToActionsDAG(actions_dag, std::make_shared<DataTypeString>(), sparkRegexp);
                auto parsed_args = parseFunctionArguments(substrait_func, actions_dag);
                parsed_args[1] = regex_expr_node;
                const auto * result_node = toFunctionNode(actions_dag, "regexpExtract", parsed_args);
                return convertNodeTypeIfNeeded(substrait_func, result_node, actions_dag);
            }
            else
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function {} 2nd argument's type must be const string", getName());
        }
        else
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function {} 2nd argument's type must be const", getName());
    }

private:
    String adjustSparkRegexpRule(String & str) const
    {
        const auto left_bracket_pos = str.find('[');
        const auto right_bracket_pos = str.find(']');

        if (left_bracket_pos == str.npos || right_bracket_pos == str.npos || left_bracket_pos >= right_bracket_pos)
            return str;

        auto throw_message = [this, &str]() -> void {
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "The value of parameter(s) 'regexp' in `{}` is invalid: '{}'", getName(), str);
        };

        ReadBufferFromString buf(str);
        std::stack<String> strs;
        strs.emplace("");
        bool nead_right_bracket = false;

        while (!buf.eof())
        {
            if (*buf.position() == '[')
            {
                strs.emplace("");
            }
            else if (*buf.position() == ']')
            {
                if (strs.size() == 1)
                {
                    // "ab]c"
                    strs.top().append("]");
                }
                else
                {
                    String back = strs.top();
                    strs.pop();
                    if (strs.size() == 1)
                    {
                        // "abc[abc]abc"
                        strs.top().append("[").append(back).append("]");
                        nead_right_bracket = false;
                    }
                    else
                    {
                        // "abc[a[abc]c]abc"
                        strs.top().append(back);
                        nead_right_bracket = true;
                    }
                }
            }
            else
            {
                strs.top() += *buf.position();
            }

            ++buf.position();
        }

        if (nead_right_bracket && strs.size() != 1)
            throw_message();

        while (strs.size() != 1)
        {
            String back = strs.top();
            strs.pop();
            strs.top().append("[").append(back);
        }

        return strs.top();
    }
};

static FunctionParserRegister<FunctionParserRegexpExtract> register_regexp_extract;
}

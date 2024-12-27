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
#include <Parser/FunctionParser.h>
namespace local_engine
{
class ParseURLParser final : public FunctionParser
{
public:
    static constexpr auto name = "parse_url";
    ParseURLParser(ParserContextPtr parser_context_) : FunctionParser(parser_context_) { }
    ~ParseURLParser() override = default;
    String getName() const override { return name; }

    String getCHFunctionName(const substrait::Expression_ScalarFunction & substrait_func) const override;
protected:

    DB::ActionsDAG::NodeRawConstPtrs parseFunctionArguments(
        const substrait::Expression_ScalarFunction & substrait_func,
        DB::ActionsDAG & actions_dag) const override;

    const DB::ActionsDAG::Node * convertNodeTypeIfNeeded(
        const substrait::Expression_ScalarFunction & substrait_func,
        const DB::ActionsDAG::Node * func_node,
        DB::ActionsDAG & actions_dag) const override;

private:
    String getQueryPartName(const substrait::Expression & expr) const;
    String selectCHFunctionName(const substrait::Expression_ScalarFunction & substrait_func) const;
};
}

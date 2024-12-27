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
#include <Parser/AggregateFunctionParser.h>

namespace local_engine
{
class NtileParser : public AggregateFunctionParser
{
public:
    explicit NtileParser(ParserContextPtr parser_context_) : AggregateFunctionParser(parser_context_) { }
    ~NtileParser() override = default;
    static constexpr auto name = "ntile";
    String getName() const override { return name; }
    String getCHFunctionName(const CommonFunctionInfo &) const override { return "ntile"; }
    String getCHFunctionName(DB::DataTypes &) const override { return "ntile"; }
    DB::ActionsDAG::NodeRawConstPtrs parseFunctionArguments(
        const CommonFunctionInfo & func_info, DB::ActionsDAG & actions_dag) const override;
};
}

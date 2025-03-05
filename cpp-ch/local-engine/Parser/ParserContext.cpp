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
#include "ParserContext.h"
#include <string>
#include <unordered_map>
#include <Functions/SparkFunctionGetJsonObject.h>
#include <Rewriter/ExpressionRewriter.h>
#include <Common/Exception.h>
namespace DB::ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace local_engine
{

void ParserContext::addSelfDefinedFunctionMapping()
{
    FunctionSignature sig;
    sig.function_name = std::string(FlattenJSONStringOnRequiredFunction::name);
    sig.signature = sig.function_name + ":";
    if (function_mapping.count(SelfDefinedFunctionReference::GET_JSON_OBJECT))
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Confict function rerefence");
    function_mapping[SelfDefinedFunctionReference::GET_JSON_OBJECT] = sig;
    legacy_function_mapping[std::to_string(SelfDefinedFunctionReference::GET_JSON_OBJECT)] = sig.signature;
}

ParserContextPtr
ParserContext::build(DB::ContextPtr context_, const std::unordered_map<String, String> & function_mapping_, const size_t & partition_index_)
{
    auto res = std::make_shared<ParserContext>();
    res->query_context = context_;
    res->legacy_function_mapping = function_mapping_;
    res->partition_index = partition_index_;
    auto & function_mapping = res->function_mapping;
    for (const auto & fm : function_mapping_)
    {
        const auto & signature = fm.second;
        Int32 ref = std::stoi(fm.first);
        FunctionSignature function_sig;
        function_sig.signature = signature;
        function_sig.function_name = fm.second.substr(0, fm.second.find(':'));
        function_mapping[ref] = function_sig;
    }
    res->addSelfDefinedFunctionMapping();
    return res;
}

ParserContextPtr ParserContext::build(DB::ContextPtr context_)
{
    auto res = std::make_shared<ParserContext>();
    res->query_context = context_;
    res->addSelfDefinedFunctionMapping();
    return res;
}

ParserContextPtr ParserContext::build(DB::ContextPtr context_, const substrait::Plan & plan_, const size_t & partition_index_)
{
    std::unordered_map<String, String> function_mapping;
    for (const auto & extension : plan_.extensions())
    {
        if (extension.has_extension_function())
        {
            function_mapping.emplace(
                std::to_string(extension.extension_function().function_anchor()), extension.extension_function().name());
        }
    }
    return build(context_, function_mapping, partition_index_);
}

DB::ContextPtr ParserContext::queryContext() const
{
    return query_context;
}

std::optional<String> ParserContext::getFunctionSignature(Int32 ref) const
{
    auto it = function_mapping.find(ref);
    if (it == function_mapping.end())
        return {};
    return it->second.signature;
}

std::optional<String> ParserContext::getFunctionSignature(const substrait::Expression_ScalarFunction & func) const
{
    return getFunctionSignature(func.function_reference());
}

std::optional<String> ParserContext::getFunctionNameInSignature(Int32 ref) const
{
    auto it = function_mapping.find(ref);
    if (it == function_mapping.end())
        return {};
    return it->second.function_name;
}

std::optional<String> ParserContext::getFunctionNameInSignature(const substrait::Expression_ScalarFunction & func) const
{
    return getFunctionNameInSignature(func.function_reference());
}

const std::unordered_map<String, String> & ParserContext::getLegacyFunctionMapping() const
{
    return legacy_function_mapping;
}
}

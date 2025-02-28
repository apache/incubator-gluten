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

#include <optional>
#include <unordered_map>
#include <Interpreters/Context_fwd.h>
#include <substrait/plan.pb.h>

namespace local_engine
{

struct FunctionSignature
{
    String signature;
    String function_name;
    static FunctionSignature parse(const String & signature);
};
// ParserContext is shared among all parsers.
class ParserContext
{
public:
    ParserContext() = default;

    using Ptr = std::shared_ptr<const ParserContext>;
    static Ptr
    build(DB::ContextPtr context_, const std::unordered_map<String, String> & function_mapping_, const size_t & partition_index_ = 0);
    static Ptr build(DB::ContextPtr context_);
    static Ptr build(DB::ContextPtr context_, const substrait::Plan & plan_, const size_t & partition_index_ = 0);

    DB::ContextPtr queryContext() const;

    std::optional<String> getFunctionSignature(Int32 ref) const;
    std::optional<String> getFunctionSignature(const substrait::Expression_ScalarFunction & func) const;
    std::optional<String> getFunctionNameInSignature(Int32 ref) const;
    std::optional<String> getFunctionNameInSignature(const substrait::Expression_ScalarFunction & func) const;
    const std::unordered_map<String, String> & getLegacyFunctionMapping() const;
    size_t getPartitionIndex() const { return partition_index; }

private:
    DB::ContextPtr query_context;
    std::unordered_map<String, String> legacy_function_mapping;
    std::unordered_map<Int32, FunctionSignature> function_mapping;
    size_t partition_index = 0;

    void addSelfDefinedFunctionMapping();
};

using ParserContextPtr = ParserContext::Ptr;
}

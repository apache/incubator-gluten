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

#include <Parser/RelParsers/RelParser.h>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}
}

namespace local_engine
{
class StreamKafkaRelParser : public RelParser
{
public:
    explicit StreamKafkaRelParser(ParserContextPtr parser_context_, const DB::ContextPtr & context_)
        : RelParser(parser_context_), context(context_)
    {
    }

    ~StreamKafkaRelParser() override = default;

    DB::QueryPlanPtr
    parse(DB::QueryPlanPtr query_plan, const substrait::Rel & rel, std::list<const substrait::Rel *> & rel_stack_) override;

    std::optional<const substrait::Rel *> getSingleInput(const substrait::Rel &) override
    {
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "StreamKafkaRelParser can't call getSingleInput().");
    }

    void setSplitInfo(String split_info_) { split_info = split_info_; }

private:
    DB::QueryPlanPtr parseRelImpl(DB::QueryPlanPtr query_plan, const substrait::ReadRel & read_rel);

    DB::ContextPtr context;

    String split_info;
};
}

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
#include <Core/Block.h>
#include <Core/SortDescription.h>
#include <Parser/RelParser.h>
#include <google/protobuf/repeated_field.h>
namespace local_engine
{
class SortRelParser : public RelParser
{
public:
    explicit SortRelParser(SerializedPlanParser * plan_paser_);
    ~SortRelParser() override = default;

    DB::QueryPlanPtr
    parse(DB::QueryPlanPtr query_plan, const substrait::Rel & sort_rel, std::list<const substrait::Rel *> & rel_stack_) override;
    static DB::SortDescription
    parseSortDescription(const google::protobuf::RepeatedPtrField<substrait::SortField> & sort_fields, const DB::Block & header);

    const substrait::Rel & getSingleInput(const substrait::Rel & rel) override { return rel.sort().input(); }

private:
    size_t parseLimit(std::list<const substrait::Rel *> & rel_stack_);
};
}

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

#include <memory>
#include <optional>
#include <Parser/RelParsers/RelParser.h>
#include <Processors/QueryPlan/LimitStep.h>

namespace local_engine
{
class FetchRelParser : public RelParser
{
public:
    explicit FetchRelParser(SerializedPlanParser * plan_parser_) : RelParser(plan_parser_) { }
    ~FetchRelParser() override = default;

    DB::QueryPlanPtr parse(DB::QueryPlanPtr query_plan, const substrait::Rel & rel, std::list<const substrait::Rel *> &)
    {
        const auto & limit = rel.fetch();
        auto limit_step = std::make_unique<DB::LimitStep>(query_plan->getCurrentDataStream(), limit.count(), limit.offset());
        limit_step->setStepDescription("LIMIT");
        steps.push_back(limit_step.get());
        query_plan->addStep(std::move(limit_step));
        return query_plan;
    }
    std::optional<const substrait::Rel *> getSingleInput(const substrait::Rel & rel) override { return &rel.fetch().input(); }
};

void registerFetchRelParser(RelParserFactory & factory)
{
    auto builder = [](SerializedPlanParser * plan_parser_) { return std::make_unique<FetchRelParser>(plan_parser_); };
    factory.registerBuilder(substrait::Rel::RelTypeCase::kFetch, builder);
}

}

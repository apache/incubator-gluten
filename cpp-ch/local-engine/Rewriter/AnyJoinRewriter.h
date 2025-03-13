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
#include <Rewriter/RelRewriter.h>
#include "substrait/algebra.pb.h"

namespace local_engine
{
// Use any join to replace following query
//  select t1.* from t1 join (select id from t2 group by id) t2 on t1.id = t2.id;
// The right table join keys are the same as the grouping keys, and there is no any aggregation
// function.
class AnyJoinRewriter : public RelRewriter
{
public:
    AnyJoinRewriter(ParserContextPtr parser_context_)
        : RelRewriter(parser_context_)
    {
    }
    ~AnyJoinRewriter() override = default;

    void rewrite(substrait::Rel &) override {}

protected:
    bool visitJoin(substrait::Rel & join_rel) override;

private:
    static bool isDeduplicationAggregate(const substrait::AggregateRel & aggregate_rel);

    bool collectOnJoinEqualConditions(const substrait::Expression & e, std::vector<const substrait::Expression *> & equal_expressions);
};
}

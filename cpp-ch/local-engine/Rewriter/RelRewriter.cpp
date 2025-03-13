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
#include <Rewriter/RelRewriter.h>
#include <Rewriter/AnyJoinRewriter.h>
#include <substrait/algebra.pb.h>
#include <Common/logger_useful.h>

namespace local_engine
{
bool RelRewriter::visitRel(substrait::Rel & rel)
{
    LOG_ERROR(getLogger("RelRewriter"), "visitRel: {}", rel.rel_type_case());
    switch (rel.rel_type_case())
    {
        case substrait::Rel::RelTypeCase::kRead:
            return visitRead(rel);
        case substrait::Rel::RelTypeCase::kFilter:
            return visitFilter(rel);
        case substrait::Rel::RelTypeCase::kFetch:
            return visitFetch(rel);
        case substrait::Rel::RelTypeCase::kAggregate:
            return visitAggregate(rel);
        case substrait::Rel::RelTypeCase::kSort:
            return visitSort(rel);
        case substrait::Rel::RelTypeCase::kJoin:
            return visitJoin(rel);
        case substrait::Rel::RelTypeCase::kProject:
            return visitProject(rel);
        case substrait::Rel::RelTypeCase::kSet:
            return visitSet(rel);
        case substrait::Rel::RelTypeCase::kCross:
            return visitCross(rel);
        case substrait::Rel::RelTypeCase::kExpand:
            return visitExpand(rel);
        case substrait::Rel::RelTypeCase::kWindow:
            return visitWindow(rel);
        case substrait::Rel::RelTypeCase::kGenerate:
            return visitGenerate(rel);
        case substrait::Rel::RelTypeCase::kWrite:
            return visitWrite(rel);
        case substrait::Rel::RelTypeCase::kTopN:
            return visitTopN(rel);
        case substrait::Rel::RelTypeCase::kWindowGroupLimit:
            return visitWindowGroupLimit(rel);
        default: {
            LOG_WARNING(getLogger("RelRewriter"), "unknown rel type: {}\n{}", rel.rel_type_case(), rel.DebugString());
            break;
        }
    }
    return false;
}

RelRewriterFactory & RelRewriterFactory::instance()
{
    static RelRewriterFactory factory;
    return factory;
}

RelRewriterFactory::RelRewriterFactory()
{
    rewriters.push_back([](ParserContextPtr parser_context_) { return std::make_unique<AnyJoinRewriter>(parser_context_); });
}

bool RelRewriterFactory::apply(ParserContextPtr parser_context, substrait::Rel & rel)
{
    bool has_changed = false;
    for (auto & rewriter : rewriters)
    {
        has_changed |= rewriter(parser_context)->visitRel(rel);
    }
    return has_changed;
}
}

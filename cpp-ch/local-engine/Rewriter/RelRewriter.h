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
#include <unordered_map>
#include <Functions/SparkFunctionGetJsonObject.h>
#include <Interpreters/Context_fwd.h>
#include <Parser/ParserContext.h>
#include <Parser/SerializedPlanParser.h>
#include <boost/core/noncopyable.hpp>
#include <substrait/algebra.pb.h>


namespace local_engine
{
class RelRewriter
{
public:
    RelRewriter(ParserContextPtr parser_context_)
        : parser_context(parser_context_)
    {
    }
    virtual ~RelRewriter() = default;
    virtual void rewrite(substrait::Rel & rel) = 0;

    // visit the substraint plan recursively
    bool visitRel(substrait::Rel & rel);

protected:
    ParserContextPtr parser_context;

    inline DB::ContextPtr getContext() const { return parser_context->queryContext(); }


    virtual bool visitRead(substrait::Rel & /*scan_rel*/) { return false; }

    virtual bool visitFilter(substrait::Rel & filter_rel) { return visitRel(*(filter_rel.mutable_filter()->mutable_input())); }

    virtual bool visitFetch(substrait::Rel & fetch_rel) { return visitRel(*(fetch_rel.mutable_fetch()->mutable_input())); }

    virtual bool visitAggregate(substrait::Rel & aggregate_rel) { return visitRel(*(aggregate_rel.mutable_aggregate()->mutable_input())); }

    virtual bool visitSort(substrait::Rel & sort_rel) { return visitRel(*(sort_rel.mutable_sort()->mutable_input())); }

    virtual bool visitJoin(substrait::Rel & join_rel)
    {
        bool left_changed = visitRel(*(join_rel.mutable_join()->mutable_left()));
        bool right_changed = visitRel(*(join_rel.mutable_join()->mutable_right()));
        return left_changed || right_changed;
    }

    virtual bool visitProject(substrait::Rel & project_rel) { return visitRel(*(project_rel.mutable_project()->mutable_input())); }

    virtual bool visitSet(substrait::Rel & set_rel)
    {
        bool has_changed = false;
        for (auto & input : (*set_rel.mutable_set()->mutable_inputs()))
        {
            has_changed |= visitRel(input);
        }
        return has_changed;
    }

    virtual bool visitCross(substrait::Rel & cross_rel)
    {
        bool left_changed = visitRel(*(cross_rel.mutable_join()->mutable_left()));
        bool right_changed = visitRel(*(cross_rel.mutable_join()->mutable_right()));
        return left_changed || right_changed;
    }

    virtual bool visitExpand(substrait::Rel & expand_rel) { return visitRel(*(expand_rel.mutable_expand()->mutable_input())); }

    virtual bool visitWindow(substrait::Rel & windows_rel) { return visitRel(*(windows_rel.mutable_expand()->mutable_input())); }

    virtual bool visitGenerate(substrait::Rel & generate_rel) { return visitRel(*(generate_rel.mutable_expand()->mutable_input())); }

    virtual bool visitWrite(substrait::Rel & write_rel) { return visitRel(*(write_rel.mutable_expand()->mutable_input())); }

    virtual bool visitTopN(substrait::Rel & top_n_rel) { return visitRel(*(top_n_rel.mutable_top_n()->mutable_input())); }

    virtual bool visitWindowGroupLimit(substrait::Rel & window_group_limit_rel)
    {
        return visitRel(*(window_group_limit_rel.mutable_windowgrouplimit()->mutable_input()));
    }
};

class RelRewriterFactory : public boost::noncopyable
{
public:
    static RelRewriterFactory & instance();
    bool apply(ParserContextPtr parser_context, substrait::Rel & rel);

protected:
    RelRewriterFactory();
    using RelRewriterCreator = std::function<std::unique_ptr<RelRewriter>(ParserContextPtr)>;
    std::vector<RelRewriterCreator> rewriters;
};

}

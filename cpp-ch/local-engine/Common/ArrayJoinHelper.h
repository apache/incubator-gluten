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
#include <Interpreters/ActionsDAG.h>

namespace DB
{
class IQueryPlanStep;
class QueryPlan;
}

namespace local_engine
{
namespace ArrayJoinHelper
{
// apply array join on one column to flatten the array column
DB::ActionsDAG applyArrayJoinOnOneColumn(const DB::Block & header, size_t column_index);

const DB::ActionsDAG::Node * findArrayJoinNode(const DB::ActionsDAG & actions_dag);

// actions_dag is a actions dag that contains the array join node, if not, the plan will not be changed.
// return the steps that are added to the plan.
std::vector<DB::IQueryPlanStep *> addArrayJoinStep(DB::ContextPtr context, DB::QueryPlan & plan, const DB::ActionsDAG & actions_dag, bool is_left);
} // namespace ArrayJoinHelper
}
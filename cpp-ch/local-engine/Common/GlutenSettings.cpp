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
#include "GlutenSettings.h"

#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Common/GlutenConfig.h>

using namespace DB;
namespace local_engine
{

bool tryGetString(const DB::Settings & settings, std::string_view name, std::string & value)
{
    if (Field field; settings.tryGet(name, field))
    {
        value = field.safeGet<String>();
        return true;
    }
    return false;
}
bool settingsEqual(const DB::Settings & settings, std::string_view name, const std::string & value)
{
    if (DB::Field field; settings.tryGet(name, field))
        return field.safeGet<String>() == value;
    return false;
}
void updateSettings(const DB::ContextMutablePtr & context, std::string_view plan)
{
    SparkConfigs::update(
        plan,
        [&](const SparkConfigs::ConfigMap & config_map)
        {
            for (const auto & [key, value] : config_map)
                context->setSetting(key, value);
        });
}

/// Even query_plan_enable_optimizations is set to false, only first pass optimizations are disabled.
/// Let's restore to the behavior before https://github.com/ClickHouse/ClickHouse/pull/73693
void setGlutenOptimizationSettings(DB::QueryPlanOptimizationSettings & optimization_settings)
{
    /// If not zero, throw if too many optimizations were applied to query plan.
    /// It helps to avoid infinite optimization loop.
    optimization_settings.max_optimizations_to_apply = 0;

    /// ------------------------------------------------------
    /// Enable/disable plan-level optimizations

    /// --- Zero-pass optimizations (Processors/QueryPlan/QueryPlan.cpp)
    optimization_settings.remove_redundant_sorting = true;

    /// --- First-pass optimizations
    optimization_settings.lift_up_array_join = true;
    optimization_settings.push_down_limit = true;
    optimization_settings.split_filter = true;
    optimization_settings.merge_expressions = true;
    optimization_settings.merge_filters = true;
    optimization_settings.filter_push_down = true;
    optimization_settings.convert_outer_join_to_inner_join = true;
    optimization_settings.execute_functions_after_sorting = true;
    optimization_settings.reuse_storage_ordering_for_window_functions = true;
    optimization_settings.lift_up_union = true;
    optimization_settings.aggregate_partitions_independently = false;
    optimization_settings.remove_redundant_distinct = true;
    optimization_settings.try_use_vector_search = true;

    /// --- Second-pass optimizations
    optimization_settings.optimize_prewhere = true;
    optimization_settings.read_in_order = true;
    optimization_settings.distinct_in_order = false;
    optimization_settings.optimize_sorting_by_input_stream_properties = true;
    optimization_settings.aggregation_in_order = false;
    optimization_settings.optimize_projection = false;

    /// --- Third-pass optimizations (Processors/QueryPlan/QueryPlan.cpp)
    optimization_settings.build_sets = true;

    /// ------------------------------------------------------

    /// Other settings related to plan-level optimizations
    optimization_settings.optimize_use_implicit_projections = false;
    optimization_settings.force_use_projection = false;
    optimization_settings.max_limit_for_ann_queries = 1'000'000;
}
}
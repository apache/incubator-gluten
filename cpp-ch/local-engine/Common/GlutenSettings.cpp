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
bool settingsEqual(
    const DB::Settings & settings, std::string_view name, const std::string & value, const std::optional<std::string> & default_value)
{
    if (DB::Field field; settings.tryGet(name, field))
        return field.safeGet<String>() == value;
    if (default_value.has_value())
        return *default_value == value;
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
}
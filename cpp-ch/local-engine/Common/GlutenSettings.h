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

#include <Interpreters/Context_fwd.h>

namespace DB
{
struct Settings;
}
namespace local_engine
{

#define SKIP_ALIAS(ALIAS_NAME)

#define DECLARE_GLUTEN_SETTING_STATIC_MEMBER_(TYPE, NAME, DEFAULT, DESCRIPTION) TYPE NAME{DEFAULT};

#define GLUTEN_SETTING_STATIC_MEMBER_(NAME) s_##NAME##_

#define INITIALIZE_GLUTEN_SETTING_STATIC_MEMBER_(TYPE, NAME, DEFAULT, DESCRIPTION) \
    static constexpr std::string_view GLUTEN_SETTING_STATIC_MEMBER_(NAME) = "gluten." #NAME;

#define DECLARE_GLUTEN_SETTINGS(SETTINGS_CLASS_NAME, LIST_OF_SETTINGS_MACRO) \
    struct SETTINGS_CLASS_NAME \
    { \
        LIST_OF_SETTINGS_MACRO(DECLARE_GLUTEN_SETTING_STATIC_MEMBER_, SKIP_ALIAS) \
        static SETTINGS_CLASS_NAME get(const DB::ContextPtr & context); \
        void set(const DB::ContextMutablePtr & context) const; \
\
    private: \
        LIST_OF_SETTINGS_MACRO(INITIALIZE_GLUTEN_SETTING_STATIC_MEMBER_, SKIP_ALIAS) \
    };

#define IMPLEMENT_GLUTEN_GET_(TYPE, NAME, DEFAULT, DESCRIPTION) \
    if (DB::Field field_##NAME; settings.tryGet(GLUTEN_SETTING_STATIC_MEMBER_(NAME), field_##NAME)) \
        result.NAME = field_##NAME.safeGet<TYPE>();

#define IMPLEMENT_GLUTEN_SET_(TYPE, NAME, DEFAULT, DESCRIPTION) context->setSetting(GLUTEN_SETTING_STATIC_MEMBER_(NAME), NAME);

#define IMPLEMENT_GLUTEN_SETTINGS(SETTINGS_CLASS_NAME, LIST_OF_SETTINGS_MACRO) \
    SETTINGS_CLASS_NAME SETTINGS_CLASS_NAME::get(const DB::ContextPtr & context) \
    { \
        SETTINGS_CLASS_NAME result; \
        const DB::Settings & settings = context->getSettingsRef(); \
        LIST_OF_SETTINGS_MACRO(IMPLEMENT_GLUTEN_GET_, SKIP_ALIAS) \
        return result; \
    } \
    void SETTINGS_CLASS_NAME::SETTINGS_CLASS_NAME::set(const DB::ContextMutablePtr & context) const \
    { \
        LIST_OF_SETTINGS_MACRO(IMPLEMENT_GLUTEN_SET_, SKIP_ALIAS) \
    }

// workaround for tryGetString

bool tryGetString(const DB::Settings & settings, std::string_view name, std::string & value);
bool settingsEqual(
    const DB::Settings & settings,
    std::string_view name,
    const std::string & value,
    const std::optional<std::string> & default_value = std::nullopt);
void updateSettings(const DB::ContextMutablePtr &, std::string_view);

namespace RuntimeSettings
{
inline constexpr auto COLLECT_METRICS = "collect_metrics";
inline constexpr auto COLLECT_METRICS_DEFAULT = "true";
}
} // namespace local_engine

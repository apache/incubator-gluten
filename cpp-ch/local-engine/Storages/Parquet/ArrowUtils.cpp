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
#include "ArrowUtils.h"

namespace local_engine
{
parquet::internal::LevelInfo computeLevelInfo(const parquet::ColumnDescriptor * descr)
{
    parquet::internal::LevelInfo level_info;
    level_info.def_level = descr->max_definition_level();
    level_info.rep_level = descr->max_repetition_level();

    int16_t min_spaced_def_level = descr->max_definition_level();
    const ::parquet::schema::Node * node = descr->schema_node().get();
    while (node != nullptr && !node->is_repeated())
    {
        if (node->is_optional())
            min_spaced_def_level--;
        node = node->parent();
    }
    level_info.repeated_ancestor_def_level = min_spaced_def_level;
    return level_info;
}
}

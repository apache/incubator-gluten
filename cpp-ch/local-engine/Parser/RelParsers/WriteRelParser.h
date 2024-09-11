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

#include <map>
#include <string>
#include <Core/Block.h>
#include <Core/Names.h>
#include <Interpreters/Context_fwd.h>
#include <Common/GlutenSettings.h>

namespace substrait
{
class WriteRel;
class NamedStruct;
}

namespace DB
{
class QueryPipelineBuilder;
using QueryPipelineBuilderPtr = std::unique_ptr<QueryPipelineBuilder>;
}

namespace local_engine
{

void addSinkTransform(const DB::ContextPtr & context, const substrait::WriteRel & write_rel, const DB::QueryPipelineBuilderPtr & builder);

/// Visible for UTs
std::map<std::string, std::string> parse_write_parameter(const std::string & input);
DB::Names collect_partition_cols(const DB::Block & header, const substrait::NamedStruct & struct_);

#define WRITE_RELATED_SETTINGS(M, ALIAS, UNIQ) \
    M(String, task_write_tmp_dir, , "The temporary directory for writing data", UNIQ) \
    M(String, task_write_filename, , "The filename for writing data", UNIQ)

DECLARE_GLUTEN_SETTINGS(GlutenWriteSettings, WRITE_RELATED_SETTINGS)

}

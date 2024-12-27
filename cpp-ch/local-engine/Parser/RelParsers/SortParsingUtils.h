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
#include <Core/Block.h>
#include <Core/SortDescription.h>
#include <google/protobuf/repeated_field.h>
#include <substrait/plan.pb.h>

namespace local_engine
{
// convert expressions into sort description
DB::SortDescription
parseSortFields(const DB::Block & header, const google::protobuf::RepeatedPtrField<substrait::Expression> & expressions);
DB::SortDescription parseSortFields(const DB::Block & header, const google::protobuf::RepeatedPtrField<substrait::SortField> & sort_fields);

std::string
buildSQLLikeSortDescription(const DB::Block & header, const google::protobuf::RepeatedPtrField<substrait::SortField> & sort_fields);
}

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
#include "SortParsingUtils.h"
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <Poco/Logger.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Parser/SubstraitParserUtils.h>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int LOGICAL_ERROR;
}

namespace local_engine
{
DB::SortDescription parseSortFields(const DB::Block & header, const google::protobuf::RepeatedPtrField<substrait::Expression> & expressions)
{
    DB::SortDescription description;
    for (const auto & expr : expressions)
    {
        auto field_index = SubstraitParserUtils::getStructFieldIndex(expr);
        if (field_index)
        {
            const auto & col_name = header.getByPosition(*field_index).name;
            description.push_back(DB::SortColumnDescription(col_name, 1, -1));
        }
        else if (expr.has_literal())
            continue;
        else
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Unknow expression as sort field: {}", expr.DebugString());
    }
    return description;
}

DB::SortDescription parseSortFields(const DB::Block & header, const google::protobuf::RepeatedPtrField<substrait::SortField> & sort_fields)
{
    static std::map<int, std::pair<int, int>> direction_map = {{1, {1, -1}}, {2, {1, 1}}, {3, {-1, 1}}, {4, {-1, -1}}};

    DB::SortDescription sort_descr;
    for (int i = 0, sz = sort_fields.size(); i < sz; ++i)
    {
        const auto & sort_field = sort_fields[i];
        /// There is no meaning to sort a const column.
        if (sort_field.expr().has_literal())
            continue;

        auto field_index = SubstraitParserUtils::getStructFieldIndex(sort_field.expr());
        if(!field_index)
        {
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Unsupport sort field");
        }

        auto direction_iter = direction_map.find(sort_field.direction());
        if (direction_iter == direction_map.end())
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Unsuppor sort direction: {}", sort_field.direction());
        const auto & col_name = header.getByPosition(*field_index).name;
        sort_descr.emplace_back(col_name, direction_iter->second.first, direction_iter->second.second);
    }
    return sort_descr;
}

std::string buildSQLLikeSortDescription(const DB::SortDescription & sort_description)
{
    DB::WriteBufferFromOwnString ostr;
    size_t n = 0;
    for (const auto & sort_column : sort_description)
    {
        if (n)
            ostr << String(", ");
        const auto & col_name = sort_column.column_name;
        ostr << "`" << col_name << "` " << (sort_column.direction == 1 ? "ASC" : "DESC") << " NULLS " << (sort_column.nulls_direction != sort_column.direction ? "FIRST" : "LAST");
        n += 1;
    }
    return ostr.str();
}
}

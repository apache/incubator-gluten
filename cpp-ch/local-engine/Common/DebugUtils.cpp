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
#include "DebugUtils.h"
#include <iostream>
#include <sstream>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <Formats/FormatSettings.h>
#include <Functions/FunctionHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <google/protobuf/json/json.h>
#include <google/protobuf/util/json_util.h>
#include <google/protobuf/wrappers.pb.h>
#include <Common/CHUtil.h>
#include <Common/QueryContext.h>
#include <Common/logger_useful.h>

namespace pb_util = google::protobuf::util;

namespace debug
{

void dumpPlan(DB::QueryPlan & plan, const char * type, bool force, LoggerPtr logger)
{
    if (!logger)
    {
        logger = getLogger("SerializedPlanParser");
        if (!logger)
            return;
    }

    if (!force && !logger->debug())
        return;

    auto out = local_engine::PlanUtil::explainPlan(plan);
    auto task_id = local_engine::QueryContext::instance().currentTaskIdOrEmpty();
    task_id = task_id.empty() ? "" : "(" + task_id + ")";
    if (force) // force
        LOG_ERROR(logger, "{}{} =>\n{}", type, task_id, out);
    else
        LOG_DEBUG(logger, "{}{} =>\n{}", type, task_id, out);
}

void dumpMessage(const google::protobuf::Message & message, const char * type, bool force, LoggerPtr logger)
{
    if (!logger)
    {
        logger = getLogger("SubstraitPlan");
        if (!logger)
            return;
    }

    if (!force && !logger->debug())
        return;
    pb_util::JsonOptions options;
    std::string json;
    if (auto s = MessageToJsonString(message, &json, options); !s.ok())
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Can not convert {} to Json", type);

    auto task_id = local_engine::QueryContext::instance().currentTaskIdOrEmpty();
    task_id = task_id.empty() ? "" : "(" + task_id + ")";
    if (force) // force
        LOG_ERROR(logger, "{}{} =>\n{}", type, task_id, json);
    else
        LOG_DEBUG(logger, "{}{} =>\n{}", type, task_id, json);
}

void headBlock(const DB::Block & block, size_t count)
{
    std::cout << "============Block============" << std::endl;
    std::cout << block.dumpStructure() << std::endl;
    // print header
    for (const auto & name : block.getNames())
        std::cout << name << "\t";
    std::cout << std::endl;

    // print rows
    for (size_t row = 0; row < std::min(count, block.rows()); ++row)
    {
        for (size_t column = 0; column < block.columns(); ++column)
        {
            const auto type = block.getByPosition(column).type;
            auto col = block.getByPosition(column).column;

            if (column > 0)
                std::cout << "\t";
            DB::WhichDataType which(type);
            if (which.isAggregateFunction())
                std::cout << "Nan";
            else if (col->isNullAt(row))
                std::cout << "null";
            else
                std::cout << toString((*col)[row]);
        }
        std::cout << std::endl;
    }
}

String printBlock(const DB::Block & block, size_t count)
{
    std::ostringstream ss;
    ss << std::string("============Block============\n");
    ss << block.dumpStructure() << String("\n");
    // print header
    for (const auto & name : block.getNames())
        ss << name << std::string("\t");
    ss << std::string("\n");

    // print rows
    for (size_t row = 0; row < std::min(count, block.rows()); ++row)
    {
        for (size_t column = 0; column < block.columns(); ++column)
        {
            const auto type = block.getByPosition(column).type;
            auto col = block.getByPosition(column).column;

            if (column > 0)
                ss << std::string("\t");
            DB::WhichDataType which(type);
            if (which.isAggregateFunction())
                ss << std::string("Nan");
            else if (col->isNullAt(row))
                ss << std::string("null");
            else
                ss << toString((*col)[row]);
        }
        ss << std::string("\n");
    }
    return ss.str();
}


void headColumn(const DB::ColumnPtr & column, size_t count)
{
    std::cout << "============Column============" << std::endl;

    // print header
    std::cout << column->getName() << "\t";
    std::cout << std::endl;

    // print rows
    for (size_t row = 0; row < std::min(count, column->size()); ++row)
        std::cout << toString((*column)[row]) << std::endl;
}

}

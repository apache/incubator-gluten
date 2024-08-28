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


#include "SubstraitParserUtils.h"
#include <Common/Exception.h>
#include <Common/logger_useful.h>

using namespace DB;

namespace local_engine
{
void logDebugMessage(const google::protobuf::Message & message, const char * type)
{
    auto * logger = &Poco::Logger::get("SubstraitPlan");
    if (logger->debug())
    {
        namespace pb_util = google::protobuf::util;
        pb_util::JsonOptions options;
        std::string json;
        auto s = pb_util::MessageToJsonString(message, &json, options);
        if (!s.ok())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Can not convert {} to Json", type);
        LOG_DEBUG(logger, "{}:\n{}", type, json);
    }
}
}
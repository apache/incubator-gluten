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
#include <boost/algorithm/string.hpp>
#include <Poco/StringTokenizer.h>
#include <Poco/URI.h>

#include "GlutenStringUtils.h"

namespace local_engine
{
PartitionValues GlutenStringUtils::parsePartitionTablePath(const std::string & file)
{
    PartitionValues result;
    Poco::StringTokenizer path(file, "/");
    for (const auto & item : path)
    {
        auto pos = item.find('=');
        if (pos != std::string::npos)
        {
            auto key = boost::to_lower_copy(item.substr(0, pos));
            auto value = item.substr(pos + 1);

            std::string unescaped_key;
            std::string unescaped_value;
            Poco::URI::decode(key, unescaped_key);
            Poco::URI::decode(value, unescaped_value);
            result.emplace_back(std::move(unescaped_key), std::move(unescaped_value));
        }
    }
    return result;
}

bool GlutenStringUtils::isNullPartitionValue(const std::string & value)
{
    return value == "__HIVE_DEFAULT_PARTITION__";
}
}

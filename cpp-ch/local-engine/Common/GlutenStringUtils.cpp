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

bool GlutenStringUtils::isNullPartitionValue(const std::string & value)
{
    return value == "__HIVE_DEFAULT_PARTITION__";
}

std::string GlutenStringUtils::dumpPartitionValues(const std::map<std::string, std::string> & values)
{
    std::string res;
    res += "[";

    for (const auto & [key, value] : values)
    {
        res += key + "=" + value + ", ";
    }

    res += "]";
    return res;
}

}

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

#include <AggregateFunctions/AggregateFunctionFactory.h>

#include "AggregateFunctionDVRoaringBitmap.h"

#include <AggregateFunctions/FactoryHelpers.h>

namespace DB
{
struct Settings;

namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int BAD_ARGUMENTS;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}
}

namespace local_engine
{

DB::AggregateFunctionPtr createAggregateFunctionDVRoaringBitmap(
    const std::string & name, const DB::DataTypes & argument_types, const DB::Array & parameters, const DB::Settings *)
{
    return DB::AggregateFunctionPtr(
        new AggregateFunctionDVRoaringBitmap<Int64, AggregateFunctionDVRoaringBitmapData>(argument_types, parameters));
}

void registerAggregateFunctionDVRoaringBitmap(DB::AggregateFunctionFactory & factory)
{
    factory.registerFunction("bitmapaggregator", createAggregateFunctionDVRoaringBitmap);
}
}

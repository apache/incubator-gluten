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

#include <base/types.h>
#include <Functions/SparkFunctionCastFloatToInt.h>

using namespace DB;

namespace local_engine
{

struct NameToUInt8 { static constexpr auto name = "sparkCastFloatToUInt8"; };
struct NameToUInt16 { static constexpr auto name = "sparkCastFloatToUInt16"; };
struct NameToUInt32 { static constexpr auto name = "sparkCastFloatToUInt32"; };
struct NameToUInt64 { static constexpr auto name = "sparkCastFloatToUInt64"; };
struct NameToUInt128 { static constexpr auto name = "sparkCastFloatToUInt128"; };
struct NameToUInt256 { static constexpr auto name = "sparkCastFloatToUInt256"; };
struct NameToInt8 { static constexpr auto name = "sparkCastFloatToInt8"; };
struct NameToInt16 { static constexpr auto name = "sparkCastFloatToInt16"; };
struct NameToInt32 { static constexpr auto name = "sparkCastFloatToInt32"; };
struct NameToInt64 { static constexpr auto name = "sparkCastFloatToInt64"; };
struct NameToInt128 { static constexpr auto name = "sparkCastFloatToInt128"; };
struct NameToInt256 { static constexpr auto name = "sparkCastFloatToInt256"; };

using SparkFunctionCastFloatToInt8 = local_engine::SparkFunctionCastFloatToInt<Int8, NameToInt8>;
using SparkFunctionCastFloatToInt16 = local_engine::SparkFunctionCastFloatToInt<Int16, NameToInt16>;
using SparkFunctionCastFloatToInt32 = local_engine::SparkFunctionCastFloatToInt<Int32, NameToInt32>;
using SparkFunctionCastFloatToInt64 = local_engine::SparkFunctionCastFloatToInt<Int64, NameToInt64>;
using SparkFunctionCastFloatToInt128 = local_engine::SparkFunctionCastFloatToInt<Int128, NameToInt128>;
using SparkFunctionCastFloatToInt256 = local_engine::SparkFunctionCastFloatToInt<Int256, NameToInt256>;
using SparkFunctionCastFloatToUInt8 = local_engine::SparkFunctionCastFloatToInt<UInt8, NameToUInt8>;
using SparkFunctionCastFloatToUInt16 = local_engine::SparkFunctionCastFloatToInt<UInt16, NameToUInt16>;
using SparkFunctionCastFloatToUInt32 = local_engine::SparkFunctionCastFloatToInt<UInt32, NameToUInt32>;
using SparkFunctionCastFloatToUInt64 = local_engine::SparkFunctionCastFloatToInt<UInt64, NameToUInt64>;
using SparkFunctionCastFloatToUInt128 = local_engine::SparkFunctionCastFloatToInt<UInt128, NameToUInt128>;
using SparkFunctionCastFloatToUInt256 = local_engine::SparkFunctionCastFloatToInt<UInt256, NameToUInt256>;

REGISTER_FUNCTION(SparkFunctionCastToInt)
{
    factory.registerFunction<SparkFunctionCastFloatToInt8>();
    factory.registerFunction<SparkFunctionCastFloatToInt16>();
    factory.registerFunction<SparkFunctionCastFloatToInt32>();
    factory.registerFunction<SparkFunctionCastFloatToInt64>();
    factory.registerFunction<SparkFunctionCastFloatToInt128>();
    factory.registerFunction<SparkFunctionCastFloatToInt256>();
    factory.registerFunction<SparkFunctionCastFloatToUInt8>();
    factory.registerFunction<SparkFunctionCastFloatToUInt16>();
    factory.registerFunction<SparkFunctionCastFloatToUInt32>();
    factory.registerFunction<SparkFunctionCastFloatToUInt64>();
    factory.registerFunction<SparkFunctionCastFloatToUInt128>();
    factory.registerFunction<SparkFunctionCastFloatToUInt256>();
}
}

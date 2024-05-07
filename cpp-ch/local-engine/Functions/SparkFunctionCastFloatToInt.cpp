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

#include <limits.h>
#include <base/types.h>
#include <base/wide_integer.h>
#include <base/wide_integer_impl.h>
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

using SparkFunctionCastFloatToInt8 = local_engine::SparkFunctionCastFloatToInt<Int8, NameToInt8, INT8_MAX, INT8_MIN>;
using SparkFunctionCastFloatToInt16 = local_engine::SparkFunctionCastFloatToInt<Int16, NameToInt16, INT16_MAX, INT16_MIN>;
using SparkFunctionCastFloatToInt32 = local_engine::SparkFunctionCastFloatToInt<Int32, NameToInt32, INT32_MAX, INT32_MIN>;
using SparkFunctionCastFloatToInt64 = local_engine::SparkFunctionCastFloatToInt<Int64, NameToInt64, INT64_MAX, INT64_MIN>;
using SparkFunctionCastFloatToInt128 = local_engine::SparkFunctionCastFloatToInt<Int128, NameToInt128, std::numeric_limits<Int128>::max(), std::numeric_limits<Int128>::min()>;
using SparkFunctionCastFloatToInt256 = local_engine::SparkFunctionCastFloatToInt<Int256, NameToInt256, std::numeric_limits<Int256>::max(), std::numeric_limits<Int256>::min()>;
using SparkFunctionCastFloatToUInt8 = local_engine::SparkFunctionCastFloatToInt<UInt8, NameToUInt8, UINT8_MAX, 0>;
using SparkFunctionCastFloatToUInt16 = local_engine::SparkFunctionCastFloatToInt<UInt16, NameToUInt16, UINT16_MAX, 0>;
using SparkFunctionCastFloatToUInt32 = local_engine::SparkFunctionCastFloatToInt<UInt32, NameToUInt32, UINT32_MAX, 0>;
using SparkFunctionCastFloatToUInt64 = local_engine::SparkFunctionCastFloatToInt<UInt64, NameToUInt64, UINT64_MAX, 0>;
using SparkFunctionCastFloatToUInt128 = local_engine::SparkFunctionCastFloatToInt<UInt128, NameToUInt128, std::numeric_limits<UInt128>::max(), 0>;
using SparkFunctionCastFloatToUInt256 = local_engine::SparkFunctionCastFloatToInt<UInt256, NameToUInt256, std::numeric_limits<UInt256>::max(), 0>;

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

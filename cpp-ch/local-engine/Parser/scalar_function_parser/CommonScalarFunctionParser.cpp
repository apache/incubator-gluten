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
#include <Functions/SparkFunctionNextDay.h>
#include <Parser/FunctionParser.h>
#include <Common/Exception.h>

namespace local_engine
{
#define REGISTER_COMMON_SCALAR_FUNCTION_PARSER(cls_name, substrait_name, ch_name) \
    class ScalarFunctionParser##cls_name : public FunctionParser \
    { \
    public: \
        ScalarFunctionParser##cls_name(SerializedPlanParser * plan_parser_) : FunctionParser(plan_parser_) \
        { \
        } \
        ~ScalarFunctionParser##cls_name() override = default; \
        static constexpr auto name = #substrait_name; \
        String getName() const override \
        { \
            return #substrait_name; \
        } \
        String getCHFunctionName(const substrait::Expression_ScalarFunction & /*substrait_func*/) const override \
        { \
            return #ch_name; \
        } \
    }; \
    static const FunctionParserRegister<ScalarFunctionParser##cls_name> register_scalar_function_parser_##cls_name;

REGISTER_COMMON_SCALAR_FUNCTION_PARSER(NextDay, next_day, spark_next_day)
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(LastDay, last_day, toLastDayOfMonth)
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(Str2Map, str_to_map, spark_str_to_map)

REGISTER_COMMON_SCALAR_FUNCTION_PARSER(IsNotNull, is_not_null, isNotNull);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(IsNull, is_null, isNull);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(GTE, gte, greaterOrEquals);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(GT, gt, greater);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(LTE, lte, lessOrEquals);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(LT, lt, less);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(And, and, and);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(Or, or, or);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(Equal, equal, equals);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(Not, not, not);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(Xor, xor, xor);

REGISTER_COMMON_SCALAR_FUNCTION_PARSER(Cast, cast, CAST);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(GetTimestamp, get_timestamp, parseDateTimeInJodaSyntaxOrNull);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(Quarter, quarter, toQuarter);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(ToUnixTimestamp, to_unix_timestamp, parseDateTimeInJodaSyntaxOrNull);

REGISTER_COMMON_SCALAR_FUNCTION_PARSER(Position, positive, identity);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(Negative, negative, negate);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(Pmod, pmod, pmod);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(abs, abs, abs);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(Ceil, ceil, ceil);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(Round, round, roundHalfUp);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(Bround, bround, roundBankers);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(Exp, exp, exp);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(Power, power, power);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(Cos, cos, cos);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(Cosh, cosh, cosh);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(Sin, sin, sin);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(Sinh, sinh, sinh);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(Tan, tan, tan);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(Tanh, tanh, tanh);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(Acos, acos, acos);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(Asin, asin, asin);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(Atan, atan, atan);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(Atan2, atan2, atan2);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(Asinh, asinh, asinh);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(Acosh, acosh, acosh);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(Atanh, atanh, atanh);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(BitwiseNot, bitwise_not, bitNot);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(BitwiseAnd, bitwise_and, bitAnd);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(BitwiseOr, bitwise_or, bitOr);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(BitwiseXor, bitwise_xor, bitXor);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(BitGet, bit_get, bitTest);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(BitCount, bit_count, bitCount);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(Sqrt, sqrt, sqrt);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(Cbrc, cbrt, cbrt);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(Degrees, degrees, degrees);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(E, e, e);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(Pi, pi, pi);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(Hex, hex, hex);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(Unhex, unhex, unhex);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(Hypot, hypot, hypot);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(Sign, sign, sign);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(Radians, radians, radians);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(Greatest, greatest, sparkGreatest);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(Least, least, sparkLeast);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(ShiftLeft, shiftleft, bitShiftLeft);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(ShiftRight, shiftright, bitShiftRight);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(Rand, rand, randCanonical);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(Bin, bin, sparkBin);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(Rint, rint, sparkRint);

REGISTER_COMMON_SCALAR_FUNCTION_PARSER(Like, like, like);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(NotLike, not_like, notLike);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(StartsWith, starts_with, startsWithUTF8);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(EndsWith, ends_with, endsWithUTF8);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(Contains, contains, countSubstrings);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(SubstringIndex, substring_index, substringIndexUTF8);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(Lower, lower, lowerUTF8);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(Upper, upper, upperUTF8);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(Strpos, strpos, positionUTF8);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(Replace, replace, replaceAll);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(RegexpReplace, regexp_replace, replaceRegexpAll);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(RegexpExtractAll, regexp_extract_all, regexpExtractAllSpark);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(Rlike, rlike, match);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(Ascii, ascii, ascii);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(Base64, base64, base64Encode);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(Unbase64, unbase64, base64Decode);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(Lpad, lpad, leftPadUTF8);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(Rpad, rpad, rightPadUTF8);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(Translate, translate, translateUTF8);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(Initcap, initcap, initcapUTF8);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(Conv, conv, sparkConv);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(Uuid, uuid, generateUUIDv4);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(LevenshteinDistance, levenshteinDistance, editDistanceUTF8);

REGISTER_COMMON_SCALAR_FUNCTION_PARSER(Crc32, crc32, CRC32);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(Murmur3Hash, murmur3hash, sparkMurmurHash3_32);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(Xxhash64, xxhash64, sparkXxHash64);

REGISTER_COMMON_SCALAR_FUNCTION_PARSER(In, in, in);

REGISTER_COMMON_SCALAR_FUNCTION_PARSER(Coalesce, coalesce, coalesce);

REGISTER_COMMON_SCALAR_FUNCTION_PARSER(FromUnixtime, from_unixtime, fromUnixTimestampInJodaSyntax);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(DateAdd, date_add, addDays);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(DateSub, date_sub, subtractDays);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(DateDiff, datediff, dateDiff);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(Second, second, toSecond);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(AddMonths, add_months, addMonths);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(DateTrunc, date_trunc, dateTrunc);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(FloorDatetime, floor_datetime, dateTrunc);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(Floor, floor, sparkFloor);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(MothsBetween, months_between, sparkMonthsBetween);


// array functions
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(Array, array, array);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(Shuffle, shuffle, arrayShuffle);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(Range, range, range);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(Flatten, flatten, sparkArrayFlatten);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(ArrayJoin, array_join, sparkArrayJoin);

// map functions
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(Map, map, map);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(GetMapValue, get_map_value, arrayElement);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(MapKeys, map_keys, mapKeys);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(MapValues, map_values, mapValues);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(MapFromArrays, map_from_arrays, mapFromArrays);


// json functions
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(FlattenJsonStringOnRequired, flattenJSONStringOnRequired, flattenJSONStringOnRequired);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(ToJson, to_json, toJSONString);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(JsonTuple, json_tuple, json_tuple);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(JsonArrayLen, json_array_length, JSONArrayLength);
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(UnscaledValue, unscaled_value, unscaleValueSpark);

// runtime filter
REGISTER_COMMON_SCALAR_FUNCTION_PARSER(MightContain, might_contain, bloomFilterContains);
}

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
package io.glutenproject.expression

object ExpressionNames {

  // Aggregation functions used by Substrait plan.
  final val SUM = "sum"
  final val AVG = "avg"
  final val COUNT = "count"
  final val COUNT_DISTINCT = "count_distinct"
  final val MIN = "min"
  final val MAX = "max"
  final val MAX_BY = "max_by"
  final val MIN_BY = "min_by"
  final val STDDEV_SAMP = "stddev_samp"
  final val STDDEV_POP = "stddev_pop"
  final val COLLECT_LIST = "collect_list"
  final val COLLECT_SET = "collect_set"
  final val BLOOM_FILTER_AGG = "bloom_filter_agg"
  final val VAR_SAMP = "var_samp"
  final val VAR_POP = "var_pop"
  final val BIT_AND_AGG = "bit_and"
  final val BIT_OR_AGG = "bit_or"
  final val BIT_XOR_AGG = "bit_xor"
  final val CORR = "corr"
  final val COVAR_POP = "covar_pop"
  final val COVAR_SAMP = "covar_samp"
  final val LAST = "last"
  final val LAST_IGNORE_NULL = "last_ignore_null"
  final val FIRST = "first"
  final val FIRST_IGNORE_NULL = "first_ignore_null"
  final val APPROX_DISTINCT = "approx_distinct"
  final val SKEWNESS = "skewness"

  // Function names used by Substrait plan.
  final val ADD = "add"
  final val SUBTRACT = "subtract"
  final val MULTIPLY = "multiply"
  final val DIVIDE = "divide"
  final val POSITIVE = "positive"
  final val NEGATIVE = "negative"
  final val AND = "and"
  final val OR = "or"
  final val CAST = "cast"
  final val COALESCE = "coalesce"
  final val LIKE = "like"
  final val RLIKE = "rlike"
  final val REGEXP_REPLACE = "regexp_replace"
  final val REGEXP_EXTRACT = "regexp_extract"
  final val REGEXP_EXTRACT_ALL = "regexp_extract_all"
  final val EQUAL = "equal"
  final val EQUAL_NULL_SAFE = "equal_null_safe"
  final val LESS_THAN = "lt"
  final val LESS_THAN_OR_EQUAL = "lte"
  final val GREATER_THAN = "gt"
  final val GREATER_THAN_OR_EQUAL = "gte"
  final val ALIAS = "alias"
  final val IS_NOT_NULL = "is_not_null"
  final val IS_NULL = "is_null"
  final val NOT = "not"
  final val IS_NAN = "isnan"
  final val NANVL = "nanvl"

  // SparkSQL String functions
  final val ASCII = "ascii"
  final val CHR = "chr"
  final val ELT = "elt"
  final val EXTRACT = "extract"
  final val ENDS_WITH = "ends_with"
  final val STARTS_WITH = "starts_with"
  final val CONCAT = "concat"
  final val CONTAINS = "contains"
  final val INSTR = "strpos" // instr
  final val LENGTH = "char_length" // length
  final val LOWER = "lower"
  final val UPPER = "upper"
  final val LOCATE = "locate"
  final val LTRIM = "ltrim"
  final val RTRIM = "rtrim"
  final val TRIM = "trim"
  final val LPAD = "lpad"
  final val RPAD = "rpad"
  final val REPLACE = "replace"
  final val REVERSE = "reverse"
  final val SPLIT = "split"
  final val SUBSTRING = "substring"
  final val SUBSTRING_INDEX = "substring_index"
  final val CONCAT_WS = "concat_ws"
  final val LEFT = "left"
  final val REPEAT = "repeat"
  final val TRANSLATE = "translate"
  final val SPACE = "space"
  final val EMPTY2NULL = "empty2null"
  final val INITCAP = "initcap"
  final val OVERLAY = "overlay"
  final val CONV = "conv"
  final val FIND_IN_SET = "find_in_set"
  final val DECODE = "decode"
  final val ENCODE = "encode"
  final val UUID = "uuid"
  final val BIT_LENGTH = "bit_length"

  // URL functions
  final val PARSE_URL = "parse_url"
  final val URL_DECODE = "url_decode"

  // SparkSQL Math functions
  final val ABS = "abs"
  final val ACOSH = "acosh"
  final val ASINH = "asinh"
  final val ATANH = "atanh"
  final val BIN = "bin"
  final val CEIL = "ceil"
  final val FLOOR = "floor"
  final val EXP = "exp"
  final val POWER = "power"
  final val PMOD = "pmod"
  final val ROUND = "round"
  final val BROUND = "bround"
  final val SIN = "sin"
  final val SINH = "sinh"
  final val TAN = "tan"
  final val TANH = "tanh"
  final val BITWISE_NOT = "bitwise_not"
  final val BITWISE_AND = "bitwise_and"
  final val BITWISE_OR = "bitwise_or"
  final val BITWISE_XOR = "bitwise_xor"
  final val BITWISE_GET = "bit_get"
  final val BITWISE_COUNT = "bit_count"
  final val SHIFTLEFT = "shiftleft"
  final val SHIFTRIGHT = "shiftright"
  final val SHIFTRIGHTUNSIGNED = "shiftrightunsigned"
  final val SQRT = "sqrt"
  final val CBRT = "cbrt"
  final val E = "e"
  final val PI = "pi"
  final val HEX = "hex"
  final val UNHEX = "unhex"
  final val HYPOT = "hypot"
  final val SIGN = "sign"
  final val LOG1P = "log1p"
  final val LOG2 = "log2"
  final val LOG = "log"
  final val LOGARITHM = "logarithm"
  final val RADIANS = "radians"
  final val GREATEST = "greatest"
  final val LEAST = "least"
  final val REMAINDER = "modulus"
  final val FACTORIAL = "factorial"
  final val RAND = "rand"

  // PrestoSQL Math functions
  final val ACOS = "acos"
  final val ASIN = "asin"
  final val ATAN = "atan"
  final val ATAN2 = "atan2"
  final val COS = "cos"
  final val COSH = "cosh"
  final val DEGREES = "degrees"
  final val LOG10 = "log10"

  // SparkSQL DateTime functions
  // Fully supporting wait for https://github.com/ClickHouse/ClickHouse/pull/43818
  final val FROM_UNIXTIME = "from_unixtime"
  final val DATE_ADD = "date_add"
  final val DATE_SUB = "date_sub"
  final val DATE_DIFF = "datediff"
  final val TO_UNIX_TIMESTAMP = "to_unix_timestamp"
  final val UNIX_TIMESTAMP = "unix_timestamp"
  final val ADD_MONTHS = "add_months"
  final val DATE_FORMAT = "date_format"
  final val TRUNC = "trunc"
  final val DATE_TRUNC = "date_trunc"
  final val GET_TIMESTAMP = "get_timestamp" // for function: to_date/to_timestamp
  final val TIMESTAMP_ADD = "timestamp_add"
  final val NEXT_DAY = "next_day"
  final val LAST_DAY = "last_day"
  final val MONTHS_BETWEEN = "months_between"
  final val DATE_FROM_UNIX_DATE = "date_from_unix_date"
  final val MAKE_TIMESTAMP = "make_timestamp"
  final val MAKE_YM_INTERVAL = "make_ym_interval"

  // JSON functions
  final val GET_JSON_OBJECT = "get_json_object"
  final val JSON_ARRAY_LENGTH = "json_array_length"
  final val TO_JSON = "to_json"
  final val FROM_JSON = "from_json"
  final val JSON_TUPLE = "json_tuple"

  // Hash functions
  final val MURMUR3HASH = "murmur3hash"
  final val XXHASH64 = "xxhash64"
  final val MD5 = "md5"
  final val SHA1 = "sha1"
  final val SHA2 = "sha2"
  final val CRC32 = "crc32"

  // Array functions
  final val SIZE = "size"
  final val SLICE = "slice"
  final val SEQUENCE = "sequence"
  final val CREATE_ARRAY = "array"
  final val GET_ARRAY_ITEM = "get_array_item"
  final val ELEMENT_AT = "element_at"
  final val ARRAY_CONTAINS = "array_contains"
  final val ARRAY_MAX = "array_max"
  final val ARRAY_MIN = "array_min"
  final val ARRAY_JOIN = "array_join"
  final val SORT_ARRAY = "sort_array"
  final val ARRAYS_OVERLAP = "arrays_overlap"
  final val ARRAY_POSITION = "array_position"
  final val ARRAY_DISTINCT = "array_distinct"
  final val ARRAY_UNION = "array_union"
  final val ARRAY_INTERSECT = "array_intersect"
  final val GET_ARRAY_STRUCT_FIELDS = "get_array_struct_fields"
  final val ARRAY_EXCEPT = "array_except"
  final val ARRAY_REPEAT = "array_repeat"

  // Map functions
  final val CREATE_MAP = "map"
  final val GET_MAP_VALUE = "get_map_value"
  final val MAP_KEYS = "map_keys"
  final val MAP_VALUES = "map_values"
  final val MAP_FROM_ARRAYS = "map_from_arrays"
  final val MAP_ENTRIES = "map_entries"
  final val STR_TO_MAP = "str_to_map"

  // struct functions
  final val GET_STRUCT_FIELD = "get_struct_field"
  final val NAMED_STRUCT = "named_struct"

  // Spark 3.3
  final val SPLIT_PART = "split_part"
  final val MIGHT_CONTAIN = "might_contain"
  final val SEC = "sec"
  final val CSC = "csc"

  // Specific expression
  final val IF = "if"
  final val ATTRIBUTE_REFERENCE = "attribute_reference"
  final val BOUND_REFERENCE = "bound_reference"
  final val LITERAL = "literal"
  final val NAMED_LAMBDA_VARIABLE = "namedlambdavariable"
  final val CASE_WHEN = "case_when"
  final val IN = "in"
  final val IN_SET = "in_set"
  final val SCALAR_SUBQUERY = "scalar_subquery"
  final val AGGREGATE = "aggregate"
  final val LAMBDAFUNCTION = "lambdafunction"
  final val EXPLODE = "explode"
  final val INLINE = "inline"
  final val POSEXPLODE = "posexplode"
  final val CHECK_OVERFLOW = "check_overflow"
  final val MAKE_DECIMAL = "make_decimal"
  final val PROMOTE_PRECISION = "promote_precision"
  final val SPARK_PARTITION_ID = "spark_partition_id"
  final val MONOTONICALLY_INCREASING_ID = "monotonically_increasing_id"

  // Directly use child expression transformer
  final val KNOWN_FLOATING_POINT_NORMALIZED = "known_floating_point_normalized"
  final val NORMALIZE_NANAND_ZERO = "normalize_nanand_zero"

  // Window functions used by Substrait plan.
  final val RANK = "rank"
  final val DENSE_RANK = "dense_rank"
  final val ROW_NUMBER = "row_number"
  final val CUME_DIST = "cume_dist"
  final val PERCENT_RANK = "percent_rank"
  final val NTILE = "ntile"
  final val LEAD = "lead"
  final val LAG = "lag"
  final val NTH_VALUE = "nth_value"

  // Decimal functions
  final val UNSCALED_VALUE = "unscaled_value"

  // A placeholder for native UDF functions
  final val UDF_PLACEHOLDER = "udf_placeholder"
}

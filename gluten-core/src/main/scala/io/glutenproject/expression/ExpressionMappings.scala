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

import io.glutenproject.sql.shims.SparkShimLoader
import io.glutenproject.GlutenConfig

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.optimizer.NormalizeNaNAndZero
import org.apache.spark.sql.execution.ScalarSubquery

object ExpressionMappings {

  // Aggregation functions used by Substrait plan.
  final val SUM = "sum"
  final val AVG = "avg"
  final val COUNT = "count"
  final val MIN = "min"
  final val MAX = "max"
  final val STDDEV = "stddev"
  final val STDDEV_SAMP = "stddev_samp"
  final val STDDEV_POP = "stddev_pop"
  final val COLLECT_LIST = "collect_list"
  final val BLOOM_FILTER_AGG = "bloom_filter_agg"
  final val VAR_SAMP = "var_samp"
  final val VAR_POP = "var_pop"
  final val BIT_AND_AGG = "bitwise_and_agg"
  final val BIT_OR_AGG = "bitwise_or_agg"
  final val CORR = "corr"
  final val COVAR_POP = "covar_pop"
  final val COVAR_SAMP = "covar_samp"
  final val LAST = "last"

  // Function names used by Substrait plan.
  final val ADD = "add"
  final val SUBTRACT = "subtract"
  final val MULTIPLY = "multiply"
  final val DIVIDE = "divide"
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
  final val LESS_THAN = "lt"
  final val LESS_THAN_OR_EQUAL = "lte"
  final val GREATER_THAN = "gt"
  final val GREATER_THAN_OR_EQUAL = "gte"
  final val ALIAS = "alias"
  final val IS_NOT_NULL = "is_not_null"
  final val IS_NULL = "is_null"
  final val NOT = "not"
  final val IS_NAN = "isnan"

  // SparkSQL String functions
  final val ASCII = "ascii"
  final val CHR = "chr"
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
  final val CONCAT_WS = "concat_ws"
  final val REPEAT = "repeat"
  final val TRANSLATE = "translate"
  final val SPACE = "space"

  // SparkSQL Math functions
  final val ABS = "abs"
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
  final val SHIFTLEFT = "shiftleft"
  final val SHIFTRIGHT = "shiftright"
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

  // JSON functions
  final val GET_JSON_OBJECT = "get_json_object"
  final val JSON_ARRAY_LENGTH = "json_array_length"
  final val TO_JSON = "to_json"
  final val FROM_JSON = "from_json"

  // Hash functions
  final val MURMUR3HASH = "murmur3hash"
  final val XXHASH64 = "xxhash64"
  final val MD5 = "md5"
  final val SHA1 = "sha1"
  final val SHA2 = "sha2"
  final val CRC32 = "crc32"

  // Array functions
  final val SIZE = "size"
  final val CREATE_ARRAY = "array"
  final val GET_ARRAY_ITEM = "get_array_item"
  final val ELEMENT_AT = "element_at"
  final val ARRAY_CONTAINS = "array_contains"
  final val ARRAY_MAX = "array_max"
  final val ARRAY_MIN = "array_min"
  final val SEQUENCE = "sequence"
  final val SORT_ARRAY = "sort_array"
  final val ARRAYS_OVERLAP = "arrays_overlap"
  final val SLICE = "slice"

  // Map functions
  final val CREATE_MAP = "map"
  final val GET_MAP_VALUE = "get_map_value"
  final val MAP_KEYS = "map_keys"
  final val MAP_VALUES = "map_values"
  final val MAP_FROM_ARRAYS = "map_from_arrays"

  // struct functions
  final val GET_STRUCT_FIELD = "get_struct_field"
  final val NAMED_STRUCT = "named_struct"

  // Spark 3.3
  final val SPLIT_PART = "split_part"
  final val MIGHT_CONTAIN = "might_contain"

  // Specific expression
  final val IF = "if"
  final val ATTRIBUTE_REFERENCE = "attribute_reference"
  final val BOUND_REFERENCE = "bound_reference"
  final val LITERAL = "literal"
  final val CASE_WHEN = "case_when"
  final val IN = "in"
  final val IN_SET = "in_set"
  final val SCALAR_SUBQUERY = "scalar_subquery"
  final val EXPLODE = "explode"
  final val POSEXPLODE = "posexplode"
  final val CHECK_OVERFLOW = "check_overflow"
  final val MAKE_DECIMAL = "make_decimal"
  final val PROMOTE_PRECISION = "promote_precision"
  final val ROW_CONSTRUCTOR = "row_constructor"

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

  // Decimal functions
  final val UNSCALED_VALUE = "unscaled_value"

  /**
   * Mapping Spark scalar expression to Substrait function name
   */
  val SCALAR_SIGS: Seq[Sig] = Seq(
    Sig[Add](ADD),
    Sig[Subtract](SUBTRACT),
    Sig[Multiply](MULTIPLY),
    Sig[Divide](DIVIDE),
    Sig[And](AND),
    Sig[Or](OR),
    Sig[Cast](CAST),
    Sig[Coalesce](COALESCE),
    Sig[Like](LIKE),
    Sig[RLike](RLIKE),
    Sig[RegExpReplace](REGEXP_REPLACE),
    Sig[RegExpExtract](REGEXP_EXTRACT),
    Sig[RegExpExtractAll](REGEXP_EXTRACT_ALL),
    Sig[EqualTo](EQUAL),
    Sig[LessThan](LESS_THAN),
    Sig[LessThanOrEqual](LESS_THAN_OR_EQUAL),
    Sig[GreaterThan](GREATER_THAN),
    Sig[GreaterThanOrEqual](GREATER_THAN_OR_EQUAL),
    Sig[Alias](ALIAS),
    Sig[IsNotNull](IS_NOT_NULL),
    Sig[IsNull](IS_NULL),
    Sig[Not](NOT),
    Sig[IsNaN](IS_NAN),
    // SparkSQL String functions
    Sig[Ascii](ASCII),
    Sig[Chr](CHR),
    Sig[Extract](EXTRACT),
    Sig[EndsWith](ENDS_WITH),
    Sig[StartsWith](STARTS_WITH),
    Sig[Concat](CONCAT),
    Sig[Contains](CONTAINS),
    Sig[StringInstr](INSTR),
    Sig[Length](LENGTH),
    Sig[Lower](LOWER),
    Sig[Upper](UPPER),
    Sig[StringLocate](LOCATE),
    Sig[StringTrimLeft](LTRIM),
    Sig[StringTrimRight](RTRIM),
    Sig[StringTrim](TRIM),
    Sig[StringLPad](LPAD),
    Sig[StringRPad](RPAD),
    Sig[StringReplace](REPLACE),
    Sig[Reverse](REVERSE),
    Sig[StringSplit](SPLIT),
    Sig[Substring](SUBSTRING),
    Sig[ConcatWs](CONCAT_WS),
    Sig[StringRepeat](REPEAT),
    Sig[StringTranslate](TRANSLATE),
    Sig[StringSpace](SPACE),
    // SparkSQL Math functions
    Sig[Abs](ABS),
    Sig[Ceil](CEIL),
    Sig[Floor](FLOOR),
    Sig[Exp](EXP),
    Sig[Pow](POWER),
    Sig[Pmod](PMOD),
    Sig[Round](ROUND),
    Sig[BRound](BROUND),
    Sig[Sin](SIN),
    Sig[Sinh](SINH),
    Sig[Tan](TAN),
    Sig[Tanh](TANH),
    Sig[BitwiseNot](BITWISE_NOT),
    Sig[BitwiseAnd](BITWISE_AND),
    Sig[BitwiseOr](BITWISE_OR),
    Sig[BitwiseXor](BITWISE_XOR),
    Sig[ShiftLeft](SHIFTLEFT),
    Sig[ShiftRight](SHIFTRIGHT),
    Sig[Sqrt](SQRT),
    Sig[Cbrt](CBRT),
    Sig[EulerNumber](E),
    Sig[Pi](PI),
    Sig[Hex](HEX),
    Sig[Unhex](UNHEX),
    Sig[Hypot](HYPOT),
    Sig[Signum](SIGN),
    Sig[Log1p](LOG1P),
    Sig[Log2](LOG2),
    Sig[Log](LOG),
    Sig[ToRadians](RADIANS),
    Sig[Greatest](GREATEST),
    Sig[Least](LEAST),
    Sig[Remainder](REMAINDER),
    Sig[Factorial](FACTORIAL),
    Sig[Rand](RAND),
    // PrestoSQL Math functions
    Sig[Acos](ACOS),
    Sig[Asin](ASIN),
    Sig[Atan](ATAN),
    Sig[Atan2](ATAN2),
    Sig[Cos](COS),
    Sig[Cosh](COSH),
    Sig[Log10](LOG10),
    Sig[ToDegrees](DEGREES),
    // SparkSQL DateTime functions
    Sig[Year](EXTRACT),
    Sig[YearOfWeek](EXTRACT),
    Sig[Quarter](EXTRACT),
    Sig[Month](EXTRACT),
    Sig[WeekOfYear](EXTRACT),
    Sig[WeekDay](EXTRACT),
    Sig[DayOfWeek](EXTRACT),
    Sig[DayOfMonth](EXTRACT),
    Sig[DayOfYear](EXTRACT),
    Sig[Hour](EXTRACT),
    Sig[Minute](EXTRACT),
    Sig[Second](EXTRACT),
    Sig[FromUnixTime](FROM_UNIXTIME),
    Sig[DateAdd](DATE_ADD),
    Sig[DateSub](DATE_SUB),
    Sig[DateDiff](DATE_DIFF),
    Sig[ToUnixTimestamp](TO_UNIX_TIMESTAMP),
    Sig[UnixTimestamp](UNIX_TIMESTAMP),
    Sig[AddMonths](ADD_MONTHS),
    Sig[DateFormatClass](DATE_FORMAT),
    Sig[TruncDate](TRUNC),
    // JSON functions
    Sig[GetJsonObject](GET_JSON_OBJECT),
    Sig[LengthOfJsonArray](JSON_ARRAY_LENGTH),
    Sig[StructsToJson](TO_JSON),
    Sig[JsonToStructs](FROM_JSON),
    // Hash functions
    Sig[Murmur3Hash](MURMUR3HASH),
    Sig[XxHash64](XXHASH64),
    Sig[Md5](MD5),
    Sig[Sha1](SHA1),
    Sig[Sha2](SHA2),
    Sig[Crc32](CRC32),
    // Array functions
    Sig[Size](SIZE),
    Sig[CreateArray](CREATE_ARRAY),
    Sig[Explode](EXPLODE),
    Sig[PosExplode](POSEXPLODE),
    Sig[GetArrayItem](GET_ARRAY_ITEM),
    Sig[ElementAt](ELEMENT_AT),
    Sig[ArrayContains](ARRAY_CONTAINS),
    Sig[ArrayMax](ARRAY_MAX),
    Sig[ArrayMin](ARRAY_MIN),
    Sig[Sequence](SEQUENCE),
    Sig[SortArray](SORT_ARRAY),
    Sig[ArraysOverlap](ARRAYS_OVERLAP),
    Sig[Slice](SLICE),

    // Map functions
    Sig[CreateMap](CREATE_MAP),
    Sig[GetMapValue](GET_MAP_VALUE),
    Sig[MapKeys](MAP_KEYS),
    Sig[MapValues](MAP_VALUES),
    Sig[MapFromArrays](MAP_FROM_ARRAYS),
    // Struct functions
    Sig[GetStructField](GET_STRUCT_FIELD),
    Sig[CreateNamedStruct](NAMED_STRUCT),
    // Directly use child expression transformer
    Sig[KnownFloatingPointNormalized](KNOWN_FLOATING_POINT_NORMALIZED),
    Sig[NormalizeNaNAndZero](NORMALIZE_NANAND_ZERO),
    // Specific expression
    Sig[If](IF),
    Sig[AttributeReference](ATTRIBUTE_REFERENCE),
    Sig[BoundReference](BOUND_REFERENCE),
    Sig[Literal](LITERAL),
    Sig[CaseWhen](CASE_WHEN),
    Sig[In](IN),
    Sig[InSet](IN_SET),
    Sig[ScalarSubquery](SCALAR_SUBQUERY),
    Sig[CheckOverflow](CHECK_OVERFLOW),
    Sig[MakeDecimal](MAKE_DECIMAL),
    Sig[PromotePrecision](PROMOTE_PRECISION),
    // Decimal
    Sig[UnscaledValue](UNSCALED_VALUE)
  ) ++ SparkShimLoader.getSparkShims.expressionMappings

  /**
   * Mapping Spark aggregation expression to Substrait function name
   */
  val AGGREGATE_SIGS: Seq[Sig] = Seq(
    Sig[Sum](SUM),
    Sig[Average](AVG),
    Sig[Count](COUNT),
    Sig[Min](MIN),
    Sig[Max](MAX),
    Sig[StddevSamp](STDDEV_SAMP),
    Sig[StddevPop](STDDEV_POP),
    Sig[CollectList](COLLECT_LIST),
    Sig[VarianceSamp](VAR_SAMP),
    Sig[VariancePop](VAR_POP),
    Sig[BitAndAgg](BIT_AND_AGG),
    Sig[BitOrAgg](BIT_OR_AGG),
    Sig[Corr](CORR),
    Sig[CovPopulation](COVAR_POP),
    Sig[CovSample](COVAR_SAMP),
    Sig[Last](LAST)
  )

  /**
   * Mapping Spark window expression to Substrait function name
   */
  val WINDOW_SIGS: Seq[Sig] = Seq(
    Sig[Rank](RANK),
    Sig[DenseRank](DENSE_RANK),
    Sig[RowNumber](ROW_NUMBER),
    Sig[CumeDist](CUME_DIST),
    Sig[PercentRank](PERCENT_RANK),
    Sig[NTile](NTILE)
  )

  // some spark new version class
  def getScalarSigOther: Map[String, String] =
    if (GlutenConfig.getConf.enableNativeBloomFilter) {
      Map((MIGHT_CONTAIN, MIGHT_CONTAIN))
    } else Map()

  def getAggSigOther: Map[String, String] =
    if (GlutenConfig.getConf.enableNativeBloomFilter) {
      Map((BLOOM_FILTER_AGG, BLOOM_FILTER_AGG))
    } else Map()

  lazy val scalar_functions_map: Map[Class[_], String] =
    SCALAR_SIGS.map(s => (s.expClass, s.name)).toMap
  lazy val aggregate_functions_map: Map[Class[_], String] =
    AGGREGATE_SIGS.map(s => (s.expClass, s.name)).toMap
  lazy val window_functions_map: Map[Class[_], String] =
    WINDOW_SIGS.map(s => (s.expClass, s.name)).toMap
}

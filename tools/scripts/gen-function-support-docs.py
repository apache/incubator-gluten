#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import os
import findspark

import argparse
import logging
import re
import subprocess
import tabulate

# Fetched from org.apache.spark.sql.catalyst.analysis.FunctionRegistry.
SPARK35_EXPRESSION_MAPPINGS = '''
    // misc non-aggregate functions
    expression[Abs]("abs"),
    expression[Coalesce]("coalesce"),
    expressionBuilder("explode", ExplodeExpressionBuilder),
    expressionGeneratorBuilderOuter("explode_outer", ExplodeExpressionBuilder),
    expression[Greatest]("greatest"),
    expression[If]("if"),
    expression[Inline]("inline"),
    expressionGeneratorOuter[Inline]("inline_outer"),
    expression[IsNaN]("isnan"),
    expression[Nvl]("ifnull", setAlias = true),
    expression[IsNull]("isnull"),
    expression[IsNotNull]("isnotnull"),
    expression[Least]("least"),
    expression[NaNvl]("nanvl"),
    expression[NullIf]("nullif"),
    expression[Nvl]("nvl"),
    expression[Nvl2]("nvl2"),
    expression[PosExplode]("posexplode"),
    expressionGeneratorOuter[PosExplode]("posexplode_outer"),
    expression[Rand]("rand"),
    expression[Rand]("random", true),
    expression[Randn]("randn"),
    expression[Stack]("stack"),
    expression[CaseWhen]("when"),

    // math functions
    expression[Acos]("acos"),
    expression[Acosh]("acosh"),
    expression[Asin]("asin"),
    expression[Asinh]("asinh"),
    expression[Atan]("atan"),
    expression[Atan2]("atan2"),
    expression[Atanh]("atanh"),
    expression[Bin]("bin"),
    expression[BRound]("bround"),
    expression[Cbrt]("cbrt"),
    expressionBuilder("ceil", CeilExpressionBuilder),
    expressionBuilder("ceiling", CeilExpressionBuilder, true),
    expression[Cos]("cos"),
    expression[Sec]("sec"),
    expression[Cosh]("cosh"),
    expression[Conv]("conv"),
    expression[ToDegrees]("degrees"),
    expression[EulerNumber]("e"),
    expression[Exp]("exp"),
    expression[Expm1]("expm1"),
    expressionBuilder("floor", FloorExpressionBuilder),
    expression[Factorial]("factorial"),
    expression[Hex]("hex"),
    expression[Hypot]("hypot"),
    expression[Logarithm]("log"),
    expression[Log10]("log10"),
    expression[Log1p]("log1p"),
    expression[Log2]("log2"),
    expression[Log]("ln"),
    expression[Remainder]("mod", true),
    expression[UnaryMinus]("negative", true),
    expression[Pi]("pi"),
    expression[Pmod]("pmod"),
    expression[UnaryPositive]("positive"),
    expression[Pow]("pow", true),
    expression[Pow]("power"),
    expression[ToRadians]("radians"),
    expression[Rint]("rint"),
    expression[Round]("round"),
    expression[ShiftLeft]("shiftleft"),
    expression[ShiftRight]("shiftright"),
    expression[ShiftRightUnsigned]("shiftrightunsigned"),
    expression[Signum]("sign", true),
    expression[Signum]("signum"),
    expression[Sin]("sin"),
    expression[Csc]("csc"),
    expression[Sinh]("sinh"),
    expression[StringToMap]("str_to_map"),
    expression[Sqrt]("sqrt"),
    expression[Tan]("tan"),
    expression[Cot]("cot"),
    expression[Tanh]("tanh"),
    expression[WidthBucket]("width_bucket"),

    expression[Add]("+"),
    expression[Subtract]("-"),
    expression[Multiply]("*"),
    expression[Divide]("/"),
    expression[IntegralDivide]("div"),
    expression[Remainder]("%"),

    // "try_*" function which always return Null instead of runtime error.
    expression[TryAdd]("try_add"),
    expression[TryDivide]("try_divide"),
    expression[TrySubtract]("try_subtract"),
    expression[TryMultiply]("try_multiply"),
    expression[TryElementAt]("try_element_at"),
    expressionBuilder("try_avg", TryAverageExpressionBuilder, setAlias = true),
    expressionBuilder("try_sum", TrySumExpressionBuilder, setAlias = true),
    expression[TryToBinary]("try_to_binary"),
    expressionBuilder("try_to_timestamp", TryToTimestampExpressionBuilder, setAlias = true),
    expression[TryAesDecrypt]("try_aes_decrypt"),

    // aggregate functions
    expression[HyperLogLogPlusPlus]("approx_count_distinct"),
    expression[Average]("avg"),
    expression[Corr]("corr"),
    expression[Count]("count"),
    expression[CountIf]("count_if"),
    expression[CovPopulation]("covar_pop"),
    expression[CovSample]("covar_samp"),
    expression[First]("first"),
    expression[First]("first_value", true),
    expression[AnyValue]("any_value"),
    expression[Kurtosis]("kurtosis"),
    expression[Last]("last"),
    expression[Last]("last_value", true),
    expression[Max]("max"),
    expression[MaxBy]("max_by"),
    expression[Average]("mean", true),
    expression[Min]("min"),
    expression[MinBy]("min_by"),
    expression[Percentile]("percentile"),
    expression[Median]("median"),
    expression[Skewness]("skewness"),
    expression[ApproximatePercentile]("percentile_approx"),
    expression[ApproximatePercentile]("approx_percentile", true),
    expression[HistogramNumeric]("histogram_numeric"),
    expression[StddevSamp]("std", true),
    expression[StddevSamp]("stddev", true),
    expression[StddevPop]("stddev_pop"),
    expression[StddevSamp]("stddev_samp"),
    expression[Sum]("sum"),
    expression[VarianceSamp]("variance", true),
    expression[VariancePop]("var_pop"),
    expression[VarianceSamp]("var_samp"),
    expression[CollectList]("collect_list"),
    expression[CollectList]("array_agg", true, Some("3.3.0")),
    expression[CollectSet]("collect_set"),
    expressionBuilder("count_min_sketch", CountMinSketchAggExpressionBuilder),
    expression[BoolAnd]("every", true),
    expression[BoolAnd]("bool_and"),
    expression[BoolOr]("any", true),
    expression[BoolOr]("some", true),
    expression[BoolOr]("bool_or"),
    expression[RegrCount]("regr_count"),
    expression[RegrAvgX]("regr_avgx"),
    expression[RegrAvgY]("regr_avgy"),
    expression[RegrR2]("regr_r2"),
    expression[RegrSXX]("regr_sxx"),
    expression[RegrSXY]("regr_sxy"),
    expression[RegrSYY]("regr_syy"),
    expression[RegrSlope]("regr_slope"),
    expression[RegrIntercept]("regr_intercept"),
    expression[Mode]("mode"),
    expression[HllSketchAgg]("hll_sketch_agg"),
    expression[HllUnionAgg]("hll_union_agg"),

    // string functions
    expression[Ascii]("ascii"),
    expression[Chr]("char", true),
    expression[Chr]("chr"),
    expressionBuilder("contains", ContainsExpressionBuilder),
    expressionBuilder("startswith", StartsWithExpressionBuilder),
    expressionBuilder("endswith", EndsWithExpressionBuilder),
    expression[Base64]("base64"),
    expression[BitLength]("bit_length"),
    expression[Length]("char_length", true),
    expression[Length]("character_length", true),
    expression[ConcatWs]("concat_ws"),
    expression[Decode]("decode"),
    expression[Elt]("elt"),
    expression[Encode]("encode"),
    expression[FindInSet]("find_in_set"),
    expression[FormatNumber]("format_number"),
    expression[FormatString]("format_string"),
    expression[ToNumber]("to_number"),
    expression[TryToNumber]("try_to_number"),
    expression[ToCharacter]("to_char"),
    expression[ToCharacter]("to_varchar", setAlias = true, Some("3.5.0")),
    expression[GetJsonObject]("get_json_object"),
    expression[InitCap]("initcap"),
    expression[StringInstr]("instr"),
    expression[Lower]("lcase", true),
    expression[Length]("length"),
    expression[Length]("len", setAlias = true, Some("3.4.0")),
    expression[Levenshtein]("levenshtein"),
    expression[Luhncheck]("luhn_check"),
    expression[Like]("like"),
    expression[ILike]("ilike"),
    expression[Lower]("lower"),
    expression[OctetLength]("octet_length"),
    expression[StringLocate]("locate"),
    expressionBuilder("lpad", LPadExpressionBuilder),
    expression[StringTrimLeft]("ltrim"),
    expression[JsonTuple]("json_tuple"),
    expression[StringLocate]("position", true),
    expression[FormatString]("printf", true),
    expression[RegExpExtract]("regexp_extract"),
    expression[RegExpExtractAll]("regexp_extract_all"),
    expression[RegExpReplace]("regexp_replace"),
    expression[StringRepeat]("repeat"),
    expression[StringReplace]("replace"),
    expression[Overlay]("overlay"),
    expression[RLike]("rlike"),
    expression[RLike]("regexp_like", true, Some("3.2.0")),
    expression[RLike]("regexp", true, Some("3.2.0")),
    expressionBuilder("rpad", RPadExpressionBuilder),
    expression[StringTrimRight]("rtrim"),
    expression[Sentences]("sentences"),
    expression[SoundEx]("soundex"),
    expression[StringSpace]("space"),
    expression[StringSplit]("split"),
    expression[SplitPart]("split_part"),
    expression[Substring]("substr", true),
    expression[Substring]("substring"),
    expression[Left]("left"),
    expression[Right]("right"),
    expression[SubstringIndex]("substring_index"),
    expression[StringTranslate]("translate"),
    expression[StringTrim]("trim"),
    expression[StringTrimBoth]("btrim"),
    expression[Upper]("ucase", true),
    expression[UnBase64]("unbase64"),
    expression[Unhex]("unhex"),
    expression[Upper]("upper"),
    expression[XPathList]("xpath"),
    expression[XPathBoolean]("xpath_boolean"),
    expression[XPathDouble]("xpath_double"),
    expression[XPathDouble]("xpath_number", true),
    expression[XPathFloat]("xpath_float"),
    expression[XPathInt]("xpath_int"),
    expression[XPathLong]("xpath_long"),
    expression[XPathShort]("xpath_short"),
    expression[XPathString]("xpath_string"),
    expression[RegExpCount]("regexp_count"),
    expression[RegExpSubStr]("regexp_substr"),
    expression[RegExpInStr]("regexp_instr"),

    // url functions
    expression[UrlEncode]("url_encode"),
    expression[UrlDecode]("url_decode"),
    expression[ParseUrl]("parse_url"),

    // datetime functions
    expression[AddMonths]("add_months"),
    expression[CurrentDate]("current_date"),
    expressionBuilder("curdate", CurDateExpressionBuilder, setAlias = true),
    expression[CurrentTimestamp]("current_timestamp"),
    expression[CurrentTimeZone]("current_timezone"),
    expression[LocalTimestamp]("localtimestamp"),
    expression[DateDiff]("datediff"),
    expression[DateDiff]("date_diff", setAlias = true, Some("3.4.0")),
    expression[DateAdd]("date_add"),
    expression[DateAdd]("dateadd", setAlias = true, Some("3.4.0")),
    expression[DateFormatClass]("date_format"),
    expression[DateSub]("date_sub"),
    expression[DayOfMonth]("day", true),
    expression[DayOfYear]("dayofyear"),
    expression[DayOfMonth]("dayofmonth"),
    expression[FromUnixTime]("from_unixtime"),
    expression[FromUTCTimestamp]("from_utc_timestamp"),
    expression[Hour]("hour"),
    expression[LastDay]("last_day"),
    expression[Minute]("minute"),
    expression[Month]("month"),
    expression[MonthsBetween]("months_between"),
    expression[NextDay]("next_day"),
    expression[Now]("now"),
    expression[Quarter]("quarter"),
    expression[Second]("second"),
    expression[ParseToTimestamp]("to_timestamp"),
    expression[ParseToDate]("to_date"),
    expression[ToBinary]("to_binary"),
    expression[ToUnixTimestamp]("to_unix_timestamp"),
    expression[ToUTCTimestamp]("to_utc_timestamp"),
    // We keep the 2 expression builders below to have different function docs.
    expressionBuilder("to_timestamp_ntz", ParseToTimestampNTZExpressionBuilder, setAlias = true),
    expressionBuilder("to_timestamp_ltz", ParseToTimestampLTZExpressionBuilder, setAlias = true),
    expression[TruncDate]("trunc"),
    expression[TruncTimestamp]("date_trunc"),
    expression[UnixTimestamp]("unix_timestamp"),
    expression[DayOfWeek]("dayofweek"),
    expression[WeekDay]("weekday"),
    expression[WeekOfYear]("weekofyear"),
    expression[Year]("year"),
    expression[TimeWindow]("window"),
    expression[SessionWindow]("session_window"),
    expression[WindowTime]("window_time"),
    expression[MakeDate]("make_date"),
    expression[MakeTimestamp]("make_timestamp"),
    // We keep the 2 expression builders below to have different function docs.
    expressionBuilder("make_timestamp_ntz", MakeTimestampNTZExpressionBuilder, setAlias = true),
    expressionBuilder("make_timestamp_ltz", MakeTimestampLTZExpressionBuilder, setAlias = true),
    expression[MakeInterval]("make_interval"),
    expression[MakeDTInterval]("make_dt_interval"),
    expression[MakeYMInterval]("make_ym_interval"),
    expression[Extract]("extract"),
    // We keep the `DatePartExpressionBuilder` to have different function docs.
    expressionBuilder("date_part", DatePartExpressionBuilder, setAlias = true),
    expressionBuilder("datepart", DatePartExpressionBuilder, setAlias = true, Some("3.4.0")),
    expression[DateFromUnixDate]("date_from_unix_date"),
    expression[UnixDate]("unix_date"),
    expression[SecondsToTimestamp]("timestamp_seconds"),
    expression[MillisToTimestamp]("timestamp_millis"),
    expression[MicrosToTimestamp]("timestamp_micros"),
    expression[UnixSeconds]("unix_seconds"),
    expression[UnixMillis]("unix_millis"),
    expression[UnixMicros]("unix_micros"),
    expression[ConvertTimezone]("convert_timezone"),

    // collection functions
    expression[CreateArray]("array"),
    expression[ArrayContains]("array_contains"),
    expression[ArraysOverlap]("arrays_overlap"),
    expression[ArrayInsert]("array_insert"),
    expression[ArrayIntersect]("array_intersect"),
    expression[ArrayJoin]("array_join"),
    expression[ArrayPosition]("array_position"),
    expression[ArraySize]("array_size"),
    expression[ArraySort]("array_sort"),
    expression[ArrayExcept]("array_except"),
    expression[ArrayUnion]("array_union"),
    expression[ArrayCompact]("array_compact"),
    expression[CreateMap]("map"),
    expression[CreateNamedStruct]("named_struct"),
    expression[ElementAt]("element_at"),
    expression[MapContainsKey]("map_contains_key"),
    expression[MapFromArrays]("map_from_arrays"),
    expression[MapKeys]("map_keys"),
    expression[MapValues]("map_values"),
    expression[MapEntries]("map_entries"),
    expression[MapFromEntries]("map_from_entries"),
    expression[MapConcat]("map_concat"),
    expression[Size]("size"),
    expression[Slice]("slice"),
    expression[Size]("cardinality", true),
    expression[ArraysZip]("arrays_zip"),
    expression[SortArray]("sort_array"),
    expression[Shuffle]("shuffle"),
    expression[ArrayMin]("array_min"),
    expression[ArrayMax]("array_max"),
    expression[ArrayAppend]("array_append"),
    expression[Reverse]("reverse"),
    expression[Concat]("concat"),
    expression[Flatten]("flatten"),
    expression[Sequence]("sequence"),
    expression[ArrayRepeat]("array_repeat"),
    expression[ArrayRemove]("array_remove"),
    expression[ArrayPrepend]("array_prepend"),
    expression[ArrayDistinct]("array_distinct"),
    expression[ArrayTransform]("transform"),
    expression[MapFilter]("map_filter"),
    expression[ArrayFilter]("filter"),
    expression[ArrayExists]("exists"),
    expression[ArrayForAll]("forall"),
    expression[ArrayAggregate]("aggregate"),
    expression[ArrayAggregate]("reduce", setAlias = true, Some("3.4.0")),
    expression[TransformValues]("transform_values"),
    expression[TransformKeys]("transform_keys"),
    expression[MapZipWith]("map_zip_with"),
    expression[ZipWith]("zip_with"),
    expression[Get]("get"),

    CreateStruct.registryEntry,

    // misc functions
    expression[AssertTrue]("assert_true"),
    expression[RaiseError]("raise_error"),
    expression[Crc32]("crc32"),
    expression[Md5]("md5"),
    expression[Uuid]("uuid"),
    expression[Murmur3Hash]("hash"),
    expression[XxHash64]("xxhash64"),
    expression[Sha1]("sha", true),
    expression[Sha1]("sha1"),
    expression[Sha2]("sha2"),
    expression[AesEncrypt]("aes_encrypt"),
    expression[AesDecrypt]("aes_decrypt"),
    expression[SparkPartitionID]("spark_partition_id"),
    expression[InputFileName]("input_file_name"),
    expression[InputFileBlockStart]("input_file_block_start"),
    expression[InputFileBlockLength]("input_file_block_length"),
    expression[MonotonicallyIncreasingID]("monotonically_increasing_id"),
    expression[CurrentDatabase]("current_database"),
    expression[CurrentDatabase]("current_schema", true),
    expression[CurrentCatalog]("current_catalog"),
    expression[CurrentUser]("current_user"),
    expression[CurrentUser]("user", setAlias = true),
    expression[CallMethodViaReflection]("reflect"),
    expression[CallMethodViaReflection]("java_method", true),
    expression[SparkVersion]("version"),
    expression[TypeOf]("typeof"),
    expression[EqualNull]("equal_null"),
    expression[HllSketchEstimate]("hll_sketch_estimate"),
    expression[HllUnion]("hll_union"),

    // grouping sets
    expression[Grouping]("grouping"),
    expression[GroupingID]("grouping_id"),

    // window functions
    expression[Lead]("lead"),
    expression[Lag]("lag"),
    expression[RowNumber]("row_number"),
    expression[CumeDist]("cume_dist"),
    expression[NthValue]("nth_value"),
    expression[NTile]("ntile"),
    expression[Rank]("rank"),
    expression[DenseRank]("dense_rank"),
    expression[PercentRank]("percent_rank"),

    // predicates
    expression[And]("and"),
    expression[In]("in"),
    expression[Not]("not"),
    expression[Or]("or"),

    // comparison operators
    expression[EqualNullSafe]("<=>"),
    expression[EqualTo]("="),
    expression[EqualTo]("=="),
    expression[GreaterThan](">"),
    expression[GreaterThanOrEqual](">="),
    expression[LessThan]("<"),
    expression[LessThanOrEqual]("<="),
    expression[Not]("!"),

    // bitwise
    expression[BitwiseAnd]("&"),
    expression[BitwiseNot]("~"),
    expression[BitwiseOr]("|"),
    expression[BitwiseXor]("^"),
    expression[BitwiseCount]("bit_count"),
    expression[BitAndAgg]("bit_and"),
    expression[BitOrAgg]("bit_or"),
    expression[BitXorAgg]("bit_xor"),
    expression[BitwiseGet]("bit_get"),
    expression[BitwiseGet]("getbit", true),

    // bitmap functions and aggregates
    expression[BitmapBucketNumber]("bitmap_bucket_number"),
    expression[BitmapBitPosition]("bitmap_bit_position"),
    expression[BitmapConstructAgg]("bitmap_construct_agg"),
    expression[BitmapCount]("bitmap_count"),
    expression[BitmapOrAgg]("bitmap_or_agg"),

    // json
    expression[StructsToJson]("to_json"),
    expression[JsonToStructs]("from_json"),
    expression[SchemaOfJson]("schema_of_json"),
    expression[LengthOfJsonArray]("json_array_length"),
    expression[JsonObjectKeys]("json_object_keys"),

    // cast
    expression[Cast]("cast"),
    // Cast aliases (SPARK-16730)
    castAlias("boolean", BooleanType),
    castAlias("tinyint", ByteType),
    castAlias("smallint", ShortType),
    castAlias("int", IntegerType),
    castAlias("bigint", LongType),
    castAlias("float", FloatType),
    castAlias("double", DoubleType),
    castAlias("decimal", DecimalType.USER_DEFAULT),
    castAlias("date", DateType),
    castAlias("timestamp", TimestampType),
    castAlias("binary", BinaryType),
    castAlias("string", StringType),

    // mask functions
    expressionBuilder("mask", MaskExpressionBuilder),

    // csv
    expression[CsvToStructs]("from_csv"),
    expression[SchemaOfCsv]("schema_of_csv"),
    expression[StructsToCsv]("to_csv")
'''

# Known Restrictions in Gluten.
LOOKAROUND_UNSUPPORTED = 'Lookaround unsupported'
GLUTEN_RESTRICTIONS = {
    'regexp': LOOKAROUND_UNSUPPORTED,
    'regexp_like': LOOKAROUND_UNSUPPORTED,
    'rlike': LOOKAROUND_UNSUPPORTED,
    'regexp_extract': LOOKAROUND_UNSUPPORTED,
    'regexp_extract_all': LOOKAROUND_UNSUPPORTED,
    'regexp_replace': LOOKAROUND_UNSUPPORTED
}

SPARK_FUNCTION_GROUPS = {
    "agg_funcs", "array_funcs", "datetime_funcs",
    "json_funcs", "map_funcs", "window_funcs",
    "math_funcs", "conditional_funcs", "generator_funcs",
    "predicate_funcs", "string_funcs", "misc_funcs",
    "bitwise_funcs", "conversion_funcs", "csv_funcs",
}

# Function groups that are not listed in the spark doc.
spark_function_missing_groups = {
    'collection_funcs',
    'hash_funcs',
    'lambda_funcs',
    'struct_funcs',
    'url_funcs',
    'xml_funcs'
}

SPARK_FUNCTION_GROUPS = SPARK_FUNCTION_GROUPS.union(spark_function_missing_groups)

SCALAR_FUNCTION_GROUPS = {'array_funcs': "Array Functions",
                          'map_funcs': "Map Functions",
                          'datetime_funcs': "Date and Timestamp Functions",
                          'json_funcs': "JSON Functions",
                          'math_funcs': "Mathematical Functions",
                          'string_funcs': "String Functions",
                          'bitwise_funcs': "Bitwise Functions",
                          'conversion_funcs': "Conversion Functions",
                          'conditional_funcs': "Conditional Functions",
                          'predicate_funcs': "Predicate Functions",
                          'csv_funcs': "Csv Functions",
                          'misc_funcs': "Misc Functions",
                          'collection_funcs': "Collection Functions",
                          'hash_funcs': "Hash Functions",
                          'lambda_funcs': "Lambda Functions",
                          'struct_funcs': "Struct Functions",
                          'url_funcs': "URL Functions",
                          'xml_funcs': "XML Functions"}

FUNCTION_GROUPS = {'scalar': SCALAR_FUNCTION_GROUPS,
                   'agg': {'agg_funcs': 'Aggregate Functions'},
                   'window': {'window_funcs': 'Window Functions'},
                   'generator': {'generator_funcs': "Generator Functions"}}


def create_spark_function_map():
    exprs = list(map(lambda x: x if x[-1] != ',' else x[:-1],
                     map(lambda x: x.strip(),
                         filter(lambda x: 'expression' in x, SPARK35_EXPRESSION_MAPPINGS.split('\n')))))

    func_map = {}
    expression_pattern = 'expression[GeneratorOuter]*\[([\w0-9]+)\]\("([^\s]+)".*'
    expression_builder_pattern = 'expression[Generator]*Builder[Outer]*\("([^\s]+)", ([\w0-9]+).*'
    for r in exprs:
        match = re.search(expression_pattern, r)

        if match:
            class_name = match.group(1)
            function_name = match.group(2)
            func_map[function_name] = class_name
        else:
            match = re.search(expression_builder_pattern, r)

            if match:
                class_name = match.group(2)
                function_name = match.group(1)
                func_map[function_name] = class_name
            else:
                logging.log(logging.WARNING, f'Could not parse expression: {r}')

    return func_map


def generate_function_list():
    jinfos = jvm.org.apache.spark.sql.api.python.PythonSQLUtils.listBuiltinFunctionInfos()

    infos = [["!=", '', 'predicate_funcs'], ["<>", "", "predicate_funcs"], ['between', '', 'predicate_funcs'],
             ['case', '', 'predicate_funcs'], ["||", '', 'misc_funcs']]
    for jinfo in filter(lambda x: x.getGroup() in SPARK_FUNCTION_GROUPS, jinfos):
        infos.append([jinfo.getName(), jinfo.getClassName().split('.')[-1], jinfo.getGroup()])

    for info in infos:
        name, classname, groupname = info
        if (name == "raise_error"):
            continue

        all_function_names.append(name)
        classname_to_function[classname] = name
        function_to_classname[name] = classname

        if groupname not in group_functions:
            group_functions[groupname] = []

        group_functions[groupname].append(name)
        if groupname in SCALAR_FUNCTION_GROUPS:
            scalar_functions.append(name)
        elif groupname == 'agg_funcs':
            agg_functions.append(name)
        elif groupname == 'window_funcs':
            window_functions.append(name)
        elif groupname == 'generator_funcs':
            generator_functions.append(name)
        else:
            logging.log(logging.WARNING, f"No matching group name for function {name}: " + groupname)


def parse_logs(log_file):
    generator_functions = ['explode', 'explode_outer', 'inline', 'inline_outer', 'posexplode', 'posexplode_outer',
                           'stack']

    scalar_support_list = {'partial': set(), 'unsupported': set()}
    agg_support_list = {'partial': set(), 'unsupported': set()}
    window_support_list = {'partial': set(), 'unsupported': set()}
    generator_support_list = {'partial': set(), 'unsupported': set()}
    try_to_binary_funcs = set(['unhex', 'encode', 'unbase64'])

    unresolved = []

    def filter_fallback_reasons():
        f = open(log_file, 'r')
        lines = f.readlines()
        lines
        ll = []

        # Filter validation logs.
        for l in lines:
            if (
                    'Validation failed for plan:' in l or 'Validation failed due to' in l or 'Validation failed at file' in l or l.startswith(
                ' - ') or l.startswith('   |- ')) and 'Native validation failed:' not in l:
                ll.append(l)

        # Extract fallback reasons.
        al = []
        for l in ll:
            if 'due to:' in l:
                al.append(l.split('due to:')[-1].strip())
            elif 'reason:' in l:
                al.append(l.split('reason:')[-1].strip())
            else:
                al.append(l)
        al = sorted(set(al))

        # Remove udf.
        return list(filter(lambda x: 'Not supported python udf' not in x and 'Not supported scala udf' not in x, al))

    def function_name_tuple(function_name):
        return (
            function_name, None if function_name not in function_to_classname else function_to_classname[function_name])

    def notFound(r):
        logging.log(logging.WARNING, f"No function name or class name found in: {r}")
        unresolved.append(r)

    java_import(jvm, "org.apache.gluten.expression.ExpressionMappings")
    jexpression_mappings = jvm.org.apache.gluten.expression.ExpressionMappings.listExpressionMappings()
    gluten_expressions = {}
    for item in jexpression_mappings:
        gluten_expressions[item._1()] = item._2()

    for f in scalar_functions:
        if f not in gluten_expressions.values() and function_to_classname[f] not in gluten_expressions.keys():
            scalar_support_list['unsupported'].add(function_name_tuple(f))

    for f in GLUTEN_RESTRICTIONS.keys():
        scalar_support_list['partial'].add(function_name_tuple(f))

    for r in filter_fallback_reasons():
        if 'Not supported to map spark function name to substrait function name' in r:
            pattern = r"class name: ([\w0-9]+)."

            # Extract class name
            match = re.search(pattern, r)

            if match:
                class_name = match.group(1)
                if class_name in classname_to_function:
                    scalar_support_list['unsupported'].add((classname_to_function[class_name], class_name))
                else:
                    logging.log(logging.INFO, f"No function name for class: {class_name}. Adding class name")
                    scalar_support_list['unsupported'].add((None, class_name))
            else:
                notFound(r)

        elif 'Not support expression' in r:
            pattern = r"Not support expression ([\w0-9]+)"

            # Extract class name
            match = re.search(pattern, r)

            if match:
                class_name = match.group(1)
                if class_name in classname_to_function:
                    scalar_support_list['unsupported'].add((classname_to_function[class_name], class_name))
                else:
                    logging.log(logging.INFO, f"No function name for class: {class_name}. Adding class name")
                    scalar_support_list['unsupported'].add((None, class_name))
            else:
                notFound(r)

        elif 'Scalar function name not registered:' in r:
            pattern = r"Scalar function name not registered:\s+([\w0-9]+)"

            # Extract the function name
            match = re.search(pattern, r)

            if match:
                function_name = match.group(1)
                scalar_support_list['unsupported'].add(function_name_tuple(function_name))
            else:
                notFound(r)

        elif 'Function is not supported:' in r:
            pattern = r"Function is not supported:\s+([\w0-9]+)"

            # Extract the function name
            match = re.search(pattern, r)

            if match:
                function_name = match.group(1)
                scalar_support_list['unsupported'].add(function_name_tuple(function_name))
            else:
                notFound(r)

        elif 'not registered with arguments:' in r:
            pattern = r"Scalar function ([\w0-9]+) not registered with arguments:"

            # Extract the function name
            match = re.search(pattern, r)

            if match:
                function_name = match.group(1)
                scalar_support_list['partial'].add(function_name_tuple(function_name))
            else:
                notFound(r)

        elif 'Could not find a valid substrait mapping' in r:
            pattern = r"Could not find a valid substrait mapping name for ([\w0-9]+)\("

            # Extract the function name
            match = re.search(pattern, r)

            if match:
                function_name = match.group(1)
                agg_support_list['unsupported'].add(function_name_tuple(function_name))
            else:
                notFound(r)

        elif 'Velox backend does not support this generator:' in r:
            pattern = r"Velox backend does not support this generator:\s+([\w0-9]+)"

            # Extract the function name
            match = re.search(pattern, r)

            if match:
                class_name = match.group(1)
                function_name = class_name.lower()
                if function_name not in generator_functions:
                    generator_support_list['unsupported'].add((None, class_name))
                elif 'outer: true' in r:
                    generator_support_list['unsupported'].add((function_name + '_outer', None))
                else:
                    generator_support_list['unsupported'].add(function_name_tuple(function_name))
            else:
                notFound(r)

        elif 'try_eval' in r and ' is not supported' in r:
            pattern = r"try_eval\((\w+)\) is not supported"
            match = re.search(pattern, r)

            if match:
                function_name = match.group(1)
                if function_name in try_to_binary_funcs:
                    try_to_binary_funcs.remove(function_name)

                    function_name = 'try_to_binary'
                    p = function_name_tuple(function_name)
                    if len(try_to_binary_funcs) == 0:
                        if p in scalar_support_list['partial']:
                            scalar_support_list['partial'].remove(p)
                        scalar_support_list['unsupported'].add(p)

                elif 'add' in function_name:
                    function_name = 'try_add'
                    scalar_support_list['partial'].add(function_name_tuple(function_name))
            else:
                notFound(r)

        elif 'Pattern is not string literal for regexp_extract' == r:
            function_name = 'regexp_extract'
            scalar_support_list['partial'].add(function_name_tuple(function_name))

        elif 'Pattern is not string literal for regexp_extract_all' == r:
            function_name = 'regexp_extract_all'
            scalar_support_list['partial'].add(function_name_tuple(function_name))

        else:
            unresolved.append(r)

    return scalar_support_list, agg_support_list, window_support_list, generator_support_list, unresolved


def generate_function_doc(category, function_support_list, output):
    num_unsupported = len(list(filter(lambda x: x[0] is not None, function_support_list['unsupported'])))
    num_unsupported_expression = len(
        list(filter(lambda x: x[0] is None and x[1] is not None, function_support_list['unsupported'])))
    num_partially_supported = len(list(filter(lambda x: x[0] is not None, function_support_list['partial'])))
    num_supported = len(scalar_functions) - num_unsupported - num_partially_supported

    logging.log(logging.WARNING, f'Number of {category} functions: {len(scalar_functions)}')
    logging.log(logging.WARNING, f'Number of unsupported {category} functions: {num_unsupported}')
    logging.log(logging.WARNING, f'Number of unsupported {category} expressions: {num_unsupported_expression}')
    logging.log(logging.WARNING, f'Number of partially supported {category} function: {num_partially_supported}')
    logging.log(logging.WARNING, f'Number of fully supported {category} function: {num_supported}')

    headers = ['Spark Functions', 'Spark Expressions', 'Status', 'Restrictions']

    lines = f'''# {category.capitalize()} Functions Support Status

**Out of {len(scalar_functions)} {category} functions in Spark 3.5, Gluten currently fully supports {num_supported} functions and partially supports {num_partially_supported} functions.**

'''

    for g in sorted(SPARK_FUNCTION_GROUPS):
        if g in FUNCTION_GROUPS[category]:
            lines += '## ' + FUNCTION_GROUPS[category][g] + '\n\n'
            data = []
            for f in sorted(group_functions[g]):
                classname = '' if f not in spark_function_map else spark_function_map[f]
                support = None
                for item in function_support_list['partial']:
                    if item[0] and item[0] == f or item[1] and item[1] == classname:
                        support = 'PS'
                        break
                if support is None:
                    for item in function_support_list['unsupported']:
                        if item[0] and item[0] == f or item[1] and item[1] == classname:
                            support = ''
                            break
                if support is None:
                    support = 'S'
                if f == '|':
                    f = '&#124;'
                elif f == '||':
                    f = '&#124;&#124;'
                data.append([f, classname, support, '' if f not in GLUTEN_RESTRICTIONS else GLUTEN_RESTRICTIONS[f]])
            table = tabulate.tabulate(data, headers, tablefmt="github")
            lines += table + '\n\n'

    with open(output, 'w') as fd:
        fd.write(lines)


def run_GlutenSQLQueryTestSuite():
    log4j_properties_file = os.path.abspath(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), 'log4j2.properties'))
    command = [
        "mvn", "test",
        "-Pspark-3.5", "-Pspark-ut", "-Pbackends-velox",
        f"-DargLine=-Dspark.test.home={spark_home} -Dlog4j2.configurationFile=file:{log4j_properties_file}",
        "-DwildcardSuites=org.apache.spark.sql.GlutenSQLQueryTestSuite",
        "-Dtest=none",
        "-Dsurefire.failIfNoSpecifiedTests=false"
    ]

    subprocess.Popen(command, cwd=gluten_home).wait()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--spark_home", type=str, required=True,
                        help="Directory to spark source code for the newest supported spark version in Gluten. "
                             "It's required the spark project has been built from source.")
    parser.add_argument("--skip_run_test_suite", action='store_true',
                        help="Whether to run test suite. Set to False to skip running the test suite.")
    args = parser.parse_args()

    spark_home = args.spark_home
    findspark.init(spark_home)

    gluten_home = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../'))
    if not args.skip_run_test_suite:
        run_GlutenSQLQueryTestSuite()

    gluten_jar = os.path.join(gluten_home, 'package', 'target', 'gluten-package-1.4.0-SNAPSHOT.jar')
    if not os.path.exists(gluten_jar):
        raise Exception(f"Gluten jar not found at {gluten_jar}")

    # Importing the required modules after findspark.
    from py4j.java_gateway import java_import
    from pyspark.java_gateway import launch_gateway
    from pyspark.conf import SparkConf

    conf = SparkConf().set("spark.jars", gluten_jar)
    jvm = launch_gateway(conf=conf).jvm

    # Generate the function list to the global variables.
    all_function_names = []
    scalar_functions = []
    agg_functions = []
    window_functions = []
    generator_functions = []
    classname_to_function = {}
    function_to_classname = {}
    group_functions = {}
    generate_function_list()

    spark_function_map = create_spark_function_map()

    scalar_support_list, agg_support_list, window_support_list, generator_support_list, unresolved = parse_logs(
        os.path.join(gluten_home, 'gluten-ut', 'spark35', 'target', 'gen-function-support-docs-tests.log'))

    generate_function_doc('scalar', scalar_support_list,
                          os.path.join(gluten_home, 'docs', 'velox-backend-scalar-function-support.md'))

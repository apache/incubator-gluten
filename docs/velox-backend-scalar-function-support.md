# Scalar Functions Support Status

**Out of 352 scalar functions in Spark 3.5, Gluten currently fully supports 229 functions and partially supports 13 functions.**

## URL Functions

| Spark Functions   | Spark Expressions   | Status   | Restrictions   |
|-------------------|---------------------|----------|----------------|
| parse_url         | ParseUrl            |          |                |
| url_decode        | UrlDecode           | PS       |                |
| url_encode        | UrlEncode           | PS       |                |

## Bitwise Functions

| Spark Functions    | Spark Expressions   | Status   | Restrictions   |
|--------------------|---------------------|----------|----------------|
| &                  | BitwiseAnd          | S        |                |
| ^                  | BitwiseXor          | S        |                |
| bit_count          | BitwiseCount        | S        |                |
| bit_get            | BitwiseGet          | S        |                |
| getbit             | BitwiseGet          | S        |                |
| shiftright         | ShiftRight          | S        |                |
| shiftrightunsigned | ShiftRightUnsigned  | S        |                |
| &#124;             | BitwiseOr           | S        |                |
| ~                  | BitwiseNot          | S        |                |

## Misc Functions

| Spark Functions             | Spark Expressions         | Status   | Restrictions   |
|-----------------------------|---------------------------|----------|----------------|
| aes_decrypt                 | AesDecrypt                |          |                |
| aes_encrypt                 | AesEncrypt                |          |                |
| assert_true                 | AssertTrue                |          |                |
| bitmap_bit_position         | BitmapBitPosition         |          |                |
| bitmap_bucket_number        | BitmapBucketNumber        |          |                |
| bitmap_count                | BitmapCount               |          |                |
| current_catalog             | CurrentCatalog            |          |                |
| current_database            | CurrentDatabase           |          |                |
| current_schema              | CurrentDatabase           |          |                |
| current_user                | CurrentUser               |          |                |
| equal_null                  | EqualNull                 |          |                |
| hll_sketch_estimate         | HllSketchEstimate         |          |                |
| hll_union                   | HllUnion                  |          |                |
| input_file_block_length     | InputFileBlockLength      |          |                |
| input_file_block_start      | InputFileBlockStart       |          |                |
| input_file_name             | InputFileName             |          |                |
| java_method                 | CallMethodViaReflection   |          |                |
| monotonically_increasing_id | MonotonicallyIncreasingID |          |                |
| reflect                     | CallMethodViaReflection   |          |                |
| spark_partition_id          | SparkPartitionID          | S        |                |
| try_aes_decrypt             | TryAesDecrypt             |          |                |
| typeof                      | TypeOf                    |          |                |
| user                        | CurrentUser               |          |                |
| uuid                        | Uuid                      | S        |                |
| version                     | SparkVersion              | S        |                |
| &#124;&#124;                |                           |          |                |

## Mathematical Functions

| Spark Functions   | Spark Expressions      | Status   | Restrictions   |
|-------------------|------------------------|----------|----------------|
| %                 | Remainder              | S        |                |
| *                 | Multiply               | S        |                |
| +                 | Add                    | S        |                |
| -                 | Subtract               | S        |                |
| /                 | Divide                 | S        |                |
| abs               | Abs                    | S        |                |
| acos              | Acos                   | S        |                |
| acosh             | Acosh                  | S        |                |
| asin              | Asin                   | S        |                |
| asinh             | Asinh                  | S        |                |
| atan              | Atan                   | S        |                |
| atan2             | Atan2                  | S        |                |
| atanh             | Atanh                  | S        |                |
| bin               | Bin                    | S        |                |
| bround            | BRound                 |          |                |
| cbrt              | Cbrt                   | S        |                |
| ceil              | CeilExpressionBuilder  | PS       |                |
| ceiling           | CeilExpressionBuilder  | PS       |                |
| conv              | Conv                   | S        |                |
| cos               | Cos                    | S        |                |
| cosh              | Cosh                   | S        |                |
| cot               | Cot                    | S        |                |
| csc               | Csc                    | S        |                |
| degrees           | ToDegrees              | S        |                |
| div               | IntegralDivide         |          |                |
| e                 | EulerNumber            | S        |                |
| exp               | Exp                    | S        |                |
| expm1             | Expm1                  | S        |                |
| factorial         | Factorial              | S        |                |
| floor             | FloorExpressionBuilder | PS       |                |
| greatest          | Greatest               | S        |                |
| hex               | Hex                    | S        |                |
| hypot             | Hypot                  | S        |                |
| least             | Least                  | S        |                |
| ln                | Log                    | S        |                |
| log               | Logarithm              | S        |                |
| log10             | Log10                  | S        |                |
| log1p             | Log1p                  | S        |                |
| log2              | Log2                   | S        |                |
| mod               | Remainder              | S        |                |
| negative          | UnaryMinus             | S        |                |
| pi                | Pi                     | S        |                |
| pmod              | Pmod                   | S        |                |
| positive          | UnaryPositive          | S        |                |
| pow               | Pow                    | S        |                |
| power             | Pow                    | S        |                |
| radians           | ToRadians              | S        |                |
| rand              | Rand                   | S        |                |
| randn             | Randn                  |          |                |
| random            | Rand                   | S        |                |
| rint              | Rint                   | S        |                |
| round             | Round                  | S        |                |
| sec               | Sec                    | S        |                |
| shiftleft         | ShiftLeft              | S        |                |
| sign              | Signum                 | S        |                |
| signum            | Signum                 | S        |                |
| sin               | Sin                    | S        |                |
| sinh              | Sinh                   | S        |                |
| sqrt              | Sqrt                   | S        |                |
| tan               | Tan                    | S        |                |
| tanh              | Tanh                   | S        |                |
| try_add           | TryAdd                 | PS       |                |
| try_divide        | TryDivide              |          |                |
| try_multiply      | TryMultiply            |          |                |
| try_subtract      | TrySubtract            |          |                |
| unhex             | Unhex                  | S        |                |
| width_bucket      | WidthBucket            | S        |                |

## Predicate Functions

| Spark Functions   | Spark Expressions   | Status   | Restrictions           |
|-------------------|---------------------|----------|------------------------|
| !                 | Not                 | S        |                        |
| !=                |                     |          |                        |
| <                 | LessThan            | S        |                        |
| <=                | LessThanOrEqual     | S        |                        |
| <=>               | EqualNullSafe       | S        |                        |
| <>                |                     |          |                        |
| =                 | EqualTo             | S        |                        |
| ==                | EqualTo             | S        |                        |
| >                 | GreaterThan         | S        |                        |
| >=                | GreaterThanOrEqual  | S        |                        |
| and               | And                 | S        |                        |
| between           |                     |          |                        |
| case              |                     |          |                        |
| ilike             | ILike               |          |                        |
| in                | In                  | PS       |                        |
| isnan             | IsNaN               | S        |                        |
| isnotnull         | IsNotNull           | S        |                        |
| isnull            | IsNull              | S        |                        |
| like              | Like                | S        |                        |
| not               | Not                 | S        |                        |
| or                | Or                  | S        |                        |
| regexp            | RLike               | PS       | Lookaround unsupported |
| regexp_like       | RLike               | PS       | Lookaround unsupported |
| rlike             | RLike               | PS       | Lookaround unsupported |

## Date and Timestamp Functions

| Spark Functions     | Spark Expressions                    | Status   | Restrictions   |
|---------------------|--------------------------------------|----------|----------------|
| add_months          | AddMonths                            | S        |                |
| convert_timezone    | ConvertTimezone                      |          |                |
| curdate             | CurDateExpressionBuilder             |          |                |
| current_date        | CurrentDate                          |          |                |
| current_timestamp   | CurrentTimestamp                     |          |                |
| current_timezone    | CurrentTimeZone                      |          |                |
| date_add            | DateAdd                              | S        |                |
| date_diff           | DateDiff                             | S        |                |
| date_format         | DateFormatClass                      | S        |                |
| date_from_unix_date | DateFromUnixDate                     | S        |                |
| date_part           | DatePartExpressionBuilder            |          |                |
| date_sub            | DateSub                              | S        |                |
| date_trunc          | TruncTimestamp                       | S        |                |
| dateadd             | DateAdd                              | S        |                |
| datediff            | DateDiff                             | S        |                |
| datepart            | DatePartExpressionBuilder            |          |                |
| day                 | DayOfMonth                           | S        |                |
| dayofmonth          | DayOfMonth                           | S        |                |
| dayofweek           | DayOfWeek                            | S        |                |
| dayofyear           | DayOfYear                            | S        |                |
| extract             | Extract                              | S        |                |
| from_unixtime       | FromUnixTime                         | S        |                |
| from_utc_timestamp  | FromUTCTimestamp                     | S        |                |
| hour                | Hour                                 | S        |                |
| last_day            | LastDay                              | S        |                |
| localtimestamp      | LocalTimestamp                       |          |                |
| make_date           | MakeDate                             | S        |                |
| make_dt_interval    | MakeDTInterval                       |          |                |
| make_interval       | MakeInterval                         |          |                |
| make_timestamp      | MakeTimestamp                        | S        |                |
| make_timestamp_ltz  | MakeTimestampLTZExpressionBuilder    |          |                |
| make_timestamp_ntz  | MakeTimestampNTZExpressionBuilder    |          |                |
| make_ym_interval    | MakeYMInterval                       | S        |                |
| minute              | Minute                               | S        |                |
| month               | Month                                | S        |                |
| months_between      | MonthsBetween                        | S        |                |
| next_day            | NextDay                              | S        |                |
| now                 | Now                                  |          |                |
| quarter             | Quarter                              | S        |                |
| second              | Second                               | S        |                |
| session_window      | SessionWindow                        |          |                |
| timestamp_micros    | MicrosToTimestamp                    | S        |                |
| timestamp_millis    | MillisToTimestamp                    | S        |                |
| timestamp_seconds   | SecondsToTimestamp                   |          |                |
| to_date             | ParseToDate                          |          |                |
| to_timestamp        | ParseToTimestamp                     |          |                |
| to_timestamp_ltz    | ParseToTimestampLTZExpressionBuilder |          |                |
| to_timestamp_ntz    | ParseToTimestampNTZExpressionBuilder |          |                |
| to_unix_timestamp   | ToUnixTimestamp                      | S        |                |
| to_utc_timestamp    | ToUTCTimestamp                       | S        |                |
| trunc               | TruncDate                            |          |                |
| try_to_timestamp    | TryToTimestampExpressionBuilder      |          |                |
| unix_date           | UnixDate                             | S        |                |
| unix_micros         | UnixMicros                           | S        |                |
| unix_millis         | UnixMillis                           | S        |                |
| unix_seconds        | UnixSeconds                          | S        |                |
| unix_timestamp      | UnixTimestamp                        | S        |                |
| weekday             | WeekDay                              | S        |                |
| weekofyear          | WeekOfYear                           | S        |                |
| window              | TimeWindow                           |          |                |
| window_time         | WindowTime                           |          |                |
| year                | Year                                 | S        |                |

## Map Functions

| Spark Functions   | Spark Expressions   | Status   | Restrictions   |
|-------------------|---------------------|----------|----------------|
| element_at        | ElementAt           | S        |                |
| map               | CreateMap           | PS       |                |
| map_concat        | MapConcat           |          |                |
| map_contains_key  | MapContainsKey      |          |                |
| map_entries       | MapEntries          | S        |                |
| map_from_arrays   | MapFromArrays       | S        |                |
| map_from_entries  | MapFromEntries      |          |                |
| map_keys          | MapKeys             | S        |                |
| map_values        | MapValues           | S        |                |
| str_to_map        | StringToMap         | S        |                |
| try_element_at    | TryElementAt        |          |                |

## String Functions

| Spark Functions    | Spark Expressions           | Status   | Restrictions           |
|--------------------|-----------------------------|----------|------------------------|
| ascii              | Ascii                       | S        |                        |
| base64             | Base64                      | S        |                        |
| bit_length         | BitLength                   | S        |                        |
| btrim              | StringTrimBoth              |          |                        |
| char               | Chr                         | S        |                        |
| char_length        | Length                      | S        |                        |
| character_length   | Length                      | S        |                        |
| chr                | Chr                         | S        |                        |
| concat_ws          | ConcatWs                    | S        |                        |
| contains           | ContainsExpressionBuilder   | S        |                        |
| decode             | Decode                      |          |                        |
| elt                | Elt                         |          |                        |
| encode             | Encode                      |          |                        |
| endswith           | EndsWithExpressionBuilder   |          |                        |
| find_in_set        | FindInSet                   | S        |                        |
| format_number      | FormatNumber                |          |                        |
| format_string      | FormatString                | S        |                        |
| initcap            | InitCap                     | S        |                        |
| instr              | StringInstr                 | S        |                        |
| lcase              | Lower                       | S        |                        |
| left               | Left                        | S        |                        |
| len                | Length                      | S        |                        |
| length             | Length                      | S        |                        |
| levenshtein        | Levenshtein                 | S        |                        |
| locate             | StringLocate                | S        |                        |
| lower              | Lower                       | S        |                        |
| lpad               | LPadExpressionBuilder       | S        |                        |
| ltrim              | StringTrimLeft              | S        |                        |
| luhn_check         | Luhncheck                   |          |                        |
| mask               | MaskExpressionBuilder       | S        |                        |
| octet_length       | OctetLength                 |          |                        |
| overlay            | Overlay                     | S        |                        |
| position           | StringLocate                | S        |                        |
| printf             | FormatString                | S        |                        |
| regexp_count       | RegExpCount                 |          |                        |
| regexp_extract     | RegExpExtract               | PS       | Lookaround unsupported |
| regexp_extract_all | RegExpExtractAll            | PS       | Lookaround unsupported |
| regexp_instr       | RegExpInStr                 |          |                        |
| regexp_replace     | RegExpReplace               | PS       | Lookaround unsupported |
| regexp_substr      | RegExpSubStr                |          |                        |
| repeat             | StringRepeat                | S        |                        |
| replace            | StringReplace               | S        |                        |
| right              | Right                       |          |                        |
| rpad               | RPadExpressionBuilder       | S        |                        |
| rtrim              | StringTrimRight             | S        |                        |
| sentences          | Sentences                   |          |                        |
| soundex            | SoundEx                     | S        |                        |
| space              | StringSpace                 | S        |                        |
| split              | StringSplit                 | S        |                        |
| split_part         | SplitPart                   | S        |                        |
| startswith         | StartsWithExpressionBuilder |          |                        |
| substr             | Substring                   | S        |                        |
| substring          | Substring                   | S        |                        |
| substring_index    | SubstringIndex              | S        |                        |
| to_binary          | ToBinary                    |          |                        |
| to_char            | ToCharacter                 |          |                        |
| to_number          | ToNumber                    |          |                        |
| to_varchar         | ToCharacter                 |          |                        |
| translate          | StringTranslate             | S        |                        |
| trim               | StringTrim                  | S        |                        |
| try_to_binary      | TryToBinary                 |          |                        |
| try_to_number      | TryToNumber                 |          |                        |
| ucase              | Upper                       | S        |                        |
| unbase64           | UnBase64                    |          |                        |
| upper              | Upper                       | S        |                        |

## Csv Functions

| Spark Functions   | Spark Expressions   | Status   | Restrictions   |
|-------------------|---------------------|----------|----------------|
| from_csv          | CsvToStructs        |          |                |
| schema_of_csv     | SchemaOfCsv         |          |                |
| to_csv            | StructsToCsv        |          |                |

## Lambda Functions

| Spark Functions   | Spark Expressions   | Status   | Restrictions   |
|-------------------|---------------------|----------|----------------|
| aggregate         | ArrayAggregate      | S        |                |
| array_sort        | ArraySort           | S        |                |
| exists            | ArrayExists         | S        |                |
| filter            | ArrayFilter         | S        |                |
| forall            | ArrayForAll         | S        |                |
| map_filter        | MapFilter           |          |                |
| map_zip_with      | MapZipWith          | S        |                |
| reduce            | ArrayAggregate      | S        |                |
| transform         | ArrayTransform      | S        |                |
| transform_keys    | TransformKeys       | S        |                |
| transform_values  | TransformValues     | S        |                |
| zip_with          | ZipWith             | S        |                |

## Array Functions

| Spark Functions   | Spark Expressions   | Status   | Restrictions   |
|-------------------|---------------------|----------|----------------|
| array             | CreateArray         | S        |                |
| array_append      | ArrayAppend         |          |                |
| array_compact     | ArrayCompact        |          |                |
| array_contains    | ArrayContains       | S        |                |
| array_distinct    | ArrayDistinct       | S        |                |
| array_except      | ArrayExcept         | S        |                |
| array_insert      | ArrayInsert         | S        |                |
| array_intersect   | ArrayIntersect      | S        |                |
| array_join        | ArrayJoin           | S        |                |
| array_max         | ArrayMax            | S        |                |
| array_min         | ArrayMin            | S        |                |
| array_position    | ArrayPosition       | S        |                |
| array_prepend     | ArrayPrepend        |          |                |
| array_remove      | ArrayRemove         | S        |                |
| array_repeat      | ArrayRepeat         | S        |                |
| array_union       | ArrayUnion          |          |                |
| arrays_overlap    | ArraysOverlap       | S        |                |
| arrays_zip        | ArraysZip           | S        |                |
| flatten           | Flatten             | S        |                |
| get               | Get                 |          |                |
| sequence          | Sequence            | S        |                |
| shuffle           | Shuffle             | S        |                |
| slice             | Slice               | S        |                |
| sort_array        | SortArray           | S        |                |

## Conversion Functions

| Spark Functions   | Spark Expressions   | Status   | Restrictions   |
|-------------------|---------------------|----------|----------------|
| bigint            |                     | S        |                |
| binary            |                     | S        |                |
| boolean           |                     | S        |                |
| cast              | Cast                | S        |                |
| date              |                     | S        |                |
| decimal           |                     | S        |                |
| double            |                     | S        |                |
| float             |                     | S        |                |
| int               |                     | S        |                |
| smallint          |                     | S        |                |
| string            |                     | S        |                |
| timestamp         |                     | S        |                |
| tinyint           |                     | S        |                |

## Hash Functions

| Spark Functions   | Spark Expressions   | Status   | Restrictions   |
|-------------------|---------------------|----------|----------------|
| crc32             | Crc32               | S        |                |
| hash              | Murmur3Hash         | S        |                |
| md5               | Md5                 | S        |                |
| sha               | Sha1                | S        |                |
| sha1              | Sha1                | S        |                |
| sha2              | Sha2                | S        |                |
| xxhash64          | XxHash64            | S        |                |

## JSON Functions

| Spark Functions   | Spark Expressions   | Status   | Restrictions   |
|-------------------|---------------------|----------|----------------|
| from_json         | JsonToStructs       |          |                |
| get_json_object   | GetJsonObject       | S        |                |
| json_array_length | LengthOfJsonArray   |          |                |
| json_object_keys  | JsonObjectKeys      |          |                |
| json_tuple        | JsonTuple           | S        |                |
| schema_of_json    | SchemaOfJson        |          |                |
| to_json           | StructsToJson       |          |                |

## Collection Functions

| Spark Functions   | Spark Expressions   | Status   | Restrictions   |
|-------------------|---------------------|----------|----------------|
| array_size        | ArraySize           |          |                |
| cardinality       | Size                | S        |                |
| concat            | Concat              | S        |                |
| reverse           | Reverse             | S        |                |
| size              | Size                | S        |                |

## Struct Functions

| Spark Functions   | Spark Expressions   | Status   | Restrictions   |
|-------------------|---------------------|----------|----------------|
| named_struct      | CreateNamedStruct   | S        |                |
| struct            |                     | S        |                |

## Conditional Functions

| Spark Functions   | Spark Expressions   | Status   | Restrictions   |
|-------------------|---------------------|----------|----------------|
| coalesce          | Coalesce            | S        |                |
| if                | If                  | S        |                |
| ifnull            | Nvl                 |          |                |
| nanvl             | NaNvl               | S        |                |
| nullif            | NullIf              |          |                |
| nvl               | Nvl                 |          |                |
| nvl2              | Nvl2                |          |                |
| when              | CaseWhen            | S        |                |

## XML Functions

| Spark Functions   | Spark Expressions   | Status   | Restrictions   |
|-------------------|---------------------|----------|----------------|
| xpath             | XPathList           |          |                |
| xpath_boolean     | XPathBoolean        |          |                |
| xpath_double      | XPathDouble         |          |                |
| xpath_float       | XPathFloat          |          |                |
| xpath_int         | XPathInt            |          |                |
| xpath_long        | XPathLong           |          |                |
| xpath_number      | XPathDouble         |          |                |
| xpath_short       | XPathShort          |          |                |
| xpath_string      | XPathString         |          |                |


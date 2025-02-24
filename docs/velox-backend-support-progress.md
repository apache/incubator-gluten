---
layout: page
title: Velox Backend's Supported Operators & Functions
nav_order: 4
---

# The Operators and Functions Support Progress

Gluten is still under active development. Here is a list of supported operators and functions.

Since the same function may have different semantics between Presto and Spark, Velox implement the functions in Presto category, if we note a
different semantics from Spark, then the function is implemented in Spark category. So Gluten will first try to find function in Velox's spark
category, if a function isn't implemented then refer to Presto category.

We use some notations to describe the supporting status of operators/functions in the tables below, they are:

| Value        | Description                                                                              |
|--------------|------------------------------------------------------------------------------------------|
| S            | Supported. Gluten or Velox supports fully.                                               |
| S*           | Mark for foldable expression that will be converted to alias after spark's optimization. |
| [Blank Cell] | Not applicable case or needs to confirm.                                                 |
| PS           | Partial Support. Velox only partially supports it.                                       |
| NS           | Not Supported. Velox backend does not support it.                                        |

And also some notations for the function implementation's restrictions:

| Value      | Description                                                                                                                                                      |
|------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Mismatched | Some functions are implemented by Velox, but have different semantics from Apache Spark, we mark them as "Mismatched".                                           |
| ANSI OFF   | Gluten doesn't support [ANSI mode](https://spark.apache.org/docs/latest/sql-ref-ansi-compliance.html). If it is enabled, Gluten will fall back to Vanilla Spark. |

### Operator Map

Gluten supports 30+ operators (Drag to right to see all data types)

| Executor                    | Description                                                                                                                                                                 | Gluten Name                                            | Velox Name            | BOOLEAN | BYTE | SHORT | INT | LONG | FLOAT | DOUBLE | STRING | NULL | BINARY | ARRAY | MAP | STRUCT(ROW) | DATE | TIMESTAMP | DECIMAL | CALENDAR | UDT |
|-----------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------|-----------------------|---------|------|-------|-----|------|-------|--------|--------|------|--------|-------|-----|-------------|------|-----------|---------|----------|-----|
| FileSourceScanExec          | Reading data from files, often from Hive tables                                                                                                                             | FileSourceScanExecTransformer                          | TableScanNode         | S       | S    | S     | S   | S    | S     | S      | S      | S    | S      | NS    | NS  | NS          | S    | NS        | NS      | NS       | NS  |
| BatchScanExec               | The backend for most file input                                                                                                                                             | BatchScanExecTransformer                               | TableScanNode         | S       | S    | S     | S   | S    | S     | S      | S      | S    | S      | NS    | NS  | NS          | S    | NS        | NS      | NS       | NS  |
| FilterExec                  | The backend for most filter statements                                                                                                                                      | FilterExecTransformer                                  | FilterNode            | S       | S    | S     | S   | S    | S     | S      | S      | S    | S      | NS    | NS  | NS          | S    | NS        | NS      | NS       | NS  |
| ProjectExec                 | The backend for most select, withColumn and dropColumn statements                                                                                                           | ProjectExecTransformer                                 | ProjectNode           | S       | S    | S     | S   | S    | S     | S      | S      | S    | S      | NS    | NS  | NS          | S    | NS        | NS      | NS       | NS  |
| HashAggregateExec           | The backend for hash based aggregations                                                                                                                                     | HashAggregateBaseTransformer                           | AggregationNode       | S       | S    | S     | S   | S    | S     | S      | S      | S    | S      | NS    | NS  | NS          | S    | NS        | NS      | NS       | NS  |
| BroadcastHashJoinExec       | Implementation of join using broadcast data                                                                                                                                 | BroadcastHashJoinExecTransformer                       | HashJoinNode          | S       | S    | S     | S   | S    | S     | S      | S      | S    | S      | NS    | NS  | NS          | S    | NS        | NS      | NS       | NS  |
| ShuffledHashJoinExec        | Implementation of join using hashed shuffled data                                                                                                                           | ShuffleHashJoinExecTransformer                         | HashJoinNode          | S       | S    | S     | S   | S    | S     | S      | S      | S    | S      | NS    | NS  | NS          | S    | NS        | NS      | NS       | NS  |
| SortExec                    | The backend for the sort operator                                                                                                                                           | SortExecTransformer                                    | OrderByNode           | S       | S    | S     | S   | S    | S     | S      | S      | S    | S      | NS    | NS  | NS          | S    | NS        | NS      | NS       | NS  |
| SortMergeJoinExec           | Sort merge join, replacing with shuffled hash join                                                                                                                          | SortMergeJoinExecTransformer                           | MergeJoinNode         | S       | S    | S     | S   | S    | S     | S      | S      | S    | S      | NS    | NS  | NS          | S    | NS        | NS      | NS       | NS  |
| WindowExec                  | Window operator backend                                                                                                                                                     | WindowExecTransformer                                  | WindowNode            | S       | S    | S     | S   | S    | S     | S      | S      | S    | S      | NS    | NS  | NS          | S    | NS        | NS      | NS       | NS  |
| GlobalLimitExec             | Limiting of results across partitions                                                                                                                                       | LimitTransformer                                       | LimitNode             | S       | S    | S     | S   | S    | S     | S      | S      | S    | S      | NS    | NS  | NS          | S    | NS        | NS      | NS       | NS  |
| LocalLimitExec              | Per-partition limiting of results                                                                                                                                           | LimitTransformer                                       | LimitNode             | S       | S    | S     | S   | S    | S     | S      | S      | S    | S      | NS    | NS  | NS          | S    | NS        | NS      | NS       | NS  |
| ExpandExec                  | The backend for the expand operator                                                                                                                                         | ExpandExecTransformer                                  | GroupIdNode           | S       | S    | S     | S   | S    | S     | S      | S      | S    | S      | NS    | NS  | NS          | S    | NS        | NS      | NS       | NS  |
| UnionExec                   | The backend for the union operator                                                                                                                                          | UnionExecTransformer                                   | N                     | S       | S    | S     | S   | S    | S     | S      | S      | S    | S      | NS    | NS  | NS          | S    | NS        | NS      | NS       | NS  |
| DataWritingCommandExec      | Writing data                                                                                                                                                                | Y                                                      | TableWriteNode        | S       | S    | S     | S   | S    | S     | S      | S      | S    | S      | S     | NS  | S           | S    | NS        | S       | NS       | NS  |
| CartesianProductExec        | Implementation of join using brute force                                                                                                                                    | CartesianProductExecTransformer                        | NestedLoopJoinNode    | S       | S    | S     | S   | S    | S     | S      | S      | S    | S      | NS    | NS  | NS          | S    | NS        | NS      | NS       | NS  |
| ShuffleExchangeExec         | The backend for most data being exchanged between processes                                                                                                                 | ColumnarShuffleExchangeExec                            | ExchangeNode          | NS      | NS   | NS    | NS  | NS   | NS    | NS     | NS     | NS   | NS     | NS    | NS  | NS          | NS   | NS        | NS      | NS       | NS  |
|                             | The unnest operation expands arrays and maps into separate columns                                                                                                          | N                                                      | UnnestNode            | NS      | NS   | NS    | NS  | NS   | NS    | NS     | NS     | NS   | NS     | NS    | NS  | NS          | NS   | NS        | NS      | NS       | NS  |
|                             | The top-n operation reorders a dataset based on one or more identified sort fields as well as a sorting order                                                               | N                                                      | TopNNode              | NS      | NS   | NS    | NS  | NS   | NS    | NS     | NS     | NS   | NS     | NS    | NS  | NS          | NS   | NS        | NS      | NS       | NS  |
|                             | The partitioned output operation redistributes data based on zero or more distribution fields                                                                               | N                                                      | PartitionedOutputNode | NS      | NS   | NS    | NS  | NS   | NS    | NS     | NS     | NS   | NS     | NS    | NS  | NS          | NS   | NS        | NS      | NS       | NS  |
|                             | The values operation returns specified data                                                                                                                                 | N                                                      | ValuesNode            | NS      | NS   | NS    | NS  | NS   | NS    | NS     | NS     | NS   | NS     | NS    | NS  | NS          | NS   | NS        | NS      | NS       | NS  |
|                             | A receiving operation that merges multiple ordered streams to maintain  orderedness                                                                                         | N                                                      | MergeExchangeNode     | NS      | NS   | NS    | NS  | NS   | NS    | NS     | NS     | NS   | NS     | NS    | NS  | NS          | NS   | NS        | NS      | NS       | NS  |
|                             | An operation that merges multiple ordered streams to maintain orderedness                                                                                                   | N                                                      | LocalMergeNode        | NS      | NS   | NS    | NS  | NS   | NS    | NS     | NS     | NS   | NS     | NS    | NS  | NS          | NS   | NS        | NS      | NS       | NS  |
|                             | Partitions input data into multiple streams or combines data from multiple streams into a single stream                                                                     | N                                                      | LocalPartitionNode    | NS      | NS   | NS    | NS  | NS   | NS    | NS     | NS     | NS   | NS     | NS    | NS  | NS          | NS   | NS        | NS      | NS       | NS  |
|                             | The enforce single row operation checks that input contains at most one row and returns that row unmodified                                                                 | N                                                      | EnforceSingleRowNode  | NS      | NS   | NS    | NS  | NS   | NS    | NS     | NS     | NS   | NS     | NS    | NS  | NS          | NS   | NS        | NS      | NS       | NS  |
|                             | The assign unique id operation adds one column at the end of the input columns with unique value per row                                                                    | N                                                      | AssignUniqueIdNode    | NS      | NS   | NS    | NS  | NS   | NS    | NS     | NS     | NS   | NS     | NS    | NS  | NS          | S    | S         | S       | S        | S   |
| ReusedExchangeExec          | A wrapper for reused exchange to have different output                                                                                                                      | ReusedExchangeExec                                     | N                     |         |      |       |     |      |       |        |        |      |        |       |     |             |      |           |         |          |     |
| CollectLimitExec            | Reduce to single partition and apply limit                                                                                                                                  | N                                                      | N                     |         |      |       |     |      |       |        |        |      |        |       |     |             |      |           |         |          |     |
| BroadcastExchangeExec       | The backend for broadcast exchange of data                                                                                                                                  | Y                                                      | Y                     | S       | S    | S     | S   | S    | S     | S      | S      | S    | S      | NS    | NS  | NS          | S    | NS        | S       | NS       | NS  |
| ObjectHashAggregateExec     | The backend for hash based aggregations supporting TypedImperativeAggregate functions                                                                                       | HashAggregateExecBaseTransformer                       | N                     |         |      |       |     |      |       |        |        |      |        |       |     |             |      |           |         |          |     |
| SortAggregateExec           | The backend for sort based aggregations                                                                                                                                     | HashAggregateExecBaseTransformer (Partially supported) | N                     |         |      |       |     |      |       |        |        |      |        |       |     |             |      |           |         |          |     |
| CoalesceExec                | Reduce the partition numbers                                                                                                                                                | CoalesceExecTransformer                                | N                     |         |      |       |     |      |       |        |        |      |        |       |     |             |      |           |         |          |     |
| GenerateExec                | The backend for operations that generate more output rows than input rows like explode                                                                                      | GenerateExecTransformer                                | UnnestNode            |         |      |       |     |      |       |        |        |      |        |       |     |             |      |           |         |          |     |
| RangeExec                   | The backend for range operator                                                                                                                                              | N                                                      | N                     |         |      |       |     |      |       |        |        |      |        |       |     |             |      |           |         |          |     |
| SampleExec                  | The backend for the sample operator                                                                                                                                         | SampleExecTransformer                                  | N                     |         |      |       |     |      |       |        |        |      |        |       |     |             |      |           |         |          |     |
| SubqueryBroadcastExec       | Plan to collect and transform the broadcast key values                                                                                                                      | Y                                                      | Y                     | S       | S    | S     | S   | S    | S     | S      | S      | S    | S      | NS    | NS  | NS          | S    | NS        | S       | NS       | NS  |
| TakeOrderedAndProjectExec   | Take the first limit elements as defined by the sortOrder, and do projection if needed                                                                                      | Y                                                      | Y                     | S       | S    | S     | S   | S    | S     | S      | S      | S    | S      | NS    | NS  | NS          | S    | NS        | S       | NS       | NS  |
| CustomShuffleReaderExec     | A wrapper of shuffle query stage                                                                                                                                            | N                                                      | N                     |         |      |       |     |      |       |        |        |      |        |       |     |             |      |           |         |          |     |
| InMemoryTableScanExec       | Implementation of InMemory Table Scan                                                                                                                                       | Y                                                      | Y                     |         |      |       |     |      |       |        |        |      |        |       |     |             |      |           |         |          |     |
| BroadcastNestedLoopJoinExec | Implementation of join using brute force. Full outer joins and joins where the broadcast side matches the join side (e.g.: LeftOuter with left broadcast) are not supported | BroadcastNestedLoopJoinExecTransformer                 | NestedLoopJoinNode    | S       | S    | S     | S   | S    | S     | S      | S      | S    | S      | NS    | NS  | NS          | S    | NS        | NS      | NS       | NS  |
| AggregateInPandasExec       | The backend for an Aggregation Pandas UDF, this accelerates the data transfer between the Java process and the Python process                                               | N                                                      | N                     |         |      |       |     |      |       |        |        |      |        |       |     |             |      |           |         |          |     |
| ArrowEvalPythonExec         | The backend of the Scalar Pandas UDFs. Accelerates the data transfer between the Java process and the Python process                                                        | N                                                      | N                     |         |      |       |     |      |       |        |        |      |        |       |     |             |      |           |         |          |     |
| FlatMapGroupsInPandasExec   | The backend for Flat Map Groups Pandas UDF, Accelerates the data transfer between the Java process and the Python process                                                   | N                                                      | N                     |         |      |       |     |      |       |        |        |      |        |       |     |             |      |           |         |          |     |
| MapInPandasExec             | The backend for Map Pandas Iterator UDF. Accelerates the data transfer between the Java process and the Python process                                                      | N                                                      | N                     |         |      |       |     |      |       |        |        |      |        |       |     |             |      |           |         |          |     |
| WindowInPandasExec          | The backend for Window Aggregation Pandas UDF, Accelerates the data transfer between the Java process and the Python process                                                | N                                                      | N                     |         |      |       |     |      |       |        |        |      |        |       |     |             |      |           |         |          |     |
| HiveTableScanExec           | The Hive table scan operator.  Column and partition pruning are both handled                                                                                                | Y                                                      | Y                     |         |      |       |     |      |       |        |        |      |        |       |     |             |      |           |         |          |     |
| InsertIntoHiveTable         | Command for writing data out to a Hive table                                                                                                                                | Y                                                      | Y                     |         |      |       |     |      |       |        |        |      |        |       |     |             |      |           |         |          |     |
| Velox2Row                   | Convert Velox format to Row format                                                                                                                                          | Y                                                      | Y                     | S       | S    | S     | S   | S    | S     | S      | S      | NS   | S      | NS    | NS  | NS          | S    | S         | NS      | NS       | NS  |
| Velox2Arrow                 | Convert Velox format to Arrow format                                                                                                                                        | Y                                                      | Y                     | S       | S    | S     | S   | S    | S     | S      | S      | NS   | S      | S     | S   | S           | S    | NS        | S       | NS       | NS  |
| WindowGroupLimitExec        | Optimize window with rank like function with filter on it                                                                                                                   | Y                                                      | Y                     | S       | S    | S     | S   | S    | S     | S      | S      | NS   | S      | S     | S   | S           | S    | NS        | S       | NS       | NS  |

#### Scalar Functions Support Status

Out of 352 scalar functions in Spark 3.5, Gluten currently fully supports 229 functions and partially supports 13 functions.

##### URL Functions

| Spark Functions | Spark Expressions | Status | Restrictions |
|-----------------|-------------------|--------|--------------|
| parse_url       | ParseUrl          |        |              |
| url_decode      | UrlDecode         | PS     |              |
| url_encode      | UrlEncode         | PS     |              |

##### Bitwise Functions

| Spark Functions    | Spark Expressions  | Status | Restrictions |
|--------------------|--------------------|--------|--------------|
| &                  | BitwiseAnd         | S      |              |
| ^                  | BitwiseXor         | S      |              |
| bit_count          | BitwiseCount       | S      |              |
| bit_get            | BitwiseGet         | S      |              |
| getbit             | BitwiseGet         | S      |              |
| shiftright         | ShiftRight         | S      |              |
| shiftrightunsigned | ShiftRightUnsigned | S      |              |
| &#124;             | BitwiseOr          | S      |              |
| ~                  | BitwiseNot         | S      |              |

##### Misc Functions

| Spark Functions             | Spark Expressions         | Status | Restrictions |
|-----------------------------|---------------------------|--------|--------------|
| aes_decrypt                 | AesDecrypt                |        |              |
| aes_encrypt                 | AesEncrypt                |        |              |
| assert_true                 | AssertTrue                |        |              |
| bitmap_bit_position         | BitmapBitPosition         |        |              |
| bitmap_bucket_number        | BitmapBucketNumber        |        |              |
| bitmap_count                | BitmapCount               |        |              |
| current_catalog             | CurrentCatalog            |        |              |
| current_database            | CurrentDatabase           |        |              |
| current_schema              | CurrentDatabase           |        |              |
| current_user                | CurrentUser               |        |              |
| equal_null                  | EqualNull                 |        |              |
| hll_sketch_estimate         | HllSketchEstimate         |        |              |
| hll_union                   | HllUnion                  |        |              |
| input_file_block_length     | InputFileBlockLength      |        |              |
| input_file_block_start      | InputFileBlockStart       |        |              |
| input_file_name             | InputFileName             |        |              |
| java_method                 | CallMethodViaReflection   |        |              |
| monotonically_increasing_id | MonotonicallyIncreasingID |        |              |
| reflect                     | CallMethodViaReflection   |        |              |
| spark_partition_id          | SparkPartitionID          | S      |              |
| try_aes_decrypt             | TryAesDecrypt             |        |              |
| typeof                      | TypeOf                    |        |              |
| user                        | CurrentUser               |        |              |
| uuid                        | Uuid                      | S      |              |
| version                     | SparkVersion              | S      |              |

##### Mathematical Functions

| Spark Functions | Spark Expressions      | Status | Restrictions |
|-----------------|------------------------|--------|--------------|
| %               | Remainder              | S      |              |
| *               | Multiply               | S      |              |
| +               | Add                    | S      |              |
| -               | Subtract               | S      |              |
| /               | Divide                 | S      |              |
| abs             | Abs                    | S      |              |
| acos            | Acos                   | S      |              |
| acosh           | Acosh                  | S      |              |
| asin            | Asin                   | S      |              |
| asinh           | Asinh                  | S      |              |
| atan            | Atan                   | S      |              |
| atan2           | Atan2                  | S      |              |
| atanh           | Atanh                  | S      |              |
| bin             | Bin                    | S      |              |
| bround          | BRound                 |        |              |
| cbrt            | Cbrt                   | S      |              |
| ceil            | CeilExpressionBuilder  | PS     |              |
| ceiling         | CeilExpressionBuilder  | PS     |              |
| conv            | Conv                   | S      |              |
| cos             | Cos                    | S      |              |
| cosh            | Cosh                   | S      |              |
| cot             | Cot                    | S      |              |
| csc             | Csc                    | S      |              |
| degrees         | ToDegrees              | S      |              |
| div             | IntegralDivide         |        |              |
| e               | EulerNumber            | S      |              |
| exp             | Exp                    | S      |              |
| expm1           | Expm1                  | S      |              |
| factorial       | Factorial              | S      |              |
| floor           | FloorExpressionBuilder | PS     |              |
| greatest        | Greatest               | S      |              |
| hex             | Hex                    | S      |              |
| hypot           | Hypot                  | S      |              |
| least           | Least                  | S      |              |
| ln              | Log                    | S      |              |
| log             | Logarithm              | S      |              |
| log10           | Log10                  | S      |              |
| log1p           | Log1p                  | S      |              |
| log2            | Log2                   | S      |              |
| mod             | Remainder              | S      |              |
| negative        | UnaryMinus             | S      |              |
| pi              | Pi                     | S      |              |
| pmod            | Pmod                   | S      |              |
| positive        | UnaryPositive          | S      |              |
| pow             | Pow                    | S      |              |
| power           | Pow                    | S      |              |
| radians         | ToRadians              | S      |              |
| rand            | Rand                   | S      |              |
| randn           | Randn                  |        |              |
| random          | Rand                   | S      |              |
| rint            | Rint                   | S      |              |
| round           | Round                  | S      |              |
| sec             | Sec                    | S      |              |
| shiftleft       | ShiftLeft              | S      |              |
| sign            | Signum                 | S      |              |
| signum          | Signum                 | S      |              |
| sin             | Sin                    | S      |              |
| sinh            | Sinh                   | S      |              |
| sqrt            | Sqrt                   | S      |              |
| tan             | Tan                    | S      |              |
| tanh            | Tanh                   | S      |              |
| try_add         | TryAdd                 | PS     |              |
| try_divide      | TryDivide              |        |              |
| try_multiply    | TryMultiply            |        |              |
| try_subtract    | TrySubtract            |        |              |
| unhex           | Unhex                  | S      |              |
| width_bucket    | WidthBucket            | S      |              |

##### Predicate Functions

| Spark Functions | Spark Expressions  | Status | Restrictions           |
|-----------------|--------------------|--------|------------------------|
| !               | Not                | S      |                        |
| <               | LessThan           | S      |                        |
| <=              | LessThanOrEqual    | S      |                        |
| <=>             | EqualNullSafe      | S      |                        |
| =               | EqualTo            | S      |                        |
| ==              | EqualTo            | S      |                        |
| >               | GreaterThan        | S      |                        |
| >=              | GreaterThanOrEqual | S      |                        |
| and             | And                | S      |                        |
| ilike           | ILike              |        |                        |
| in              | In                 | PS     |                        |
| isnan           | IsNaN              | S      |                        |
| isnotnull       | IsNotNull          | S      |                        |
| isnull          | IsNull             | S      |                        |
| like            | Like               | S      |                        |
| not             | Not                | S      |                        |
| or              | Or                 | S      |                        |
| regexp          | RLike              | PS     | Lookaround unsupported |
| regexp_like     | RLike              | PS     | Lookaround unsupported |
| rlike           | RLike              | PS     | Lookaround unsupported |

##### Date and Timestamp Functions

| Spark Functions     | Spark Expressions                    | Status | Restrictions |
|---------------------|--------------------------------------|--------|--------------|
| add_months          | AddMonths                            | S      |              |
| convert_timezone    | ConvertTimezone                      |        |              |
| curdate             | CurDateExpressionBuilder             |        |              |
| current_date        | CurrentDate                          |        |              |
| current_timestamp   | CurrentTimestamp                     |        |              |
| current_timezone    | CurrentTimeZone                      |        |              |
| date_add            | DateAdd                              | S      |              |
| date_diff           | DateDiff                             | S      |              |
| date_format         | DateFormatClass                      | S      |              |
| date_from_unix_date | DateFromUnixDate                     | S      |              |
| date_part           | DatePartExpressionBuilder            |        |              |
| date_sub            | DateSub                              | S      |              |
| date_trunc          | TruncTimestamp                       | S      |              |
| dateadd             | DateAdd                              | S      |              |
| datediff            | DateDiff                             | S      |              |
| datepart            | DatePartExpressionBuilder            |        |              |
| day                 | DayOfMonth                           | S      |              |
| dayofmonth          | DayOfMonth                           | S      |              |
| dayofweek           | DayOfWeek                            | S      |              |
| dayofyear           | DayOfYear                            | S      |              |
| extract             | Extract                              | S      |              |
| from_unixtime       | FromUnixTime                         | S      |              |
| from_utc_timestamp  | FromUTCTimestamp                     | S      |              |
| hour                | Hour                                 | S      |              |
| last_day            | LastDay                              | S      |              |
| localtimestamp      | LocalTimestamp                       |        |              |
| make_date           | MakeDate                             | S      |              |
| make_dt_interval    | MakeDTInterval                       |        |              |
| make_interval       | MakeInterval                         |        |              |
| make_timestamp      | MakeTimestamp                        | S      |              |
| make_timestamp_ltz  | MakeTimestampLTZExpressionBuilder    |        |              |
| make_timestamp_ntz  | MakeTimestampNTZExpressionBuilder    |        |              |
| make_ym_interval    | MakeYMInterval                       | S      |              |
| minute              | Minute                               | S      |              |
| month               | Month                                | S      |              |
| months_between      | MonthsBetween                        | S      |              |
| next_day            | NextDay                              | S      |              |
| now                 | Now                                  |        |              |
| quarter             | Quarter                              | S      |              |
| second              | Second                               | S      |              |
| session_window      | SessionWindow                        |        |              |
| timestamp_micros    | MicrosToTimestamp                    | S      |              |
| timestamp_millis    | MillisToTimestamp                    | S      |              |
| timestamp_seconds   | SecondsToTimestamp                   |        |              |
| to_date             | ParseToDate                          |        |              |
| to_timestamp        | ParseToTimestamp                     |        |              |
| to_timestamp_ltz    | ParseToTimestampLTZExpressionBuilder |        |              |
| to_timestamp_ntz    | ParseToTimestampNTZExpressionBuilder |        |              |
| to_unix_timestamp   | ToUnixTimestamp                      | S      |              |
| to_utc_timestamp    | ToUTCTimestamp                       | S      |              |
| trunc               | TruncDate                            |        |              |
| try_to_timestamp    | TryToTimestampExpressionBuilder      |        |              |
| unix_date           | UnixDate                             | S      |              |
| unix_micros         | UnixMicros                           | S      |              |
| unix_millis         | UnixMillis                           | S      |              |
| unix_seconds        | UnixSeconds                          | S      |              |
| unix_timestamp      | UnixTimestamp                        | S      |              |
| weekday             | WeekDay                              | S      |              |
| weekofyear          | WeekOfYear                           | S      |              |
| window              | TimeWindow                           |        |              |
| window_time         | WindowTime                           |        |              |
| year                | Year                                 | S      |              |

##### Map Functions

| Spark Functions  | Spark Expressions | Status | Restrictions |
|------------------|-------------------|--------|--------------|
| element_at       | ElementAt         | S      |              |
| map              | CreateMap         | PS     |              |
| map_concat       | MapConcat         |        |              |
| map_contains_key | MapContainsKey    |        |              |
| map_entries      | MapEntries        | S      |              |
| map_from_arrays  | MapFromArrays     | S      |              |
| map_from_entries | MapFromEntries    |        |              |
| map_keys         | MapKeys           | S      |              |
| map_values       | MapValues         | S      |              |
| str_to_map       | StringToMap       | S      |              |
| try_element_at   | TryElementAt      |        |              |

##### String Functions

| Spark Functions    | Spark Expressions           | Status | Restrictions           |
|--------------------|-----------------------------|--------|------------------------|
| ascii              | Ascii                       | S      |                        |
| base64             | Base64                      | S      |                        |
| bit_length         | BitLength                   | S      |                        |
| btrim              | StringTrimBoth              |        |                        |
| char               | Chr                         | S      |                        |
| char_length        | Length                      | S      |                        |
| character_length   | Length                      | S      |                        |
| chr                | Chr                         | S      |                        |
| concat_ws          | ConcatWs                    | S      |                        |
| contains           | ContainsExpressionBuilder   | S      |                        |
| decode             | Decode                      |        |                        |
| elt                | Elt                         |        |                        |
| encode             | Encode                      |        |                        |
| endswith           | EndsWithExpressionBuilder   |        |                        |
| find_in_set        | FindInSet                   | S      |                        |
| format_number      | FormatNumber                |        |                        |
| format_string      | FormatString                | S      |                        |
| initcap            | InitCap                     | S      |                        |
| instr              | StringInstr                 | S      |                        |
| lcase              | Lower                       | S      |                        |
| left               | Left                        | S      |                        |
| len                | Length                      | S      |                        |
| length             | Length                      | S      |                        |
| levenshtein        | Levenshtein                 | S      |                        |
| locate             | StringLocate                | S      |                        |
| lower              | Lower                       | S      |                        |
| lpad               | LPadExpressionBuilder       | S      |                        |
| ltrim              | StringTrimLeft              | S      |                        |
| luhn_check         | Luhncheck                   |        |                        |
| mask               | MaskExpressionBuilder       | S      |                        |
| octet_length       | OctetLength                 |        |                        |
| overlay            | Overlay                     | S      |                        |
| position           | StringLocate                | S      |                        |
| printf             | FormatString                | S      |                        |
| regexp_count       | RegExpCount                 |        |                        |
| regexp_extract     | RegExpExtract               | PS     | Lookaround unsupported |
| regexp_extract_all | RegExpExtractAll            | PS     | Lookaround unsupported |
| regexp_instr       | RegExpInStr                 |        |                        |
| regexp_replace     | RegExpReplace               | PS     | Lookaround unsupported |
| regexp_substr      | RegExpSubStr                |        |                        |
| repeat             | StringRepeat                | S      |                        |
| replace            | StringReplace               | S      |                        |
| right              | Right                       |        |                        |
| rpad               | RPadExpressionBuilder       | S      |                        |
| rtrim              | StringTrimRight             | S      |                        |
| sentences          | Sentences                   |        |                        |
| soundex            | SoundEx                     | S      |                        |
| space              | StringSpace                 | S      |                        |
| split              | StringSplit                 | S      |                        |
| split_part         | SplitPart                   | S      |                        |
| startswith         | StartsWithExpressionBuilder |        |                        |
| substr             | Substring                   | S      |                        |
| substring          | Substring                   | S      |                        |
| substring_index    | SubstringIndex              | S      |                        |
| to_binary          | ToBinary                    |        |                        |
| to_char            | ToCharacter                 |        |                        |
| to_number          | ToNumber                    |        |                        |
| to_varchar         | ToCharacter                 |        |                        |
| translate          | StringTranslate             | S      |                        |
| trim               | StringTrim                  | S      |                        |
| try_to_binary      | TryToBinary                 |        |                        |
| try_to_number      | TryToNumber                 |        |                        |
| ucase              | Upper                       | S      |                        |
| unbase64           | UnBase64                    |        |                        |
| upper              | Upper                       | S      |                        |

##### Csv Functions

| Spark Functions | Spark Expressions | Status | Restrictions |
|-----------------|-------------------|--------|--------------|
| from_csv        | CsvToStructs      |        |              |
| schema_of_csv   | SchemaOfCsv       |        |              |
| to_csv          | StructsToCsv      |        |              |

##### Lambda Functions

| Spark Functions  | Spark Expressions | Status | Restrictions |
|------------------|-------------------|--------|--------------|
| aggregate        | ArrayAggregate    | S      |              |
| array_sort       | ArraySort         | S      |              |
| exists           | ArrayExists       | S      |              |
| filter           | ArrayFilter       | S      |              |
| forall           | ArrayForAll       | S      |              |
| map_filter       | MapFilter         |        |              |
| map_zip_with     | MapZipWith        | S      |              |
| reduce           | ArrayAggregate    | S      |              |
| transform        | ArrayTransform    | S      |              |
| transform_keys   | TransformKeys     | S      |              |
| transform_values | TransformValues   | S      |              |
| zip_with         | ZipWith           | S      |              |

##### Array Functions

| Spark Functions | Spark Expressions | Status | Restrictions |
|-----------------|-------------------|--------|--------------|
| array           | CreateArray       | S      |              |
| array_append    | ArrayAppend       |        |              |
| array_compact   | ArrayCompact      |        |              |
| array_contains  | ArrayContains     | S      |              |
| array_distinct  | ArrayDistinct     | S      |              |
| array_except    | ArrayExcept       | S      |              |
| array_insert    | ArrayInsert       | S      |              |
| array_intersect | ArrayIntersect    | S      |              |
| array_join      | ArrayJoin         | S      |              |
| array_max       | ArrayMax          | S      |              |
| array_min       | ArrayMin          | S      |              |
| array_position  | ArrayPosition     | S      |              |
| array_prepend   | ArrayPrepend      |        |              |
| array_remove    | ArrayRemove       | S      |              |
| array_repeat    | ArrayRepeat       | S      |              |
| array_union     | ArrayUnion        |        |              |
| arrays_overlap  | ArraysOverlap     | S      |              |
| arrays_zip      | ArraysZip         | S      |              |
| flatten         | Flatten           | S      |              |
| get             | Get               |        |              |
| sequence        | Sequence          | S      |              |
| shuffle         | Shuffle           | S      |              |
| slice           | Slice             | S      |              |
| sort_array      | SortArray         | S      |              |

##### Conversion Functions

For detailed cast support status, please check below section [Cast Support Status](#cast-support-status).

| Spark Functions | Spark Expressions | Status | Restrictions |
|-----------------|-------------------|--------|--------------|
| bigint          |                   | S      |              |
| binary          |                   | S      |              |
| boolean         |                   | S      |              |
| cast            | Cast              | S      |              |
| date            |                   | S      |              |
| decimal         |                   | S      |              |
| double          |                   | S      |              |
| float           |                   | S      |              |
| int             |                   | S      |              |
| smallint        |                   | S      |              |
| string          |                   | S      |              |
| timestamp       |                   | S      |              |
| tinyint         |                   | S      |              |

##### Hash Functions

| Spark Functions | Spark Expressions | Status | Restrictions |
|-----------------|-------------------|--------|--------------|
| crc32           | Crc32             | S      |              |
| hash            | Murmur3Hash       | S      |              |
| md5             | Md5               | S      |              |
| sha             | Sha1              | S      |              |
| sha1            | Sha1              | S      |              |
| sha2            | Sha2              | S      |              |
| xxhash64        | XxHash64          | S      |              |

##### JSON Functions

| Spark Functions   | Spark Expressions | Status | Restrictions |
|-------------------|-------------------|--------|--------------|
| from_json         | JsonToStructs     |        |              |
| get_json_object   | GetJsonObject     | S      |              |
| json_array_length | LengthOfJsonArray |        |              |
| json_object_keys  | JsonObjectKeys    |        |              |
| json_tuple        | JsonTuple         | S      |              |
| schema_of_json    | SchemaOfJson      |        |              |
| to_json           | StructsToJson     |        |              |

##### Collection Functions

| Spark Functions | Spark Expressions | Status | Restrictions |
|-----------------|-------------------|--------|--------------|
| array_size      | ArraySize         |        |              |
| cardinality     | Size              | S      |              |
| concat          | Concat            | S      |              |
| reverse         | Reverse           | S      |              |
| size            | Size              | S      |              |

##### Struct Functions

| Spark Functions | Spark Expressions | Status | Restrictions |
|-----------------|-------------------|--------|--------------|
| named_struct    | CreateNamedStruct | S      |              |
| struct          |                   | S      |              |

##### Conditional Functions

| Spark Functions | Spark Expressions | Status | Restrictions |
|-----------------|-------------------|--------|--------------|
| coalesce        | Coalesce          | S      |              |
| if              | If                | S      |              |
| ifnull          | Nvl               |        |              |
| nanvl           | NaNvl             | S      |              |
| nullif          | NullIf            |        |              |
| nvl             | Nvl               |        |              |
| nvl2            | Nvl2              |        |              |
| when            | CaseWhen          | S      |              |

##### XML Functions

| Spark Functions | Spark Expressions | Status | Restrictions |
|-----------------|-------------------|--------|--------------|
| xpath           | XPathList         |        |              |
| xpath_boolean   | XPathBoolean      |        |              |
| xpath_double    | XPathDouble       |        |              |
| xpath_float     | XPathFloat        |        |              |
| xpath_int       | XPathInt          |        |              |
| xpath_long      | XPathLong         |        |              |
| xpath_number    | XPathDouble       |        |              |
| xpath_short     | XPathShort        |        |              |
| xpath_string    | XPathString       |        |              |

#### Cast Support Status

* S: supported.
* NS: not supported.
* -: not accepted by Spark.
* N/A: not applicable case, e.g., from type is as same as to type, where cast will not actually happen.

| From \ To | BOOLEAN | BYTE | SHORT | INT | LONG | FLOAT | DOUBLE | DECIMAL | DATE | TIMESTAMP | STRING | BINARY | ARRAY | MAP | STRUCT | NULL |
|-----------|---------|------|-------|-----|------|-------|--------|---------|------|-----------|--------|--------|-------|-----|--------|------|
| BOOLEAN   | N/A     | S    | S     | S   | S    | S     | S      | S       | -    | NS        | S      | -      | -     | -   | -      | -    |
| BYTE      | S       | N/A  | S     | S   | S    | S     | S      | S       | -    | NS        | S      | S      | -     | -   | -      | -    |
| SHORT     | S       | S    | N/A   | S   | S    | S     | S      | S       | -    | NS        | S      | S      | -     | -   | -      | -    |
| INT       | S       | S    | S     | N/A | S    | S     | S      | S       | -    | NS        | S      | S      | -     | -   | -      | -    |
| LONG      | S       | S    | S     | S   | N/A  | S     | S      | S       | -    | NS        | S      | S      | -     | -   | -      | -    |
| FLOAT     | S       | S    | S     | S   | S    | N/A   | S      | S       | -    | NS        | S      | -      | -     | -   | -      | -    |
| DOUBLE    | S       | S    | S     | S   | S    | S     | N/A    | S       | -    | NS        | S      | -      | -     | -   | -      | -    |
| DECIMAL   | S       | S    | S     | S   | S    | S     | S      | N/A     | -    | NS        | S      | -      | -     | -   | -      | -    |
| DATE      | NS      | NS   | NS    | NS  | NS   | NS    | NS     | NS      | N/A  | NS        | NS     | -      | -     | -   | -      | -    |
| TIMESTAMP | NS      | NS   | NS    | NS  | NS   | NS    | NS     | NS      | NS   | N/A       | NS     | -      | -     | -   | -      | -    |
| STRING    | S       | S    | S     | S   | S    | S     | S      | S       | NS   | NS        | N/A    | -      | -     | -   | -      | -    |
| BINARY    | S       | S    | S     | S   | S    | S     | S      | S       | NS   | NS        | S      | N/A    | -     | -   | -      | -    |
| ARRAY     | -       | -    | -     | -   | -    | -     | -      | -       | -    | -         | NS     | -      | N/A   | -   | -      | -    |
| Map       | -       | -    | -     | -   | -    | -     | -      | -       | -    | -         | NS     | -      | -     | N/A | -      | -    |
| STRUCT    | -       | -    | -     | -   | -    | -     | -      | -       | -    | -         | NS     | -      | -     | -   | N/A    | -    |
| NULL      | S       | S    | S     | S   | S    | S     | S      | S       | S    | NS        | S      | S      | S     | S   | S      | N/A  |

#### Other functions' support status

| Spark Functions             | Velox/Presto Functions | Velox/Spark functions | Gluten | Restrictions           | BOOLEAN | BYTE | SHORT | INT | LONG | FLOAT | DOUBLE | DATE | TIMESTAMP | STRING | DECIMAL | NULL | BINARY | CALENDAR | ARRAY | MAP | STRUCT | UDT |
|-----------------------------|------------------------|-----------------------|--------|------------------------|---------|------|-------|-----|------|-------|--------|------|-----------|--------|---------|------|--------|----------|-------|-----|--------|-----|
| bit_and                     | bitwise_and_agg        |                       | S      |                        |         | S    | S     | S   | S    | S     |        |      |           |        |         |      |        |          |       |     |        |     |
| bit_or                      |                        |                       | S      |                        |         |      |       |     |      |       |        |      |           |        |         |      |        |          |       |     |        |     |
| bit_xor                     |                        | bit_xor               | S      |                        |         |      |       |     |      |       |        |      |           |        |         |      |        |          |       |     |        |     |
| explode                     |                        |                       |        |                        |         |      |       |     |      |       |        |      |           |        |         |      |        |          |       |     |        |     |
| explode_outer               |                        |                       |        |                        |         |      |       |     |      |       |        |      |           |        |         |      |        |          |       |     |        |     |
| get_map_value               |                        | element_at            | S      |                        |         |      |       |     |      |       |        |      |           |        |         |      |        |          |       | S   |        |     |
| posexplode_outer            |                        |                       |        |                        |         |      |       |     |      |       |        |      |           |        |         |      |        |          |       |     |        |     |
| any                         |                        |                       |        |                        |         |      |       |     |      |       |        |      |           |        |         |      |        |          |       |     |        |     |
| approx_count_distinct       | approx_distinct        |                       | S      |                        | S       | S    | S     | S   | S    | S     | S      | S    |           | S      |         |      |        |          |       |     |        |     |
| approx_percentile           |                        |                       |        |                        |         |      |       |     |      |       |        |      |           |        |         |      |        |          |       |     |        |     |
| avg                         | avg                    |                       | S      | ANSI OFF               |         | S    | S     | S   | S    | S     |        |      |           |        |         |      |        |          |       |     |        |     |
| bool_and                    |                        |                       |        |                        |         |      |       |     |      |       |        |      |           |        |         |      |        |          |       |     |        |     |
| bool_or                     |                        |                       |        |                        |         |      |       |     |      |       |        |      |           |        |         |      |        |          |       |     |        |     |
| collect_list                |                        |                       | S      |                        |         |      |       |     |      |       |        |      |           |        |         |      |        |          |       |     |        |     |
| collect_set                 |                        |                       | S      |                        |         |      |       |     |      |       |        |      |           |        |         |      |        |          |       |     |        |     |
| corr                        | corr                   |                       | S      |                        |         |      | S     | S   | S    | S     | S      |      |           |        |         |      |        |          |       |     |        |     |
| count                       | count                  |                       | S      |                        |         |      | S     | S   | S    | S     | S      |      |           |        |         |      |        |          |       |     |        |     |
| count_if                    | count_if               |                       |        |                        |         | S    | S     | S   | S    | S     |        |      |           |        |         |      |        |          |       |     |        |     |
| count_min_sketch            |                        |                       |        |                        |         |      |       |     |      |       |        |      |           |        |         |      |        |          |       |     |        |     |
| covar_pop                   | covar_pop              |                       | S      |                        |         | S    | S     | S   | S    | S     |        |      |           |        |         |      |        |          |       |     |        |     |
| covar_samp                  | covar_samp             |                       | S      |                        |         | S    | S     | S   | S    | S     |        |      |           |        |         |      |        |          |       |     |        |     |
| every                       |                        |                       |        |                        |         |      |       |     |      |       |        |      |           |        |         |      |        |          |       |     |        |     |
| first                       |                        | first                 | S      |                        |         |      |       |     |      |       |        |      |           |        |         |      |        |          |       |     |        |     |
| first_value                 |                        | first_value           | S      |                        |         |      |       |     |      |       |        |      |           |        |         |      |        |          |       |     |        |     |
| grouping                    |                        |                       |        |                        |         |      |       |     |      |       |        |      |           |        |         |      |        |          |       |     |        |     |
| grouping_id                 |                        |                       |        |                        |         |      |       |     |      |       |        |      |           |        |         |      |        |          |       |     |        |     |
| kurtosis                    | kurtosis               | kurtosis              | S      |                        |         |      | S     | S   | S    | S     | S      |      |           |        |         |      |        |          |       |     |        |     |
| last                        |                        | last                  | S      |                        |         |      |       |     |      |       |        |      |           |        |         |      |        |          |       |     |        |     |
| last_value                  |                        | last_value            | S      |                        |         |      |       |     |      |       |        |      |           |        |         |      |        |          |       |     |        |     |
| max                         | max                    |                       | S      |                        |         |      | S     | S   | S    | S     | S      |      |           |        |         |      |        |          |       |     |        |     |
| max_by                      |                        |                       | S      |                        |         |      |       |     |      |       |        |      |           |        |         |      |        |          |       |     |        |     |
| mean                        | avg                    |                       | S      | ANSI OFF               |         |      |       |     |      |       |        |      |           |        |         |      |        |          |       |     |        |     |
| min                         | min                    |                       | S      |                        |         |      | S     | S   | S    | S     | S      |      |           |        |         |      |        |          |       |     |        |     |
| min_by                      |                        |                       | S      |                        |         |      |       |     |      |       |        |      |           |        |         |      |        |          |       |     |        |     |
| regr_avgx                   | regr_avgx              | regr_avgx             | S      |                        |         |      | S     | S   | S    | S     | S      |      |           |        |         |      |        |          |       |     |        |     |
| regr_avgy                   | regr_avgy              | regr_avgy             | S      |                        |         |      | S     | S   | S    | S     | S      |      |           |        |         |      |        |          |       |     |        |     |
| regr_count                  | regr_count             | regr_count            | S      |                        |         |      | S     | S   | S    | S     | S      |      |           |        |         |      |        |          |       |     |        |     |
| regr_r2                     | regr_r2                | regr_r2               | S      |                        |         |      | S     | S   | S    | S     | S      |      |           |        |         |      |        |          |       |     |        |     |
| regr_intercept              | regr_intercept         | regr_intercept        | S      |                        |         |      | S     | S   | S    | S     | S      |      |           |        |         |      |        |          |       |     |        |     |
| regr_slope                  | regr_slope             | regr_slope            | S      |                        |         |      | S     | S   | S    | S     | S      |      |           |        |         |      |        |          |       |     |        |     |
| regr_sxy                    | regr_sxy               | regr_sxy              | S      |                        |         |      | S     | S   | S    | S     | S      |      |           |        |         |      |        |          |       |     |        |     |
| regr_sxx                    | regr_sxx               | regr_sxx              | S      |                        |         |      | S     | S   | S    | S     | S      |      |           |        |         |      |        |          |       |     |        |     |
| regr_syy                    | regr_syy               | regr_syy              | S      |                        |         |      | S     | S   | S    | S     | S      |      |           |        |         |      |        |          |       |     |        |     |
| skewness                    | skewness               | skewness              | S      |                        |         |      | S     | S   | S    | S     | S      |      |           |        |         |      |        |          |       |     |        |     |
| some                        |                        |                       |        |                        |         |      |       |     |      |       |        |      |           |        |         |      |        |          |       |     |        |     |
| std                         | stddev                 |                       | S      |                        |         |      | S     | S   | S    | S     | S      |      |           |        |         |      |        |          |       |     |        |     |
| stddev                      | stddev                 |                       | S      |                        |         |      | S     | S   | S    | S     | S      |      |           |        |         |      |        |          |       |     |        |     |
| stddev_pop                  | stddev_pop             |                       | S      |                        |         | S    | S     | S   | S    | S     |        |      |           |        |         |      |        |          |       |     |        |     |
| stddev_samp                 | stddev_samp            |                       | S      |                        |         |      | S     | S   | S    | S     | S      |      |           |        |         |      |        |          |       |     |        |     |
| sum                         | sum                    |                       | S      | ANSI OFF               |         | S    | S     | S   | S    | S     |        |      |           |        |         |      |        |          |       |     |        |     |
| var_pop                     | var_pop                |                       | S      |                        |         | S    | S     | S   | S    | S     |        |      |           |        |         |      |        |          |       |     |        |     |
| var_samp                    | var_samp               |                       | S      |                        |         | S    | S     | S   | S    | S     |        |      |           |        |         |      |        |          |       |     |        |     |
| variance                    | variance               |                       | S      |                        |         | S    | S     | S   | S    | S     |        |      |           |        |         |      |        |          |       |     |        |     |
| cume_dist                   | cume_dist              |                       | S      |                        |         |      |       |     |      |       |        |      |           |        |         |      |        |          |       |     |        |     |
| dense_rank                  | dense_rank             |                       | S      |                        |         |      |       |     |      |       |        |      |           |        |         |      |        |          |       |     |        |     |
| lag                         |                        |                       | S      |                        |         |      |       |     |      |       |        |      |           |        |         |      |        |          |       |     |        |     |
| lead                        |                        |                       | S      |                        |         |      |       |     |      |       |        |      |           |        |         |      |        |          |       |     |        |     |
| nth_value                   | nth_value              | nth_value             | PS     |                        |         |      |       |     |      |       |        |      |           |        |         |      |        |          |       |     |        |     |
| ntile                       | ntile                  | ntile                 | S      |                        |         |      |       |     |      |       |        |      |           |        |         |      |        |          |       |     |        |     |
| percent_rank                | percent_rank           |                       | S      |                        |         |      |       |     |      |       |        |      |           |        |         |      |        |          |       |     |        |     |
| rank                        | rank                   |                       | S      |                        |         |      |       |     |      |       |        |      |           |        |         |      |        |          |       |     |        |     |
| row_number                  | row_number             |                       | S      |                        |         |      | S     | S   | S    |       |        |      |           |        |         |      |        |          |       |     |        |     |
| inline                      |                        |                       |        |                        |         |      |       |     |      |       |        |      |           |        |         |      |        |          |       |     |        |     |
| inline_outer                |                        |                       |        |                        |         |      |       |     |      |       |        |      |           |        |         |      |        |          |       |     |        |     |
| raise_error                 |                        | raise_error           | S      |                        |         |      |       |     |      |       |        |      |           |        |         |      |        |          |       |     |        |     |
| stack                       |                        |                       | S      |                        | S       | S    | S     | S   | S    | S     | S      | S    | S         | S      | S       | S    | S      | S        | S     | S   | S      | S   |
| try_substract               |                        |                       | S      |                        |         |      |       |     |      |       |        |      |           |        |         |      |        |          |       |     |        |     |
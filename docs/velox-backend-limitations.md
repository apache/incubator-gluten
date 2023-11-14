---
layout: page
title: Velox Backend Limitations
nav_order: 5
---
This document describes the limitations of velox backend by listing some known cases where exception will be thrown, gluten behaves incompatibly with spark, or certain plan's execution
must fall back to vanilla spark, etc.

### Runtime BloomFilter

Velox BloomFilter's implementation is different from Spark's. So if `might_contain` falls back, but `bloom_filter_agg` is offloaded to velox, an exception will be thrown.

#### example

```sql
SELECT might_contain(null, null) both_null,
       might_contain(null, 1L) null_bf,
       might_contain((SELECT bloom_filter_agg(cast(id as long)) from range(1, 10000)),
            null) null_value
```

The below exception will be thrown.

```
Unexpected Bloom filter version number (512)
java.io.IOException: Unexpected Bloom filter version number (512)
 at org.apache.spark.util.sketch.BloomFilterImpl.readFrom0(BloomFilterImpl.java:256)
 at org.apache.spark.util.sketch.BloomFilterImpl.readFrom(BloomFilterImpl.java:265)
 at org.apache.spark.util.sketch.BloomFilter.readFrom(BloomFilter.java:178)
```

#### Solution

Set the gluten config `spark.gluten.sql.native.bloomFilter=false` to fall back to vanilla bloom filter, you can also disable runtime filter by setting spark config `spark.sql.optimizer.runtime.bloomFilter.enabled=false`.

### ANSI (fallback behavior)

Gluten currently doesn't support ANSI mode. If ANSI is enabled, Spark plan's execution will always fall back to vanilla Spark.

### Case Sensitive mode (incompatible behavior)

Gluten only supports spark default case-insensitive mode. If case-sensitive mode is enabled, user may get incorrect result.

### Spark's columnar reading (fatal error)

If the user enables Spark's columnar reading, error can occur due to Spark's columnar vector is not compatible with
Gluten's.

### JSON functions (incompatible behavior)

Velox only supports double quotes surrounded strings, not single quotes, in JSON data. If single quotes are used, gluten will produce incorrect result.

### Lookaround pattern for regexp functions (fallback behavior)

In velox, lookaround (lookahead/lookbehind) pattern is not supported in RE2-based implementations for Spark functions,
such as `rlike`, `regexp_extract`, etc.

### FileSource format (fallback behavior)
Currently, Gluten only fully supports parquet file format. If other format is used, scan operator will fall back to vanilla spark.

### Parquet read conf (incompatible behavior)
Gluten supports `spark.files.ignoreCorruptFiles` and `spark.files.ignoreMissingFiles` with default false, if true, the behavior is same as config false.
Gluten ignores `spark.sql.parquet.datetimeRebaseModeInRead`, it only returns what write in parquet file. It does not consider the difference between legacy
hybrid (Julian Gregorian) calendar and Proleptic Gregorian calendar. The result may be different with vanilla spark.

### Parquet write conf (incompatible behavior)

Spark has `spark.sql.parquet.datetimeRebaseModeInWrite` config to decide whether legacy hybrid (Julian + Gregorian) calendar 
or Proleptic Gregorian calendar should be used during parquet writing for dates/timestamps. If the parquet to read is written
by Spark with this config as true, Velox's TableScan will output different result when reading it back.

### Partitioned Table Scan (fallback behavior)
Gluten only support the partitioned table scan when the file path contain the partition info, otherwise will fall back to vanilla spark.

### NaN support (incompatible behavior)
Velox does NOT support NaN. So unexpected result can be obtained for a few cases, e.g., comparing a number with NaN.

### File compression codec (exception)

Some compression codecs are not supported in Velox on certain file format.
Exception occurs when Velox TableScan is used to read files with unsupported compression codec.

| File Format | none | zlib | zstd | snappy | lzo | lz4 | gzip |
|-------------|------|------|------|--------|-----|-----|------|
| Parquet     | Y    | N    | Y    | Y      | N   | N   | Y    |
| DWRF        | Y    | Y    | Y    | Y      | Y   | Y   | N    |


### Native Write

#### Offload native write to velox

We implemented write support by overriding the following vanilla Spark classes. And you need to ensure preferentially load the Gluten jar to overwrite the jar of vanilla spark. Refer to [How to prioritize loading Gluten jars in Spark](https://github.com/oap-project/gluten/blob/main/docs/velox-backend-troubleshooting.md#incompatible-class-error-when-using-native-writer). It should be noted that if the user also modifies the following overriding classes, the user's changes may be overwritten.

```
./shims/spark32/src/main/scala/org/apache/spark/sql/hive/execution/HiveFileFormat.scala
./shims/spark32/src/main/scala/org/apache/spark/sql/execution/datasources/parquet/ParquetFileFormat.scala
./shims/spark32/src/main/scala/org/apache/spark/sql/execution/datasources/orc/OrcFileFormat.scala
./shims/spark32/src/main/scala/org/apache/spark/sql/execution/stat/StatFunctions.scala
./shims/spark32/src/main/scala/org/apache/spark/sql/execution/datasources/BasicWriteStatsTracker.scala
./shims/spark32/src/main/scala/org/apache/spark/sql/execution/datasources/FileFormatDataWriter.scala
./shims/spark32/src/main/scala/org/apache/spark/sql/execution/datasources/FileFormatWriter.scala

```

### Velox Parquet Write

#### Configuration (incompatible behavior)

Parquet write only support three configs, other will not take effect.

- compression code:
  - sql conf: `spark.sql.parquet.compression.codec`
  - option: `compression.codec`
- block size
  - sql conf: `spark.gluten.sql.columnar.parquet.write.blockSize`
  - option: `parquet.block.size`
- block rows
  - sql conf: `spark.gluten.sql.native.parquet.write.blockRows`
  - option: `parquet.block.rows`

#### Static partition write

Velox exclusively supports static partition writes and does not support dynamic partition writes.

```scala
spark.sql("CREATE TABLE t (c int, d long, e long) STORED AS PARQUET partitioned by (c, d)")
spark.sql("INSERT OVERWRITE TABLE t partition(c=1, d=2) SELECT 3 as e")
```

#### Write a dynamic partitioned or bucketed table (exception)

Velox does not support dynamic partition write and bucket write, e.g.,

```scala
spark.range(100).selectExpr("id as c1", "id % 7 as p")
  .write
  .format("parquet")
  .partitionBy("p")
  .save(f.getCanonicalPath)
```

#### CTAS (exception)

Velox does not create table as select, e.g.,

```scala
spark.range(100).toDF("id")
  .write
  .format("parquet")
  .saveAsTable("velox_ctas")
```

### Spill

`OutOfMemoryExcetpion` may still be triggered within current implementation of spill-to-disk feature, when shuffle partitions is set to a large number. When this case happens, please try to reduce the partition number to get rid of the OOM.

### TableScan on data types

- Byte type (fallback behavior)
- Timestamp type

  Only reading with INT96 and dictionary encoding is supported. When reading INT64 represented millisecond/microsecond timestamps, or INT96 represented timestamps of other encodings, exceptions can occur.

- Complex types
  - Parquet scan of nested array with struct or array as element type is not supported in Velox (fallback behavior).
  - Parquet scan of nested map with struct as key type, or array type as value type is not supported in Velox (fallback behavior).

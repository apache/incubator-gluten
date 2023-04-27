---
layout: page
title: Compatibility
nav_order: 6
---
This document describes some corner cases which will throw exception or has different appearance, cannot successfully fall back to vanilla.

### Runtime BloomFilter

Because velox BloomFilter implementation is different with spark, so if the query fallbacks might_contain to velox but offload bloom_filter_agg to velox, it will throw exception.

#### example

```sql
SELECT might_contain(null, null) both_null,
       might_contain(null, 1L) null_bf,
       might_contain((SELECT bloom_filter_agg(cast(id as long)) from range(1, 10000)),
            null) null_value
```

Will throw exception

```
Unexpected Bloom filter version number (512)
java.io.IOException: Unexpected Bloom filter version number (512)
 at org.apache.spark.util.sketch.BloomFilterImpl.readFrom0(BloomFilterImpl.java:256)
 at org.apache.spark.util.sketch.BloomFilterImpl.readFrom(BloomFilterImpl.java:265)
 at org.apache.spark.util.sketch.BloomFilter.readFrom(BloomFilter.java:178)
```

#### Solution

Set the gluten config `spark.gluten.sql.native.bloomFilter=false`, it will fallback to vanilla bloom filter, you can also disable runtime filter by setting spark config `spark.sql.optimizer.runtime.bloomFilter.enabled=false`

### ANSI (fallback behavior)

Gluten currently doesn't support ANSI mode, if Spark configured ansi, gluten will fallback to vanilla Spark.

### Case Sensitive mode (incompatible behavior)

Gluten only supports spark default case-insensitive mode, if case-sensitive, may get incorrect result.

### Spark's columnar reading (fatal error)

If the user enables Spark's columnar reading, error can occur due to Spark's columnar vector is not compatible with
Gluten's.

### JSON FUNCTION (incompatible behavior)

Gluten only supports double quotes surrounded strings, not single quotes, in JSON data. If user use single quotes, will get incorrect result.

### Lookaround pattern for regexp functions (fallback behavior)

In velox, lookaround (lookahead/lookbehind) pattern is not supported in RE2-based implementations for Spark functions,
such as `rlike`, `regexp_extract`, etc.

### FileSource format (fallback behavior)
Gluten only supports parquet, if is other format, will fallback to vanilla spark.

### Parquet read conf (incompatible behavior)
Gluten supports `spark.files.ignoreCorruptFiles` and `spark.files.ignoreMissingFiles` with default false, if true, the behavior is same as config false.
Gluten ignore `spark.sql.parquet.datetimeRebaseModeInRead`, it only returns what write in parquet file. It does not consider the difference between legacy hybrid (Julian Gregorian) calendar and Proleptic Gregorian calendar. The result maybe different with vanilla spark.

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

### Describe() method in DataFrame can not work (wrong result)
The df.describe() method can not work in Gluten with spark 3.2 and spark 3.3, which is a bug in vanilla spark. Already fixed in vanilla spark 3.3 [here](https://github.com/apache/spark/pull/40914). And we will keep this issue in spark3.2 and 3.3. And it will be fixed after upgrading the spark version to 3.4.
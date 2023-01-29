This document discribes some corner cases which will throw exception or has different apperance, cannot successfully fallback to vanilla.

# Runtime BloomFilter

Because velox BloomFilter implemention is different with spark, so if the query fallbacks might_contain to velox but offload bloom_filter_agg to velox, it will throw exception.

## example

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

## Solution

Set the gluten config `spark.gluten.sql.native.bloomFilter=false`, it will fallback to vanilla bloom filter, you can also disable runtime filter by setting spark config `spark.sql.optimizer.runtime.bloomFilter.enabled=false`

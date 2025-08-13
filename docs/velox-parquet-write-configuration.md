layout: page
title: Parquet write configuration
nav_order: 17

## Parquet write configurations in Spark/Velox/Gluten
|                                                   | parquet-mr default                         | Spark default | Velox Default | Gluten Support |
|---------------------------------------------------| ------------------------------------------ |---------------| ------------- |----------------|
| -------------------Spark----------------          |                                            |               |               |                |
| spark.sql.parquet.outputTimestampType             |                                            | int96         |               |                |
| spark.sql.parquet.writeLegacyFormat               |                                            | false         |               |                |
| -------------------Velox/Arrow----------------    |                                            |               |               |                |
| write_batch_size                                  |                                            |               | 1024          | Y (batch size) |
| rowgroup_length                                   |                                            |               | 1M            |                |
| compression_level                                 |                                            |               | 0             |                |
| page_index                                        |                                            |               | false         |                |
| decimal_as_integer                                |                                            |               | false         |                |
| statistics_enabled                                |                                            |               | false         |                |
| -------------------parquet-mr----------------     |                                            |               |               |                |
| parquet.summary.metadata.level                    | all                                        |               |               |                |
| parquet.enable.summary-metadata                   | true                                       |               |               |                |
| parquet.block.size                                | 128m                                       |               |               | Y              |
| parquet.page.size                                 | 1m                                         |               | 1M            | Y              |
| parquet.compression                               | uncompressed                               | snappy        | uncompressed  | Y              |
| parquet.write.support.class                       | org.apache.parquet.hadoop.api.WriteSupport |               |               |                |
| parquet.enable.dictionary                         | true                                       |               | true          | Y              |
| parquet.dictionary.page.size                      | 1m                                         |               | 1m            |                |
| parquet.validation                                | false                                      |               |               |                |
| parquet.writer.version                            | PARQUET_1_0                                |               | PARQUET_2_6   | Y              |
| parquet.memory.pool.ratio                         | 0.95                                       |               |               |                |
| parquet.memory.min.chunk.size                     | 1m                                         |               |               |                |
| parquet.writer.max-padding                        | 8m                                         |               |               |                |
| parquet.page.size.row.check.min                   | 100                                        |               |               |                |
| parquet.page.size.row.check.max                   | 10000                                      |               |               |                |
| parquet.page.value.count.threshold                | Integer.MAX_VALUE / 2                      |               |               |                |
| parquet.page.size.check.estimate                  | true                                       |               |               |                |
| parquet.columnindex.truncate.length               | 64                                         |               |               |                |
| parquet.statistics.truncate.length                | 2147483647                                 |               |               |                |
| parquet.bloom.filter.enabled                      | false                                      |               |               |                |
| parquet.bloom.filter.adaptive.enabled             | false                                      |               |               |                |
| parquet.bloom.filter.candidates.number            | 5                                          |               |               |                |
| parquet.bloom.filter.expected.ndv                 |                                            |               |               |                |
| parquet.bloom.filter.fpp                          | 0.01                                       |               |               |                |
| parquet.bloom.filter.max.bytes                    | 1m                                         |               |               |                |
| parquet.decrypt.off-heap.buffer.enabled           | false                                      |               |               |                |
| parquet.page.row.count.limit                      | 20000                                      |               |               |                |
| parquet.page.write-checksum.enabled               | true                                       |               | false         |                |
| parquet.crypto.factory.class                      | None                                       |               |               |                |
| parquet.compression.codec.zstd.bufferPool.enabled | true                                       |               |               |                |
| parquet.compression.codec.zstd.level              | 3                                          |               | 0             | Y              |
| parquet.compression.codec.zstd.workers            | 0                                          |               |               |                |

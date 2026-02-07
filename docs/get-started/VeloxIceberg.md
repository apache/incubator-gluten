---
layout: page
title: Iceberg Support in Velox Backend
nav_order: 8
parent: Getting-Started
---

# Iceberg Support in Velox Backend

## Supported Spark version

All the spark version is supported, but for convenience, only Spark 3.4 is well tested.
Now only read is supported in Gluten.

## Support Status
Following value indicates the iceberg support progress:

| Value          | Description                                                                |
|----------------|----------------------------------------------------------------------------|
| Offload        | Offload to the Velox backend                                               |
| PartialOffload | Some operators offload and some fallback                                   |
| Fallback       | Fallback to spark to execute                                               |
| Exception      | Cannot fallback by some conditions, throw the exception                    |
| ResultMismatch | Some hidden bug may cause result mismatch, especially for some corner case |

## Adding catalogs
Fallback

## Creating a table
Fallback

## Writing
Fallback
````
INSERT INTO local.db.table VALUES (1, 'a'), (2, 'b'), (3, 'c');
````
PartialOffload

The write is fallback while read is offload.
````
INSERT INTO local.db.table SELECT id, data FROM source WHERE length(data) = 1;
````

## Reading
### Read data
Offload/Fallback

| Table Type  | No Delete       | Position Delete | Equality Delete |
|-------------|-----------------|-----------------|-----------------|
| unpartition | Offload         | Offload         | Fallback        |
| partition   | Fallback mostly | Fallback mostly | Fallback        |
| metadata    | Fallback        |                 |                 |

Offload the simple query.
````
SELECT count(1) as count, data
FROM local.db.table
GROUP BY data;
````

If delete by Spark and copy on read, will generate position delete file, the query may offload.

If delete by Flink, may generate the equality delete file, fallback in tht case.

Now we only offload the simple query, for partition table, many operators are fallback by Expression
StaticInvoke such as BucketFunction, wait to be supported.

DataFrame reads are supported and can now reference tables by name using spark.table:

````
val df = spark.table("local.db.table")
df.count()
````

### Read metadata
Fallback
````
SELECT data, _file FROM local.db.table;
````

## DataType
Timestamptz in orc format is not supported, throws exception.
UUID type and Fixed type is fallback.

## Format
PartialOffload

Supports parquet and orc format.
Not support avro format.

## SQL
Only support SELECT.

## Schema evolution
PartialOffload

Gluten uses column name to match the parquet file, so if the column is renamed or
the added column name is same to the deleted column, the scan will fall back.

## Configuration
### Catalogs
All the catalog configurations are transparent to Gluten

### SQL Extensions
Fallback

Supports the option `spark.sql.extensions`, fallback the SQL command `CALL`.

### Runtime configuration
The "Gluten Support" column is now ready to be populated with:

✅ Supported<br>
❌ Not Supported<br>
⚠️ Partial Support<br>
🔄 In Progress<br>
🚫 Not applied or transparent to Gluten<br>

### Spark SQL Options
| Spark option | Default | Description | Gluten Support |
| --- | --- | --- | --- |
| spark.sql.iceberg.vectorization.enabled | Table default | Enables vectorized reads of data files |❌|
| spark.sql.iceberg.parquet.reader-type | ICEBERG | Sets Parquet reader implementation (ICEBERG,COMET) |✅ |
| spark.sql.iceberg.check-nullability | true | Validate that the write schema's nullability matches the table's nullability |✅ |
| spark.sql.iceberg.check-ordering | true | Validates the write schema column order matches the table schema order |✅ |
| spark.sql.iceberg.planning.preserve-data-grouping | false | When true, co-locate scan tasks for the same partition in the same read split, used in Storage Partitioned Joins |✅ |
| spark.sql.iceberg.aggregate-push-down.enabled | true | Enables pushdown of aggregate functions (MAX, MIN, COUNT) | |
| spark.sql.iceberg.distribution-mode | See Spark Writes | Controls distribution strategy during writes | ✅ |
| spark.wap.id | null | Write-Audit-Publish snapshot staging ID | |
| spark.wap.branch | null | WAP branch name for snapshot commit | |
| spark.sql.iceberg.compression-codec | Table default | Write compression codec (e.g., zstd, snappy) | |
| spark.sql.iceberg.compression-level | Table default | Compression level for Parquet/Avro | |
| spark.sql.iceberg.compression-strategy | Table default | Compression strategy for ORC | |
| spark.sql.iceberg.data-planning-mode | AUTO | Scan planning mode for data files (AUTO, LOCAL, DISTRIBUTED) | |
| spark.sql.iceberg.delete-planning-mode | AUTO | Scan planning mode for delete files (AUTO, LOCAL, DISTRIBUTED) | |
| spark.sql.iceberg.advisory-partition-size | Table default | Advisory size (bytes) used for writing to the Table when Spark's Adaptive Query Execution is enabled. Used to size output files | |
| spark.sql.iceberg.locality.enabled | false | Report locality information for Spark task placement on executors |✅ |
| spark.sql.iceberg.executor-cache.enabled | true | Enables cache for executor-side (currently used to cache Delete Files) |❌|
| spark.sql.iceberg.executor-cache.timeout | 10 | Timeout in minutes for executor cache entries |❌|
| spark.sql.iceberg.executor-cache.max-entry-size | 67108864 (64MB) | Max size per cache entry (bytes) |❌|
| spark.sql.iceberg.executor-cache.max-total-size | 134217728 (128MB) | Max total executor cache size (bytes) |❌|
| spark.sql.iceberg.executor-cache.locality.enabled | false | Enables locality-aware executor cache usage |❌|
| spark.sql.iceberg.merge-schema | false | Enables modifying the table schema to match the write schema. Only adds columns missing columns |✅|
| spark.sql.iceberg.report-column-stats | true | Report Puffin Table Statistics if available to Spark's Cost Based Optimizer. CBO must be enabled for this to be effective |✅|

#### Read options
| Spark option | Default | Description | Gluten Support |
| --- | --- | --- | --- |
| snapshot-id | (latest) | Snapshot ID of the table snapshot to read |✅|
| as-of-timestamp | (latest) | A timestamp in milliseconds; the snapshot used will be the snapshot current at this time. |✅|
| split-size | As per table property | Overrides this table's read.split.target-size and read.split.metadata-target-size |✅|
| lookback | As per table property | Overrides this table's read.split.planning-lookback |✅|
| file-open-cost | As per table property | Overrides this table's read.split.open-file-cost |✅|
| vectorization-enabled | As per table property | Overrides this table's read.parquet.vectorization.enabled |❌|
| batch-size | As per table property | Overrides this table's read.parquet.vectorization.batch-size |❌|
| stream-from-timestamp | (none) | A timestamp in milliseconds to stream from; if before the oldest known ancestor snapshot, the oldest will be used | |
| streaming-max-files-per-micro-batch | INT_MAX | Maximum number of files per microbatch | |
| streaming-max-rows-per-micro-batch | INT_MAX | Maximum number of rows per microbatch | |

#### Write options

| Spark option | Default | Description | Gluten Support |
| --- | --- | --- | --- |
| write-format | Table write.format.default | File format to use for this write operation; parquet, avro, or orc |⚠️ Parquet only|
| target-file-size-bytes | As per table property | Overrides this table's write.target-file-size-bytes | |
| check-nullability | true | Sets the nullable check on fields | |
| snapshot-property.custom-key | null | Adds an entry with custom-key and corresponding value in the snapshot summary (the snapshot-property. prefix is only required for DSv2) | |
| fanout-enabled | false | Overrides this table's write.spark.fanout.enabled |✅|
| check-ordering | true | Checks if input schema and table schema are same | |
| isolation-level | null | Desired isolation level for Dataframe overwrite operations. null => no checks (for idempotent writes), serializable => check for concurrent inserts or deletes in destination partitions, snapshot => checks for concurrent deletes in destination partitions. | |
| validate-from-snapshot-id | null | If isolation level is set, id of base snapshot from which to check concurrent write conflicts into a table. Should be the snapshot before any reads from the table. Can be obtained via Table API or Snapshots table. If null, the table's oldest known snapshot is used. | |
| compression-codec | Table write.(fileformat).compression-codec | Overrides this table's compression codec for this write | |
| compression-level | Table write.(fileformat).compression-level | Overrides this table's compression level for Parquet and Avro tables for this write | |
| compression-strategy | Table write.orc.compression-strategy | Overrides this table's compression strategy for ORC tables for this write | |
| distribution-mode | See Spark Writes for defaults | Override this table's distribution mode for this write |🚫|
| delete-granularity | file | Override this table's delete granularity for this write | |

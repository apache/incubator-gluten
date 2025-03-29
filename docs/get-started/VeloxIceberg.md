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

## Configuration
### Catalogs
Supports all the catalog options, which is not used in native engine.

### SQL Extensions
Fallback

Supports the option `spark.sql.extensions`, fallback the SQL command `CALL`.

### Runtime configuration
#### Read options

| Spark option	         | Status      |
|-----------------------|-------------|
| snapshot-id           | Support     |
| as-of-timestamp       | Support     |
| split-size            | Support     |
| lookback              | Support     |
| file-open-cost        | Support     |
| vectorization-enabled | Not Support |
| batch-size            | Not Support |

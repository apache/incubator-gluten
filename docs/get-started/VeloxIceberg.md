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
Offload/Exception
````
SELECT count(1) as count, data
FROM local.db.table
GROUP BY data;
````
Offload/Exception

DataFrame reads are supported and can now reference tables by name using spark.table:
````
val df = spark.table("local.db.table")
df.count()
````
Restriction:
1. Scan operator in other operator such as delete
2. Scan metadata table is not tested

## DataType
Timestamptz in orc format is not supported, throws exception

## Format
PartialOffload

Supports parquet and orc format.
Not support avro format.

## SQL
Only support SELECT.

## Configuration
Not support.

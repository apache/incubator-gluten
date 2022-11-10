# Test on Velox backend with TPC-DS workload

## Test dataset
Parquet format is supported. Here are the steps to generate the testing datasets:

### Generate the Parquet dataset
Please refer to the scripts in ./gen_data/parquet_dataset/ directory to generate parquet dataset. Note this script relies on the [spark-sql-perf](https://github.com/databricks/spark-sql-perf) package from Databricks.

In tpcds_datagen_parquet.sh, several parameters should be configured according to the system.
```
spark_sql_perf_jar=/PATH/TO/spark-sql-perf_2.12-0.5.1-SNAPSHOT.jar
...
  --num-executors 14 
  --executor-cores 8 
  --conf spark.sql.shuffle.partitions=224 
...
```

In tpcds_datagen_parquet.scala, the parameters and dirs should be configured as well.
```
val scaleFactor = "100" // scaleFactor defines the size of the dataset to generate (in GB).
val numPartitions = 200  // how many dsdgen partitions to run - number of input tasks.
...
val rootDir = "/PATH/TO/TPCDS_PARQUET_PATH" // root directory of location to create data in.
val dbgenDir = "/PATH/TO/TPCDS_DBGEN" // location of dbgen
```

Currently, Gluten with Velox can support Parquet file format and three compression codec including snappy, gzip, zstd.

## Test Queries
We provided the test queries in ./tpcds.queries.updated directory, which changed all DATE fields to STRING since DATE type is not well supported in Gluten with Velox backend.
We also provided a scala code in ./run_tpcds/ directory about how to run TPC-DS queries.

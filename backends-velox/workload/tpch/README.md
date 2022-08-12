#Test on Velox backends with TPCH workloads

## Test datasets
As Parquet format is not perfectly supported by Velox right now, DWRF(a fork of the ORC file format) format files are used. Here's the steps to generate the testing datasets:

### Generate the Parquet dataset.
Please refer to the scripts in ./gen_data/parquet_dataset/ directory to generate parquet dataset. Note this script relies on the [spark-sql-perf](https://github.com/databricks/spark-sql-perf) package from Databricks.

In tpch_datagen_parquet.sh, several parameters should be configured according to the system.
```
spark_sql_perf_jar=/PATH/TO/spark-sql-perf_2.12-0.5.1-SNAPSHOT.jar
...
  --num-executors 14 
  --executor-cores 8 
  --conf spark.sql.shuffle.partitions=224 
...
```

In tpch_datagen_parquet.scala, the parameters and dirs should be configured as well.
```
val scaleFactor = "100" // scaleFactor defines the size of the dataset to generate (in GB).
val numPartitions = 200  // how many dsdgen partitions to run - number of input tasks.
...
val rootDir = "/PATH/TO/TPCH_PARQUET_PATH" // root directory of location to create data in.
val dbgenDir = "/PATH/TO/TPCH_DBGEN" // location of dbgen
```

### Convert the Parquet dataset to DWRF dataset
And then please refer to the scripts in ./gen_data/dwrf_dataset/ directory to convert the Parquet dataset to DWRF dataset.

In tpch_convert_parquet_dwrf.sh, spark configures should be set according to the system.

```
export GLUTEN_HOME=/home/sparkuser/workspace/workspace/Gluten_Perf_ICX/
...
--executor-cores 8                                      \
--num-executors 14                                       \
```

In tpch_convert_parquet_dwrf.scala, the table path should be configured.
```
val parquet_file_path = "/PATH/TO/TPCH_PARQUET_PATH"
val dwrf_file_path = "/PATH/TO/TPCH_DWRF_PATH"
```

## Test Queries
We provided the test queries in ./tpch.queries.updated directory, which changed all DATE feilds to STRING since DATE type is not well supported in Gluten with Velox backend.
We also provided a scala code in ./run_tpch/ directory about how to run TPC-H queries.

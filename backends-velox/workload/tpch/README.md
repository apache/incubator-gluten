#Test on Velox backends with TPCH workloads

## Test datasets
As Parquet format is not perfectly supported by Velox right now, DWRF(a fork of the ORC file format) format files are used. Here's the steps to generate the testing datasets:

### Generate the Parquet dataset.
Please refer to the scripts in ./gen_data/parquet_dataset/ directory to generate parquet dataset. Note this script relies on the [spark-sql-perf](https://github.com/databricks/spark-sql-perf) package from Databricks.

### Convert the Parquet dataset to DWRF dataset
And then please refer to the scripts in ./gen_data/dwrf_dataset/ directory to convert the Parquet dataset to DWRF dataset.

## Test Queries
We provided the test queries in ./tpch.queries.updated directory, which changed all DATE feilds to STRING since DATE type is not well supported in Gluten with Velox backend.
We also provided a scala code in ./run_tpch/ directory about how to run TPC-H queries.

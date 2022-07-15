# Test datasets

### Generate the Parquet dataset.
You can refer to the scripts in ./gen_data/dwrf_dataset/ directory to generate parquet dataset.

### Convert the Parquet dataset to DWRF dataset
And then you can refer to the scripts in gen_data/parquet_dataset/ directory to convert dwrf dataset.

# Test Queries
We provided the test queries in ./tpch.queries.updated directory, which changed Date to String since Date hasn't been supported in Gluten with Velox backend.

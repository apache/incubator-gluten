
Run an example:
```bash
  cd gluten/cpp/velox/benchmarks &&
  ./generic_benchmark \
  --benchmark_filter=InputFromBatchVector \
  hash_join.json \
  $PWD/data/parquet/bm_lineitem/part-00000-8bd1ea02-5f13-449f-b7ef-e32a0f11583d-c000.snappy.parquet \
  $PWD/data/parquet/bm_part/part-00000-d8bbcbeb-f056-4b7f-8f80-7e5ee7260b9f-c000.snappy.parquet
```

Generate data and run a query fragment of TPC-H Q17:

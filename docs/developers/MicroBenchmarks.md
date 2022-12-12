# Generate Micro Benchmarks for Velox Backend

**This document explains how to use the existing micro benchmark template in Gluten Cpp.**

A micro benchmark for Velox backend is provided in Gluten Cpp to simulate the execution of a reducer stage in Spark.
It serves as a more convenient alternative to debug in Gluten Cpp comparing with directly debugging in a Spark job.
Developers can use it to create their own workloads, debug in native process, profile the hotspot and do optimizations.

Currently, data input files are in Parquet. We load the data into Arrow format, then add Arrow2Velox to feed 
the data into Velox pipeline to reproduce the reducer stage. Shuffle exchange is not included.

## Try the example

To run a micro benchmark, user should provide one file that contains the substrait plan in JSON format, and 
one or more input data files in parquet format.
The commands below help to generate example input files:

```shell
cd /path_to_gluten/ep/build-arrow/src
./get_arrow.sh
./build_arrow_for_velox.sh --build_test=ON --build_benchmarks=ON

cd /path_to_gluten/ep/build-velox/src
# get velox and compile
./get_velox.sh
./build_velox.sh

# set BUILD_TESTS and BUILD_BENCHMARKS = ON in gluten cpp compile shell
cd /path_to_gluten/cpp
./compile.sh --build_velox_backend=ON --build_test=ON --build_benchmarks=ON

# Build gluten. If you are using spark 3.3, replace -Pspark-3.2 with -Pspark-3.3
cd /path_to_gluten
mvn clean package -Pspark-3.2 -Pbackends-velox

mvn test -Pspark-3.2 -Pbackends-velox -pl backends-velox -am \
-DtagsToInclude="io.glutenproject.tags.GenerateExample" -Dtest=none -DfailIfNoTests=false -Darrow.version=10.0.0-SNAPSHOT -Dexec.skip
```

The generated example files are placed in gluten/backends-velox:
```shell
$ tree gluten/backends-velox/generated-native-benchmark/
gluten/backends-velox/generated-native-benchmark/
├── example.json
├── example_lineitem
│   ├── part-00000-3ec19189-d20e-4240-85ae-88631d46b612-c000.snappy.parquet
│   └── _SUCCESS
└── example_orders
    ├── part-00000-1e66fb98-4dd6-47a6-8679-8625dbc437ee-c000.snappy.parquet
    └── _SUCCESS
```

Run micro benchmark with the generated files as input. You need to specify the **absolute** path to the input files:
```shell
cd /path/to/gluten/cpp/velox/benchmarks
./generic_benchmark \
/home/sparkuser/github/oap-project/gluten/backends-velox/generated-native-benchmark/example.json \
/home/sparkuser/github/oap-project/gluten/backends-velox/generated-native-benchmark/example_orders/part-00000-1e66fb98-4dd6-47a6-8679-8625dbc437ee-c000.snappy.parquet \
/home/sparkuser/github/oap-project/gluten/backends-velox/generated-native-benchmark/example_lineitem/part-00000-3ec19189-d20e-4240-85ae-88631d46b612-c000.snappy.parquet \
--threads 1 --iterations 1 --noprint-result --benchmark_filter=InputFromBatchVector
```
The output should like:
```shell
2022-11-18T16:49:56+08:00
Running ./generic_benchmark
Run on (192 X 3800 MHz CPU s)
CPU Caches:
  L1 Data 48 KiB (x96)
  L1 Instruction 32 KiB (x96)
  L2 Unified 2048 KiB (x96)
  L3 Unified 99840 KiB (x2)
Load Average: 0.28, 1.17, 1.59
***WARNING*** CPU scaling is enabled, the benchmark real time measurements may be noisy and will incur extra overhead.
-- Project[expressions: (n3_0:BIGINT, ROW["n1_0"]), (n3_1:VARCHAR, ROW["n1_1"])] -> n3_0:BIGINT, n3_1:VARCHAR
   Output: 535 rows (65.81KB, 1 batches), Cpu time: 36.33us, Blocked wall time: 0ns, Peak memory: 1.00MB, Memory allocations: 3, Threads: 1
      queuedWallNanos    sum: 2.00us, count: 2, min: 0ns, max: 2.00us
  -- HashJoin[RIGHT SEMI (FILTER) n0_0=n1_0] -> n1_0:BIGINT, n1_1:VARCHAR
     Output: 535 rows (65.81KB, 1 batches), Cpu time: 191.56us, Blocked wall time: 0ns, Peak memory: 2.00MB, Memory allocations: 8
     HashBuild: Input: 582 rows (16.45KB, 1 batches), Output: 0 rows (0B, 0 batches), Cpu time: 1.84us, Blocked wall time: 0ns, Peak memory: 1.00MB, Memory allocations: 3, Threads: 1
        distinctKey0       sum: 583, count: 1, min: 583, max: 583
        queuedWallNanos    sum: 0ns, count: 1, min: 0ns, max: 0ns
        rangeKey0          sum: 59748, count: 1, min: 59748, max: 59748
     HashProbe: Input: 37897 rows (296.07KB, 1 batches), Output: 535 rows (65.81KB, 1 batches), Cpu time: 189.71us, Blocked wall time: 0ns, Peak memory: 1.00MB, Memory allocations: 5, Threads: 1
        queuedWallNanos    sum: 0ns, count: 1, min: 0ns, max: 0ns
    -- ArrowStream[] -> n0_0:BIGINT
       Input: 0 rows (0B, 0 batches), Output: 37897 rows (296.07KB, 1 batches), Cpu time: 1.29ms, Blocked wall time: 0ns, Peak memory: 0B, Memory allocations: 0, Threads: 1
    -- ArrowStream[] -> n1_0:BIGINT, n1_1:VARCHAR
       Input: 0 rows (0B, 0 batches), Output: 582 rows (16.45KB, 1 batches), Cpu time: 894.22us, Blocked wall time: 0ns, Peak memory: 0B, Memory allocations: 0, Threads: 1

-----------------------------------------------------------------------------------------------------------------------------
Benchmark                                                                   Time             CPU   Iterations UserCounters...
-----------------------------------------------------------------------------------------------------------------------------
InputFromBatchVector/iterations:1/process_time/real_time/threads:1   41304520 ns     23740340 ns            1 collect_batch_time=34.7812M elapsed_time=41.3113M

```

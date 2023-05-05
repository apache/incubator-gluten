---
layout: page
title: Micro Benchmarks for Velox Backend
nav_order: 3
parent: Developer Overview
---
# Generate Micro Benchmarks for Velox Backend

**This document explains how to use the existing micro benchmark template in Gluten Cpp.**

A micro benchmark for Velox backend is provided in Gluten Cpp to simulate the execution of a first or middle stage in Spark.
It serves as a more convenient alternative to debug in Gluten Cpp comparing with directly debugging in a Spark job.
Developers can use it to create their own workloads, debug in native process, profile the hotspot and do optimizations.

To simulate a first stage, you need to dump the Substrait plan into a JSON file.

To simulate a middle stage, in addition to the JSON file, you also need to save the input data of this stage into Parquet files.
The benchmark will load the data into Arrow format, then add Arrow2Velox to feed 
the data into Velox pipeline to reproduce the reducer stage. Shuffle exchange is not included.

Please refer to the sections below to learn how to dump the Substrait plan and create the input data files.

## Try the example

To run a micro benchmark, user should provide one file that contains the Substrait plan in JSON format, and optional 
one or more input data files in parquet format.
The commands below help to generate example input files:

```shell
cd /path_to_gluten/ep/build-arrow/src
./get_arrow.sh
./build_arrow.sh --build_tests=ON --build_benchmarks=ON

cd /path_to_gluten/ep/build-velox/src
# get velox and compile
./get_velox.sh
./build_velox.sh

# set BUILD_TESTS and BUILD_BENCHMARKS = ON in gluten cpp compile shell
cd /path_to_gluten/cpp
mkdir build
cd build
cmake -DBUILD_VELOX_BACKEND=ON -DBUILD_TESTS=ON -DBUILD_BENCHMARKS=ON ..
make -j

# Build gluten. If you are using spark 3.3, replace -Pspark-3.2 with -Pspark-3.3
cd /path_to_gluten
mvn clean package -Pspark-3.2 -Pbackends-velox

mvn test -Pspark-3.2 -Pbackends-velox -pl backends-velox -am \
-DtagsToInclude="io.glutenproject.tags.GenerateExample" -Dtest=none -DfailIfNoTests=false -Darrow.version=11.0.0-gluten -Dexec.skip
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
cd /path/to/gluten/cpp/build/velox/benchmarks
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

## Generate Substrait plan and input for any quey

Build the gluten debug version.

```shell
cd /path_to_gluten/cpp/
mkdir build
cd build
cmake -DBUILD_VELOX_BACKEND=ON -DBUILD_TESTS=ON -DBUILD_BENCHMARKS=ON -DCMAKE_BUILD_TYPE=RelWithDebInfo ..
make -j
```
Run the query by spark-shell, and get the Stage Id from spark UI.
Get the Substrait plan from console output.

Example:
```shell
################################################## received substrait::Plan:
Task stageId: 2, partitionId: 855, taskId: 857; {"extensions":[{"extensionFunction":{"name":"sum:req_i32"}}],"relations":[{"root":{"input":{"fetch":{"common":{"direct":{}},"input":{"project":{"common":{"direct":{}},"input":{"aggregate":{"common":{"direct":{}},"input":{"read":{"common":{"direct":{}},"baseSchema":{"names":["i_product_name#15","i_brand#16","spark_grouping_id#14","sum#22"],"struct":{"types":[{"string":{"nullability":"NULLABILITY_NULLABLE"}},{"string":{"nullability":"NULLABILITY_NULLABLE"}},{"i64":{"nullability":"NULLABILITY_REQUIRED"}},{"i64":{"nullability":"NULLABILITY_NULLABLE"}}]}},"localFiles":{"items":[{"uriFile":"iterator:0"}]}}},"groupings":[{"groupingExpressions":[{"selection":{"directReference":{"structField":{}}}},{"selection":{"directReference":{"structField":{"field":1}}}},{"selection":{"directReference":{"structField":{"field":2}}}}]}],"measures":[{"measure":{"phase":"AGGREGATION_PHASE_INTERMEDIATE_TO_RESULT","outputType":{"i64":{"nullability":"NULLABILITY_NULLABLE"}},"arguments":[{"value":{"selection":{"directReference":{"structField":{"field":3}}}}}]}}]}},"expressions":[{"selection":{"directReference":{"structField":{"field":3}}}}]}},"count":"100"}},"names":["qoh#10"]}}]}
```

| Parameters | Description | Recommend Setting |
| ---------- | ----------- | --------------- |
| spark.gluten.sql.debug | Whether open debug mode | true |
| spark.gluten.sql.benchmark_task.stageId | Spark task stage id | 2 |
| spark.gluten.sql.benchmark_task.partitionId | Spark task partition id, default value -1 means all the partition of this stage | -1 |
| spark.gluten.sql.benchmark_task.taskId | If not specify partition id, use spark task attempt id, default value -1 means all the partition of this stage | -1 |
| spark.gluten.saveDir | Directory should exist and be empty, save the stage input to this directory, parquet name format is input_${taskId}_${iteratorIndex}_${partitionId}.parquet | /path/to/saveDir |

Save the Substrait plan to a json file, suppose the name is "plan.json", and output is /tmp/save/input_34_0_1.parquet and /tmp/save/input_34_0_2.parquet, please use spark to combine the 2 files to 1 file.

```java
val df = spark.read.format("parquet").load("/tmp/save")
df.repartition(1).write.format("parquet").save("/tmp/new_save")
```

The first arg is the json query file path, the following args are file iterators.

```json
"localFiles": {
    "items": [{
            "uriFile": "iterator:0"
        }
    ]
}
```

Run benchmark.

```shell
cd /path/to/gluten/cpp/build/velox/benchmarks
./generic_benchmark \
/plan/to/plan.json \
/tmp/new_save/generate.parquet \
--threads 1 --noprint-result
```

For some complex queries, stageId may cannot represent the Substrait plan input, please get the taskId from spark UI, and get your target parquet from saveDir.

In this example, only one partition input with partition id 2, taskId is 36, iterator length is 2.

```shell
cd /path/to/gluten/cpp/build/velox/benchmarks
./generic_benchmark \
/plan/to/complex_plan.json \
/tmp/save/input_36_0_2.parquet /tmp/save/input_36_1_2.parquet \
--threads 1 --noprint-result
```

## Save ouput to parquet to analyze

You can also save the output to a parquet file to analyze.

```shell
cd /path/to/gluten/cpp/build/velox/benchmarks
./generic_benchmark \
/plan/to/plan.json \
/tmp/save/input_1.parquet /tmp/save/input_2.parquet \
--threads 1 --noprint-result --write-file=/path/to/result.parquet
```

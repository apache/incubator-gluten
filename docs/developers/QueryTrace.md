---
layout: page
title: How To Use Gluten
nav_order: 1
parent: Developer Overview
---

# Background
Currently, we have [MicroBenchmarks](https://github.com/apache/incubator-gluten/blob/main/docs/developers/MicroBenchmarks.md) to profile the Velox plan execution in stage level, now we have query trace replayer to specify plan node id to profile in operator level.

We cannot profile operator level directly because ValueStreamNode cannot serialize and deserialize, so we should generate benchmark first, and then enable query trace in benchmark which replaces the ValueStreamNode to ValuesNode.

Relevant link:
https://facebookincubator.github.io/velox/develop/debugging/tracing.html

https://facebookincubator.github.io/velox/configs.html#tracing

https://velox-lib.io/blog/velox-query-tracing

# Usage
Firstly, we should use MicroBenchmark to generate the input parquet data file and substrait json plan.

```shell
/tmp/saveDir
├── conf_35_0.ini
├── data_35_0_0.parquet
├── plan_35_0.json
└── split_35_0_0.json
```
Secondly, execute the benchmark to get the task id and query trace node id, for example, the task id is `Gluten_Stage_0_TID_0_VTID_0` and we will profile partial aggregation if node id `7`.

```shell
W0113 15:30:18.903225 2524110 GenericBenchmark.cc:251] Setting CPU for thread 0 to 0
I0113 15:30:19.093111 2524110 Task.cpp:2051] Terminating task Gluten_Stage_0_TID_0_VTID_0 with state Finished after running for 142ms
W0113 15:30:19.093384 2524110 GenericBenchmark.cc:485] {Task Gluten_Stage_0_TID_0_VTID_0 (Gluten_Stage_0_TID_0_VTID_0) Finished
Plan:
-- Project[8][expressions: (n8_3:INTEGER, hash_with_seed(42,"n6_5","n6_6")), (n8_4:DOUBLE, "n6_5"), (n8_5:DOUBLE, "n6_6"), (n8_6:BIGINT, "n7_2")] -> n8_3:INTEGER, n8_4:DOUBLE, n8_5:DOUBLE, n8_6:BIGINT
  -- Aggregation[7][PARTIAL [n6_5, n6_6] n7_2 := sum_partial("n6_7")] -> n6_5:DOUBLE, n6_6:DOUBLE, n7_2:BIGINT
    -- Project[6][expressions: (n6_5:DOUBLE, "n5_7"), (n6_6:DOUBLE, "n5_8"), (n6_7:BIGINT, unscaled_value("n5_9"))] -> n6_5:DOUBLE, n6_6:DOUBLE, n6_7:BIGINT
      -- Project[5][expressions: (n5_7:DOUBLE, "n1_4"), (n5_8:DOUBLE, "n1_5"), (n5_9:DECIMAL(7, 2), "n1_6"), (n5_10:DOUBLE, "n1_7"), (n5_11:DOUBLE, "n3_1")] -> n5_7:DOUBLE, n5_8:DOUBLE, n5_9:DECIMAL(7, 2), n5_10:DOUBLE, n5_11:DOUBLE
        -- HashJoin[4][INNER n1_8=n3_2] -> n1_4:DOUBLE, n1_5:DOUBLE, n1_6:DECIMAL(7, 2), n1_7:DOUBLE, n1_8:DOUBLE, n3_1:DOUBLE, n3_2:DOUBLE
          -- Project[1][expressions: (n1_4:DOUBLE, "n0_0"), (n1_5:DOUBLE, "n0_1"), (n1_6:DECIMAL(7, 2), "n0_2"), (n1_7:DOUBLE, "n0_3"), (n1_8:DOUBLE, "n0_3")] -> n1_4:DOUBLE, n1_5:DOUBLE, n1_6:DECIMAL(7, 2), n1_7:DOUBLE, n1_8:DOUBLE
            -- TableScan[0][table: hive_table, remaining filter: (isnotnull("sr_store_sk"))] -> n0_0:DOUBLE, n0_1:DOUBLE, n0_2:DECIMAL(7, 2), n0_3:DOUBLE
          -- Project[3][expressions: (n3_1:DOUBLE, "n2_0"), (n3_2:DOUBLE, "n2_0")] -> n3_1:DOUBLE, n3_2:DOUBLE
            -- ValueStream[2][] -> n2_0:DOUBLE
```

Thirdly, execute the benchmark and enable the query trace.

```shell
 ./generic_benchmark --conf /tmp/saveDir/conf_35_0.ini --plan /tmp/saveDir/plan_35_0.json \  --split /tmp/saveDir/split_35_0_0.json --data /tmp/saveDir/data_35_0_0.parquet  --query_trace_enabled=true --query_trace_dir=/tmp/query_trace --query_trace_node_ids=7 --query_trace_max_bytes=100000000000   --query_trace_task_reg_exp=Gluten_Stage_0_TID_0_VTID_0 --threads 1 --noprint-result
```

We can see the following message, which indicates the query trace runs successfully.
```shell
I0114 11:06:52.461263 2587302 Task.cpp:3072] Trace input for plan nodes 7 from task Gluten_Stage_0_TID_0_VTID_0
I0114 11:06:52.462595 2587302 Operator.cpp:135] Trace input for operator type: PartialAggregation, operator id: 5, pipeline: 0, driver: 0, task: Gluten_Stage_0_TID_0_VTID_0
```

Now we can see the data in query trace directory `/tmp/query_trace`.

```shell
/tmp/query_trace/
└── Gluten_Stage_0_TID_0_VTID_0
    ├── 7
    │   └── 0
    │       └── 0
    │           ├── op_input_trace.data
    │           └── op_trace_summary.json
    └── task_trace_meta.json
```
Fourthly, replay the query. Now we still have some issues to fix, we don't have the query id.
Wait for this issue to fix https://github.com/facebookincubator/velox/issues/12084

```shell
/mnt/DP_disk1/code/velox/build/velox/tool/trace# ./velox_query_replayer  --root_dir /tmp/query_trace --task_id Gluten_Stage_0_TID_0_VTID_0 --summary

terminate called after throwing an instance of 'facebook::velox::VeloxUserError'
  what():  Exception: VeloxUserError
Error Source: USER
Error Code: INVALID_ARGUMENT
Reason: --query_id must be provided
Retriable: False
Expression: !FLAGS_query_id.empty()
Function: init
File: /mnt/DP_disk1/code/velox/velox/tool/trace/TraceReplayRunner.cpp
Line: 241

```
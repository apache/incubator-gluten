
# Introduction

## Problem Statement

Apache Spark is a stable, mature project that has been under development for many years. The project has proven to be one of the best frameworks to scale out of processing petabyte-scale datasets. However, the Spark community has had to address performance challenges that required various optimizations over time. A key optimization introduced in Spark 2.0 replaced Volcano mode with whole-stage code-generation to achieve a 2x speedup. Since then most of the optimization works at the query plan level. The operator's performance stopped to grow.

<p align="center">
<img src="https://user-images.githubusercontent.com/47296334/199614366-6209ed91-8955-4c7a-8829-e80ccdfaded9.png" width="800">
</p>

On the other side, SQL engine is researched for years. There are product or libraries like Clickhouse, Arrow or Velox. By using features like native implementation, columnar data format as well as vectorized data processing, these libraries outperform much of Spark's JVM based SQL eingine. However these libraries are running on single node.

## Gluten's Solution

“Gluten” is Latin for glue. Main goal of project Gluten is to “glue" the SparkSQL and native libraries. So we can take use of and benefit from Spark SQL's scale out framework as well native libraries' high performance.

The basic rule of Gluten's design is that we would reuse spark's whole control flow and as many JVM code as possible but offload the compute intensive data processing part to native code. Here is what Gluten does:
* Transform Spark’s whole stage physical plan to Substrait plan and send to native
* Offload performance critical data processing to native library
* Define clear JNI interfaces for native libraries
* Switch the native backends easily
* Reuse Spark’s distributed control flow
* Manage data sharing between JVM and native
* Extend support to native accelerators

## Architecture

The overview chart is like below. Spark physical plan is transformed to substrait plan. Substrait is to create a well defined cross-language specification for data compute operations. More details can be found from https://substrait.io/. Then substrait plan is passed to native through JNI call. In native the operator chain should be built and start to run. We use Spark3.0's columnar API as the data interface, so the native library should return Columnar Batch to Spark. We may need to wrap the columnar batch for each native backend. Gazelle engine's c++ code use Apache Arrow data format as its basic data format, so the returned data to Spark JVM is ArrowColumnarBatch.
<p align="center">
<img src="https://user-images.githubusercontent.com/47296334/199617207-1140698a-4d53-462d-9bc7-303d14be060b.png" width="800">
</p>
There are several native libraries we may offload. Currently we are working on Clickhouse and Velox as native backend. Velox is a C++ database acceleration library which provides reusable, extensible, and high-performance data processing components. More details can be found from https://github.com/facebookincubator/velox/. We also implemented a basic backend using Arrow Computer Engine which is for reference only. Gluten can also be easily extended to any accelerator libraries as backend.

There are several key component in GLuten:
* Query plan conversion which convert Spark's physical plan into substrait plan in each stage.
* Unified memory management in Spark is used to control the native memory allocation as well
* Columnar shuffle is used to shuffle columnar data directly. The shuffle service still reuse the one in Spark core. The exchange operator is reimplemented to support columnar data format
* For unsupported operators or functions Gluten fallback the operator to Vanilla Spark. There are C2R and R2C converter to convert the columnar data and Spark's internal row data. Both C2R and R2C are implemented natively as well
* Metrics are very important to get insight of Spark's execution, identify the issues or bottlenecks. Gluten collects the metrics from native library and shows in Spark UI.
* Shim layer is used to support mutiple releases of Spark. Gluten does not and will not support all the Spark releases. The plan is to support the latest 2-3 spark stable release only.

# Usage

Gluten is still under actively developping now. There isn't released binary yet. The only way to use Gluten is to build from source.
Gluten can support Spark3.2 and 3.3 now by shim layer.

## Build and Install Gluten with Velox backend

<img src="https://github.com/facebookincubator/velox/raw/main/static/logo.svg" width="200">

If you would like to build and try Gluten with **Velox** backend, please follow the steps in [Build with Velox](./docs/Velox.md) to build and install the necessary libraries, compile Velox and try out the TPC-H workload.

## Build and Install Gluten with ClickHouse backend

![logo](./docs/image/ClickHouse/logo.png)

If you would like to build and try  Gluten with **ClickHouse** backend, please follow the steps in [Build with ClickHouse Backend](./docs/ClickHouse.md). ClickHouse backend is devleoped by [Kyligence](https://kyligence.io/), please visit https://github.com/Kyligence/ClickHouse for more infomation.

## Build and Install Gluten with Arrow backend

If you would like to build and try Gluten with **Arrow** backend, please follow the steps in [Build with Arrow Backend](./docs/ArrowBackend.md). Arrow backend only support parquet scan and parquet write now. All other operators are fallback to Vanilla Spark.

## Build script parameters

[Gluten Usage](./docs/GlutenUsage.md) listed the parameters and their default value of build command for your reference

# Contribution

Gluten project welcomes everyone to contribute. 

## Community

Currently we communite with all developers and users in a wechat group(Chinese only), Spark channel in Velox Slack group. Contact us if you would like to join in.

## Bug Reports

Feel free to submit any bugs, issues or enhancement requirements to github issue list. Be sure to follow the bug fill template so we can solve it quickly. If you already implement a PR and would like to contribute, you may submit an issue firstly and refer to the issue in the PR. 

## Documentation

Unfortunately we haven't create the orginazed documentation site for Gluten. Currently all document is hold in [docs](https://github.com/oap-project/gluten/tree/main/docs). Ping us if you would like to know more details about the Gluten design. Gluten is still in developping now, some designs may change. Feel free to talk with us if you have better design.

# Performance

We use Decision Support Benchmark1(TPC-H Like) to evaluate the performance for Gluten project.
Decision Support Benchmark1 is a query set modified from [TPC-H benchmark](http://tpc.org/tpch/default5.asp). Because some features are not fully supported, there are some changes during the testing. Firstly we change column data type like Decimal to Double and Date to String. Secondly we use Parquet file format for Velox testing & MergeTree file format for Clickhouse testing compared to Parquet file format as baseline. Thirdly we modify the SQLs to use double and string data type for both Gluten and baseline, please check [Decision Support Benchmark1](./backends-velox/workload/tpch) has the script and queries as the examples to run the performance testing for Velox backend.

The testing environment is using single node with 2TB datasize and using Spark3.1.1 for both baseline and Gluten. The Decision Support Benchmark1 result shows an average speedup of 2.07x and up to 8.1X speedup in a single query with Gluten and Velox backend. Spark3.2 performance is pretty close.
![Performance](./docs/image/velox_decision_support_bench1_22queries_performance.png)

The testing environment is using a 8-nodes AWS cluster with 1TB datasize and using Spark3.1.1 for both baseline and Gluten. The Decision Support Benchmark1 result shows an average speedup of 2.12x and up to 3.48x speedup with Gluten and Clickhouse backend.
![Performance](./docs/image/clickhouse_decision_support_bench1_22queries_performance.png)


# Reference

Please check below links for more related information.
- [Gluten Intro Video at Data AI Summit 2022](https://databricks.com/dataaisummit/session/gazelle-jni-middle-layer-offload-spark-sql-native-engines-execution)
- [Gluten Intro Article at Medium.com](https://medium.com/intel-analytics-software/accelerate-spark-sql-queries-with-gluten-9000b65d1b4e)
- [Gluten Intro Article at Kyligence.io(in Chinese)](https://cn.kyligence.io/blog/gluten-spark/)
- [Velox Intro from Meta](https://engineering.fb.com/2022/08/31/open-source/velox/)

# License

Gluten is under Apache 2.0 license(https://www.apache.org/licenses/LICENSE-2.0).

# Contact

rui.mo@intel.com; binwei.yang@intel.com; weiting.chen@intel.com;
chang.chen@kyligence.io; zhichao.zhang@kyligence.io; neng.liu@kyligence.io


##### \* LEGAL NOTICE: Your use of this software and any required dependent software (the "Software Package") is subject to the terms and conditions of the software license agreements for the Software Package, which may also include notices, disclaimers, or license terms for third party or open source software included in or with the Software Package, and your use indicates your acceptance of all such terms. Please refer to the "TPP.txt" or other similarly-named text file included with the Software Package for additional details.


![Gluten](docs/image/gluten-logo.svg)

# Apache Gluten (Incubating): A Middle Layer for Offloading JVM-based SQL Engines' Execution to Native Engines

[![OpenSSF Best Practices](https://www.bestpractices.dev/projects/8452/badge)](https://www.bestpractices.dev/projects/8452)

*<b>This project is still under active development now, and doesn't have a stable release. Welcome to evaluate it.</b>*

# 1. Introduction
## Problem Statement
Apache Spark is a stable, mature project that has been developed for many years. It is one of the best frameworks to scale out for processing petabyte-scale datasets. However, the Spark community has had to address
performance challenges that require various optimizations over time. As a key optimization in Spark 2.0, Whole Stage Code Generation is introduced to replace Volcano Model, which achieves 2x speedup. Henceforth, most
optimizations are at query plan level. Single operator's performance almost stops growing.

<p align="center">
<img src="https://user-images.githubusercontent.com/47296334/199853029-b6d0ea19-f8e4-4f62-9562-2838f7f159a7.png" width="800">
</p>

On the other side, native SQL engines have been developed for a few years, such as Clickhouse, Arrow and Velox, etc. With features like native execution, columnar data format and vectorized
data processing, these native engines can outperform Spark's JVM based SQL engine. However, they only support single node execution.

## Gluten's Basic Design
“Gluten” is Latin for "glue". The main goal of Gluten project is to glue native engines with SparkSQL. Thus, we can benefit from high scalability of Spark SQL framework and high performance of native engines.

The basic design rule is that we would reuse Spark's whole control flow and as much JVM code as possible but offload the compute-intensive data processing to native side. Here is what Gluten does basically:
* Transform Spark’s physical plan to Substrait plan, then transform it to native engine's plan.
* Offload performance-critical data processing to native engine.
* Define clear JNI interfaces for native SQL engines.
* Switch available native backends easily.
* Reuse Spark’s distributed control flow.
* Manage data sharing between JVM and native.
* Extensible to support more native engines.

## Target User
Gluten's target user is anyone who aspires to accelerate SparkSQL fundamentally. As a plugin to Spark, Gluten doesn't require any change for dataframe API or SQL query, but only requires user to make correct configuration.
See Gluten configuration properties [here](https://github.com/apache/incubator-gluten/blob/main/docs/Configuration.md).

## References
You can click below links for more related information.
- [Gluten Intro Video at Data AI Summit 2022](https://www.youtube.com/watch?v=0Q6gHT_N-1U)
- [Gluten Intro Article at Medium.com](https://medium.com/intel-analytics-software/accelerate-spark-sql-queries-with-gluten-9000b65d1b4e)
- [Gluten Intro Article at Kyligence.io(in Chinese)](https://cn.kyligence.io/blog/gluten-spark/)
- [Velox Intro from Meta](https://engineering.fb.com/2023/03/09/open-source/velox-open-source-execution-engine/)

# 2. Architecture
The overview chart is like below. Substrait provides a well-defined cross-language specification for data compute operations (see more details [here](https://substrait.io/)). Spark physical plan is transformed to Substrait plan. Then Substrait plan is passed to native through JNI call.
On native side, the native operator chain will be built out and offloaded to native engine. Gluten will return Columnar Batch to Spark and Spark Columnar API (since Spark-3.0) will be used at execution time. Gluten uses Apache Arrow data format as its basic data format, so the returned data to Spark JVM is ArrowColumnarBatch.
<p align="center">
<img src="https://user-images.githubusercontent.com/47296334/199617207-1140698a-4d53-462d-9bc7-303d14be060b.png" width="800">
</p>
Currently, Gluten only supports Clickhouse backend & Velox backend. Velox is a C++ database acceleration library which provides reusable, extensible and high-performance data processing components. More details can be found from https://github.com/facebookincubator/velox/. Gluten can also be extended to support more backends.

There are several key components in Gluten:
* **Query Plan Conversion**: converts Spark's physical plan to Substrait plan.
* **Unified Memory Management**: controls native memory allocation.
* **Columnar Shuffle**: shuffles Gluten columnar data. The shuffle service still reuses the one in Spark core. A kind of columnar exchange operator is implemented to support Gluten columnar data format.
* **Fallback Mechanism**: supports falling back to Vanilla spark for unsupported operators. Gluten ColumnarToRow (C2R) and RowToColumnar (R2C) will convert Gluten columnar data and Spark's internal row data if needed. Both C2R and R2C are implemented in native code as well
* **Metrics**: collected from Gluten native engine to help identify bugs, performance bottlenecks, etc. The metrics are displayed in Spark UI.
* **Shim Layer**: supports multiple Spark versions. We plan to only support Spark's latest 2 or 3 releases. Currently, Spark-3.2, Spark-3.3 & Spark-3.4 (experimental) are supported.

# 3. User Guide
Here is a basic configuration to enable Gluten in Spark.

```
export GLUTEN_JAR=/PATH/TO/GLUTEN_JAR
spark-shell \
  --master yarn --deploy-mode client \
  --conf spark.plugins=org.apache.gluten.GlutenPlugin \
  --conf spark.memory.offHeap.enabled=true \
  --conf spark.memory.offHeap.size=20g \
  --conf spark.driver.extraClassPath=${GLUTEN_JAR} \
  --conf spark.executor.extraClassPath=${GLUTEN_JAR} \
  --conf spark.shuffle.manager=org.apache.spark.shuffle.sort.ColumnarShuffleManager
  ...
```

There are two ways to acquire Gluten jar for the above configuration.

### Use Released Jar
Please download a tar package [here](https://downloads.apache.org/incubator/gluten/), then extract out Gluten jar from it.
It was verified on Centos-7, Centos-8, Ubuntu-20.04 and Ubuntu-22.04.

### Build From Source
For **Velox** backend, please refer to [Velox.md](./docs/get-started/Velox.md) and [build-guide.md](./docs/get-started/build-guide.md).

For **ClickHouse** backend, please refer to [ClickHouse.md](./docs/get-started/ClickHouse.md). ClickHouse backend is developed by [Kyligence](https://kyligence.io/), please visit https://github.com/Kyligence/ClickHouse for more information.

Gluten jar will be generated under `/PATH/TO/GLUTEN/package/target/` after the build.

# 4. Gluten Website
https://gluten.apache.org/

# 5. Contribution
Welcome to contribute to Gluten project! See [CONTRIBUTING.md](CONTRIBUTING.md) about how to make contributions.

# 6. Community
Gluten successfully became Apache incubator project in March 2024. Here are several ways to contact us:

## GitHub
Welcome to report any issue or create any discussion related to Gluten in GitHub. Please do a search from GitHub issue list before creating a new one to avoid repetition.

## Mail Lists
For any technical discussion, please send email to [dev@gluten.apache.org](mailto:dev@gluten.apache.org). You can go to [archives](https://lists.apache.org/list.html?dev@gluten.apache.org)
for getting historical discussions. Please click [here](mailto:dev-subscribe@gluten.apache.org) to subscribe the mail list.

## Slack Channel (English communication)
Please click [here](https://github.com/apache/incubator-gluten/discussions/8429) to get invitation for ASF Slack workspace where you can find "incubator-gluten" channel.

The ASF Slack login entry: https://the-asf.slack.com/.

## WeChat Group (Chinese communication)
For PRC developers/users, please contact weitingchen at apache.org or zhangzc at apache.org for getting invited to the WeChat group. 

# 7. Performance
We use Decision Support Benchmark1 (TPC-H like) to evaluate Gluten's performance.
Decision Support Benchmark1 is a query set modified from [TPC-H benchmark](http://tpc.org/tpch/default5.asp). We use Parquet file format for Velox testing & MergeTree file format for Clickhouse testing, compared to Parquet file format as baseline. See [Decision Support Benchmark1](./tools/workload/tpch).

The below test environment: single node with 2TB data; Spark-3.3.2 for both baseline and Gluten. The Decision Support Benchmark1 result (tested in Jun. 2023) shows an overall speedup of 2.71x and up to 14.53x speedup in a single query with Gluten Velox backend used.

![Performance](./docs/image/velox_decision_support_bench1_22queries_performance.png)

The below testing environment: a 8-nodes AWS cluster with 1TB data; Spark-3.1.1 for both baseline and Gluten. The Decision Support Benchmark1 result shows an average speedup of 2.12x and up to 3.48x speedup with Gluten Clickhouse backend.

![Performance](./docs/image/clickhouse_decision_support_bench1_22queries_performance.png)

# 8. Qualification Tool

The Qualification Tool is a utility to analyze Spark event log files and assess the compatibility and performance of SQL workloads with Gluten. This tool helps users understand how their workloads can benefit from Gluten.

## Features
- Analyzes Spark SQL execution plans for compatibility with Gluten.
- Supports various types of event log files, including single files, folders, compressed files, and rolling event logs.
- Generates detailed reports highlighting supported and unsupported operations.
- Provides metrics on SQL execution times and operator impact.
- Offers configurable options such as threading, output directory, and date-based filtering.

## Usage

To use the Qualification Tool, follow the instructions in its [README](tools/qualification-tool/README.MD).

## Example Command
```bash
java -jar target/qualification-tool-1.3.0-SNAPSHOT-jar-with-dependencies.jar -f /path/to/eventlog
```
For detailed usage instructions and advanced options, see the Qualification Tool README.

# 9. License
Gluten is licensed under [Apache 2.0 license](https://www.apache.org/licenses/LICENSE-2.0).

# 10. Acknowledgements
Gluten was initiated by Intel and Kyligence in 2022. Several companies are also actively participating in the development, such as BIGO, Meituan, Alibaba Cloud, NetEase, Baidu, Microsoft, IBM, Google, etc.

<a href="https://github.com/apache/incubator-gluten/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=apache/incubator-gluten&columns=25" />
</a>

##### \* LEGAL NOTICE: Your use of this software and any required dependent software (the "Software Package") is subject to the terms and conditions of the software license agreements for the Software Package, which may also include notices, disclaimers, or license terms for third party or open source software included in or with the Software Package, and your use indicates your acceptance of all such terms. Please refer to the "TPP.txt" or other similarly-named text file included with the Software Package for additional details.

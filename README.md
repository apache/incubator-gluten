# Gluten: A Spark Plugin to Offload SQL Engine to Native Library
*<b>This plugin is still under active development now, and doesn't have a stable release. Welcome to evaluate it. If you encounter any issues or have any suggestions, please submit to our issue list. We'd love to hear your feedback</b>*

# 1 Introduction

## 1.1 Problem Statement

Apache Spark is a stable, mature project that has been under development for many years. The project has proven to be one of the best frameworks to scale out of processing petabyte-scale datasets. However, the Spark community has had to address performance challenges that required various optimizations over time. A key optimization introduced in Spark 2.0 replaced Volcano mode with whole-stage code-generation to achieve a 2x speedup. Since then most of the optimization works at the query plan level. The operator's performance stopped to grow.

<p align="center">
<img src="https://user-images.githubusercontent.com/47296334/199853029-b6d0ea19-f8e4-4f62-9562-2838f7f159a7.png" width="800">
</p>

On the other side, SQL engine is researched for years. There are product or libraries like Clickhouse, Arrow or Velox. By using features like native implementation, columnar data format as well as vectorized data processing, these libraries outperform much of Spark's JVM based SQL engine. However these libraries are running on single node.

## 1.2 Gluten's Solution

“Gluten” is Latin for glue. Main goal of project Gluten is to “glue" the SparkSQL and native libraries. So we can take use of and benefit from Spark SQL's scale out framework as well native libraries' high performance.

The basic rule of Gluten's design is that we would reuse spark's whole control flow and as many JVM code as possible but offload the compute intensive data processing part to native code. Here is what Gluten does:
* Transform Spark’s whole stage physical plan to Substrait plan and send to native
* Offload performance critical data processing to native library
* Define clear JNI interfaces for native libraries
* Switch the native backends easily
* Reuse Spark’s distributed control flow
* Manage data sharing between JVM and native
* Extend support to native accelerators

## 1.3 Target User

Gluten targets to the Spark administrators and Spark users who want to improve their spark cluster's performance fundamentally. Gluten is a plugin to Spark. It's designed to offload the SQL engine to native without any dataframe API or SQL query changes. SparkSQL users can run their current Spark job on Gluten seamlessly, no code changes are needed. However as a plugin, Gluten needs some configurations to enable it when you start Spark context. All configurations are listed [here](https://github.com/oap-project/gluten/blob/main/docs/Configuration.md)

## 1.4 References:

You may click below links for more related information.
- [Gluten Intro Video at Data AI Summit 2022](https://databricks.com/dataaisummit/session/gazelle-jni-middle-layer-offload-spark-sql-native-engines-execution)
- [Gluten Intro Article at Medium.com](https://medium.com/intel-analytics-software/accelerate-spark-sql-queries-with-gluten-9000b65d1b4e)
- [Gluten Intro Article at Kyligence.io(in Chinese)](https://cn.kyligence.io/blog/gluten-spark/)
- [Velox Intro from Meta](https://engineering.fb.com/2022/08/31/open-source/velox/)


# 2 Architecture

The overview chart is like below. Spark physical plan is transformed to substrait plan. Substrait is to create a well defined cross-language specification for data compute operations. More details can be found from https://substrait.io/. Then substrait plan is passed to native through JNI call. In native the operator chain should be built and start to run. We use Spark3.0's columnar API as the data interface, so the native library should return Columnar Batch to Spark. We may need to wrap the columnar batch for each native backend. Gazelle engine's c++ code use Apache Arrow data format as its basic data format, so the returned data to Spark JVM is ArrowColumnarBatch.
<p align="center">
<img src="https://user-images.githubusercontent.com/47296334/199617207-1140698a-4d53-462d-9bc7-303d14be060b.png" width="800">
</p>
There are several native libraries we may offload. Currently we are working on Clickhouse and Velox as native backend. Velox is a C++ database acceleration library which provides reusable, extensible, and high-performance data processing components. More details can be found from https://github.com/facebookincubator/velox/. We also implemented a basic backend using Arrow Computer Engine which is for reference only. Gluten can also be easily extended to any accelerator libraries as backend.

There are several key component in Gluten:
* Query plan conversion which convert Spark's physical plan into substrait plan in each stage.
* Unified memory management in Spark is used to control the native memory allocation as well
* Columnar shuffle is used to shuffle columnar data directly. The shuffle service still reuses the one in Spark core. The exchange operator is reimplemented to support columnar data format
* For unsupported operators or functions Gluten fallback the operator to Vanilla Spark. There are C2R and R2C converter to convert the columnar data and Spark's internal row data. Both C2R and R2C are implemented natively as well
* Metrics are very important to get insight of Spark's execution, identify the issues or bottlenecks. Gluten collects the metrics from native library and shows in Spark UI.
* Shim layer is used to support multiple releases of Spark. Gluten only plans to support the latest 2-3 spark stable releases, with no plans to add support on older spark releases. Current support is on Spark 3.2 and Spark 3.3.

# 3 Usage

Gluten is still under active development now. There isn't a released binary yet. The only way to use Gluten is to build from source, copy the jar to your spark jars, then enable Gluten plugin when you start your spark context. Here is the simple example. Refer to Velox or Clickhouse backend below for more details

```export gluten_jvm_jar = /PATH/TO/GLUTEN/backends-velox/target/gluten-spark3.2_2.12-1.0.0-snapshot-jar-with-dependencies.jar 
spark-shell 
  --master yarn --deploy-mode client \
  --conf spark.plugins=io.glutenproject.GlutenPlugin \
  --conf spark.gluten.sql.columnar.backend.lib=velox or ch \
  --conf spark.driver.extraClassPath=${gluten_jvm_jar} \
  --conf spark.executor.extraClassPath=${gluten_jvm_jar} \
  --conf spark.shuffle.manager=org.apache.spark.shuffle.sort.ColumnarShuffleManager \
  ...
  ```

## 3.1 Build and Install Gluten with Velox backend

<img src="https://github.com/facebookincubator/velox/raw/main/static/logo.svg" width="200">

If you would like to build and try Gluten with **Velox** backend, please follow the steps in [Build with Velox](./docs/Velox.md) to build and install the necessary libraries, compile Velox and try out the TPC-H workload.

## 3.2 Build and Install Gluten with ClickHouse backend

![logo](./docs/image/ClickHouse/logo.png)

If you would like to build and try  Gluten with **ClickHouse** backend, please follow the steps in [Build with ClickHouse Backend](./docs/ClickHouse.md). ClickHouse backend is developed by [Kyligence](https://kyligence.io/), please visit https://github.com/Kyligence/ClickHouse for more infomation.

## 3.3 Build and Install Gluten with Arrow backend

If you would like to build and try Gluten with **Arrow** backend, please follow the steps in [Build with Arrow Backend](./docs/ArrowBackend.md). Arrow backend only support parquet scan and parquet write now. All other operators are fallback to Vanilla Spark.

## 3.4 Build script parameters

[Gluten Usage](./docs/GlutenUsage.md) listed the parameters and their default value of build command for your reference

## 3.5 Jar conflicts

Several libraries Gluten used is newer than Spark's, including protobuf (Both Velox and CK backend), flatbuffers (Velox backend), and arrow-* (Velox backend). These libraries are compiled from source and packed into Gluten.jars. Jvm should search them from Gluten.jar firstly and load them. But for some reason jvm loads the jars from spark_home/jars which causes conflict. You may use below commands to remove the jars from spark_home/jars. We are still investigating the root cause. Welcome to share if you have good solution.

```
rm -rf $SPARK_HOME/jars/protobuf-*
# velox backend only
rm -rf $SPARK_HOME/jars/flatbuffers-*
rm -rf $SPARK_HOME/jars/arrow-*
```

# 4 Contribution

Gluten project welcomes everyone to contribute. 

## 4.1 Community

Currently we communicate with all developers and users in a wechat group(Chinese only), and a Spark channel in Velox Slack group. Contact us if you would like to join in. Refer to Contact info below

## 4.2 Bug Reports

Feel free to submit any bugs, issues or enhancement requirements to github issue list. Be sure to follow the bug fill template so we can solve it quickly. If you already implement a PR and would like to contribute, you may submit an issue firstly and refer to the issue in the PR. 

## 4.3 Documentation

Unfortunately we haven't organized the documentation site for Gluten. Currently all document is hold in [docs](https://github.com/oap-project/gluten/tree/main/docs). Ping us if you would like to know more details about the Gluten design. Gluten is still under development now, and some designs may change. Feel free to talk with us and share other design and ideas.

CppCodingStyle.md is provided for the purpose of helping C++ developers to contribute code, this work is still in progess, so propose a new modification PR without any hesitation if you have good ideas. [CppCodingStyle.md](https://github.com/oap-project/gluten/tree/main/docs/developers/CppCodingStyle.md)

# 5 Performance

We use Decision Support Benchmark1(TPC-H Like) to evaluate the performance for Gluten project.
Decision Support Benchmark1 is a query set modified from [TPC-H benchmark](http://tpc.org/tpch/default5.asp). Because some features are not fully supported, there are some changes during the testing. Firstly we change column data type like Decimal to Double and Date to String. Secondly we use Parquet file format for Velox testing & MergeTree file format for Clickhouse testing compared to Parquet file format as baseline. Thirdly we modify the SQLs to use double and string data type for both Gluten and baseline, please check [Decision Support Benchmark1](./backends-velox/workload/tpch) has the script and queries as the examples to run the performance testing for Velox backend.

The testing environment is using single node with 2TB datasize and using Spark3.1.1 for both baseline and Gluten. The Decision Support Benchmark1 result shows an average speedup of 2.07x and up to 8.1X speedup in a single query with Gluten and Velox backend. Spark3.2 performance is pretty close. Performance data is tested in Sep. 2022. Contact us if you'd like to know latest performance number
![Performance](./docs/image/velox_decision_support_bench1_22queries_performance.png)

The testing environment is using a 8-nodes AWS cluster with 1TB datasize and using Spark3.1.1 for both baseline and Gluten. The Decision Support Benchmark1 result shows an average speedup of 2.12x and up to 3.48x speedup with Gluten and Clickhouse backend.
![Performance](./docs/image/clickhouse_decision_support_bench1_22queries_performance.png)



# 6 License

Gluten is under Apache 2.0 license(https://www.apache.org/licenses/LICENSE-2.0).

# 7 Contact

rui.mo@intel.com; binwei.yang@intel.com; weiting.chen@intel.com;
chang.chen@kyligence.io; zhichao.zhang@kyligence.io; neng.liu@kyligence.io


##### \* LEGAL NOTICE: Your use of this software and any required dependent software (the "Software Package") is subject to the terms and conditions of the software license agreements for the Software Package, which may also include notices, disclaimers, or license terms for third party or open source software included in or with the Software Package, and your use indicates your acceptance of all such terms. Please refer to the "TPP.txt" or other similarly-named text file included with the Software Package for additional details.


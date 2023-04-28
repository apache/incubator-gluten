---
layout: default
title: Home
nav_order: 1
permalink: /
description: This site serves as a collection of documentation about the Gluten, a plugin to Double SparkSQL's Performance
---
# Overview
Gluten: Plugin to Double SparkSQL's Performance

# 1 Introduction

## 1.1 Problem Statement

Apache Spark is a stable, mature project that has been under development for many years. The project has been proven to be one of the best frameworks to scale out of processing petabyte-scale datasets. However, the Spark community has had to address performance challenges that required various optimizations over time. A key optimization introduced in Spark 2.0 replaced Volcano mode with whole-stage code-generation to achieve a 2x speedup. Since then most of the optimization works at the query plan level. The operator's performance stopped to grow.

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
- [Gluten Intro Video at Data AI Summit 2022](https://www.youtube.com/watch?v=0Q6gHT_N-1U)
- [Gluten Intro Article at Medium.com](https://medium.com/intel-analytics-software/accelerate-spark-sql-queries-with-gluten-9000b65d1b4e)
- [Gluten Intro Article at Kyligence.io(in Chinese)](https://cn.kyligence.io/blog/gluten-spark/)
- [Velox Intro from Meta](https://engineering.fb.com/2023/03/09/open-source/velox-open-source-execution-engine/)


# 2 Architecture

The overview chart is like below. Spark physical plan is transformed to substrait plan. Substrait is to create a well defined cross-language specification for data compute operations. More details can be found from https://substrait.io/. Then substrait plan is passed to native through JNI call. In native the operator chain should be built and start to run. We use Spark3.0's columnar API as the data interface, so the native library should return Columnar Batch to Spark. We may need to wrap the columnar batch for each native backend. Gluten's c++ code use Apache Arrow data format as its basic data format, so the returned data to Spark JVM is ArrowColumnarBatch.
<p align="center">
<img src="https://user-images.githubusercontent.com/47296334/199617207-1140698a-4d53-462d-9bc7-303d14be060b.png" width="800">
</p>
There are several native libraries we may offload. Currently we are working on Clickhouse and Velox as native backend. Velox is a C++ database acceleration library which provides reusable, extensible, and high-performance data processing components. More details can be found from https://github.com/facebookincubator/velox/. Gluten can also be easily extended to any accelerator libraries as backend.

There are several key component in Gluten:
* Query plan conversion which convert Spark's physical plan into substrait plan in each stage.
* Unified memory management in Spark is used to control the native memory allocation as well
* Columnar shuffle is used to shuffle columnar data directly. The shuffle service still reuses the one in Spark core. The exchange operator is reimplemented to support columnar data format
* For unsupported operators or functions Gluten fallback the operator to Vanilla Spark. There are C2R and R2C converter to convert the columnar data and Spark's internal row data. Both C2R and R2C are implemented natively as well
* Metrics are very important to get insight of Spark's execution, identify the issues or bottlenecks. Gluten collects the metrics from native library and shows in Spark UI.
* Shim layer is used to support multiple releases of Spark. Gluten only plans to support the latest 2-3 spark stable releases, with no plans to add support on older spark releases. Current support is on Spark 3.2 and Spark 3.3.

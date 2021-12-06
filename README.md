##### \* LEGAL NOTICE: Your use of this software and any required dependent software (the "Software Package") is subject to the terms and conditions of the software license agreements for the Software Package, which may also include notices, disclaimers, or license terms for third party or open source software included in or with the Software Package, and your use indicates your acceptance of all such terms. Please refer to the "TPP.txt" or other similarly-named text file included with the Software Package for additional details.

##### \* Optimized Analytics Package for Spark* Platform is under Apache 2.0 (https://www.apache.org/licenses/LICENSE-2.0).

# Gazelle JNI

A Spark JNI Layer to enable different native libraries based on Substrait

## Introduction

![Overview](./docs/image/nativesql_arch.png)

Spark SQL works very well with structured row-based data. It used WholeStageCodeGen to improve the performance by Java JIT code. However Java JIT is usually not working very well on utilizing latest SIMD instructions, especially under complicated queries. [Apache Arrow](https://arrow.apache.org/) provided CPU-cache friendly columnar in-memory layout, its SIMD-optimized kernels and LLVM-based SQL engine Gandiva are also very efficient.

Gazelle Plugin reimplements Spark SQL execution layer with SIMD-friendly columnar data processing based on Apache Arrow, 
and leverages Arrow's CPU-cache friendly columnar in-memory layout, SIMD-optimized kernels and LLVM-based expression engine to bring better performance to Spark SQL.

## How to use OAP: Gazelle JNI

There are three ways to use OAP: Gazelle Plugin,
1. Use precompiled jars
2. Building by Conda Environment
3. Building by Yourself

### Use precompiled jars

Please go to [OAP's Maven Central Repository](https://repo1.maven.org/maven2/com/intel/oap/) to find Gazelle Plugin jars.
For usage, you will require below two jar files:
1. spark-arrow-datasource-standard-<version>-jar-with-dependencies.jar is located in com/intel/oap/spark-arrow-datasource-standard/<version>/
2. spark-columnar-core-<version>-jar-with-dependencies.jar is located in com/intel/oap/spark-columnar-core/<version>/
Please notice the files are fat jars shipped with our custom Arrow library and pre-compiled from our server(using GCC 9.3.0 and LLVM 7.0.1), which means you will require to pre-install GCC 9.3.0 and LLVM 7.0.1 in your system for normal usage. 

### Building by Conda

If you already have a working Hadoop Spark Cluster, we provide a Conda package which will automatically install dependencies needed by OAP, you can refer to [OAP-Installation-Guide](./docs/OAP-Installation-Guide.md) for more information. Once finished [OAP-Installation-Guide](./docs/OAP-Installation-Guide.md), you can find built `spark-columnar-core-<version>-jar-with-dependencies.jar` under `$HOME/miniconda2/envs/oapenv/oap_jars`.
Then you can just skip below steps and jump to [Get Started](#get-started).

### Building by yourself

If you prefer to build from the source code on your hand, please follow below steps to set up your environment.

#### Prerequisite

There are some requirements before you build the project.
Please check the document [Prerequisite](./docs/Prerequisite.md) and make sure you have already installed the software in your system.
If you are running a SPARK Cluster, please make sure all the software are installed in every single node.

#### Installation

Please check the document [Installation Guide](./docs/Installation.md) 

## Get started

To enable Gazelle Plugin, the previous built jar `spark-columnar-core-<version>-jar-with-dependencies.jar` should be added to Spark configuration. We also recommend to use `spark-arrow-datasource-standard-<version>-jar-with-dependencies.jar`. We will demonstrate an example by using both jar files.
SPARK related options are:

* `spark.driver.extraClassPath` : Set to load jar file to driver.
* `spark.executor.extraClassPath` : Set to load jar file to executor.
* `jars` : Set to copy jar file to the executors when using yarn cluster mode.
* `spark.executorEnv.ARROW_LIBHDFS3_DIR` : Optional if you are using a custom libhdfs3.so.
* `spark.executorEnv.LD_LIBRARY_PATH` : Optional if you are using a custom libhdfs3.so.

For Spark Standalone Mode, please set the above value as relative path to the jar file.
For Spark Yarn Cluster Mode, please set the above value as absolute path to the jar file.

More Configuration, please check the document [Configuration Guide](./docs/Configuration.md)

Example to run Spark Shell with ArrowDataSource jar file
```
${SPARK_HOME}/bin/spark-shell \
        --verbose \
        --master yarn \
        --driver-memory 10G \
        --conf spark.driver.extraClassPath=$PATH_TO_JAR/spark-arrow-datasource-standard-<version>-jar-with-dependencies.jar:$PATH_TO_JAR/spark-columnar-core-<version>-jar-with-dependencies.jar \
        --conf spark.executor.extraClassPath=$PATH_TO_JAR/spark-arrow-datasource-standard-<version>-jar-with-dependencies.jar:$PATH_TO_JAR/spark-columnar-core-<version>-jar-with-dependencies.jar \
        --conf spark.driver.cores=1 \
        --conf spark.executor.instances=12 \
        --conf spark.executor.cores=6 \
        --conf spark.executor.memory=20G \
        --conf spark.memory.offHeap.size=80G \
        --conf spark.task.cpus=1 \
        --conf spark.locality.wait=0s \
        --conf spark.sql.shuffle.partitions=72 \
        --conf spark.executorEnv.ARROW_LIBHDFS3_DIR="$PATH_TO_LIBHDFS3_DIR/" \
        --conf spark.executorEnv.LD_LIBRARY_PATH="$PATH_TO_LIBHDFS3_DEPENDENCIES_DIR"
        --jars $PATH_TO_JAR/spark-arrow-datasource-standard-<version>-jar-with-dependencies.jar,$PATH_TO_JAR/spark-columnar-core-<version>-jar-with-dependencies.jar
```

Here is one example to verify if Gazelle Plugin works, make sure you have TPC-H dataset.  We could do a simple projection on one parquet table. For detailed testing scripts, please refer to [Solution Guide](https://github.com/Intel-bigdata/Solution_navigator/tree/master/nativesql).
```
val orders = spark.read.format("arrow").load("hdfs:////user/root/date_tpch_10/orders")
orders.createOrReplaceTempView("orders")
spark.sql("select * from orders where o_orderdate > date '1998-07-26'").show(20000, false)
```

The result should showup on Spark console and you can check the DAG diagram with some Columnar Processing stage. Gazelle Plugin still lacks some features, please check out the [limitations](./docs/limitations.md).

## Could/K8s Integration

### Amazone EMR

Please refer to [Gazelle_on_Dataproc](https://github.com/oap-project/oap-tools/tree/master/integrations/oap/emr) to find details about how to use OAP Gazelle on Amazon EMR Cloud.

###  Google Cloud Dataproc

Gazelle Plugin now supports to run on Dataproc 2.0, we provide a guide to help quickly install Gazelle Plugin and run TPC-DS with notebooks or scripts.

Please refer to [Gazelle_on_Dataproc](https://github.com/oap-project/oap-tools/blob/master/docs/Gazelle_on_Dataproc.md) to find details about:

1. Create a cluster on Dataproc 2.0 with initialization actions. 
Gazelle Plugin jars compiled with `-Pdataproc-2.0` parameter will installed by Conda in all cluster nodes.
   
2. Config for enabling Gazelle Plugin.

3. Run TPC-DS with notebooks or scripts.

You can also find more information about how to use [OAP on Google Dataproc](https://github.com/oap-project/oap-tools/tree/master/integrations/oap/dataproc).

## Coding Style

* For Java code, we used [google-java-format](https://github.com/google/google-java-format)
* For Scala code, we used [Spark Scala Format](https://github.com/apache/spark/blob/master/dev/.scalafmt.conf), please use [scalafmt](https://github.com/scalameta/scalafmt) or run ./scalafmt for scala codes format
* For Cpp codes, we used Clang-Format, check on this link [google-vim-codefmt](https://github.com/google/vim-codefmt) for details.

## Contact

binwei.yang@intel.com

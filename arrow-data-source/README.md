# Arrow Data Source 

A Spark DataSource implementation for reading files into Arrow compatible columnar vectors.

## Note

The development of this library is still in progress. As a result some of the functionality may not be constantly stable for being used in production environments that have not been fully considered due to the limited testing capabilities so far.

## Build

### Prerequisite

There are some requirements before you build the project.
Please make sure you have already installed the software in your system.

1. GCC 7.0 or higher version
2. java8 OpenJDK -> yum install java-1.8.0-openjdk
3. cmake 3.16 or higher version
4. maven 3.6 or higher version
5. Hadoop 2.7.5 or higher version
6. Spark 3.1.1 or higher version
7. Intel Optimized Arrow 4.0.0

### Building by Conda

If you already have a working Hadoop Spark Cluster, we provide a Conda package which will automatically install dependencies needed by OAP, you can refer to [OAP-Installation-Guide](../docs/OAP-Installation-Guide.md) for more information. Once finished [OAP-Installation-Guide](../docs/OAP-Installation-Guide.md), you can find built `spark-arrow-datasource-standard-<version>-jar-with-dependencies.jar` under `$HOME/miniconda2/envs/oapenv/oap_jars`.
Then you can just skip steps below and jump to [Get Started](#get-started).

### cmake installation

If you are facing some trouble when installing cmake, please follow below steps to install cmake.

```
// installing cmake 3.16.1
sudo yum install cmake3

// If you have an existing cmake, you can use below command to set it as an option within alternatives command
sudo alternatives --install /usr/local/bin/cmake cmake /usr/bin/cmake 10 --slave /usr/local/bin/ctest ctest /usr/bin/ctest --slave /usr/local/bin/cpack cpack /usr/bin/cpack --slave /usr/local/bin/ccmake ccmake /usr/bin/ccmake --family cmake

// Set cmake3 as an option within alternatives command
sudo alternatives --install /usr/local/bin/cmake cmake /usr/bin/cmake3 20 --slave /usr/local/bin/ctest ctest /usr/bin/ctest3 --slave /usr/local/bin/cpack cpack /usr/bin/cpack3 --slave /usr/local/bin/ccmake ccmake /usr/bin/ccmake3 --family cmake

// Use alternatives to choose cmake version
sudo alternatives --config cmake
```

### maven installation

If you are facing some trouble when installing maven, please follow below steps to install maven

```
// installing maven 3.6.3
Go to https://maven.apache.org/download.cgi and download the specific version of maven

// Below command use maven 3.6.3 as an example
wget https://ftp.wayne.edu/apache/maven/maven-3/3.6.3/binaries/apache-maven-3.6.3-bin.tar.gz
tar xzf apache-maven-3.6.3-bin.tar.gz
mkdir /usr/local/maven
mv apache-maven-3.6.3/ /usr/local/maven/

// Set maven 3.6.3 as an option within alternatives command
sudo alternatives --install /usr/bin/mvn mvn /usr/local/maven/apache-maven-3.6.3/bin/mvn 1

// Use alternatives to choose mvn version
sudo alternatives --config mvn
```

### Hadoop Native Library(Default)

Please make sure you have set up Hadoop directory properly with Hadoop Native Libraries
By default, Apache Arrow would scan `$HADOOP_HOME` and find the native Hadoop library `libhdfs.so`(under `$HADOOP_HOME/lib/native` directory) to be used for Hadoop client.

You can also use `ARROW_LIBHDFS_DIR` to configure the location of `libhdfs.so` if it is installed in other directory than `$HADOOP_HOME/lib/native`

If your SPARK and HADOOP are separated in different nodes, please find `libhdfs.so` in your Hadoop cluster and copy it to SPARK cluster, then use one of the above methods to set it properly.

For more information, please check
Arrow HDFS interface [documentation](https://github.com/apache/arrow/blob/master/cpp/apidoc/HDFS.md)
Hadoop Native Library, please read the official Hadoop website [documentation](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/NativeLibraries.html)

### Use libhdfs3 library for better performance(Optional)

For better performance ArrowDataSource reads HDFS files using the third-party library `libhdfs3`. The library must be pre-installed on machines Spark Executor nodes are running on.

To install the library, use of [Conda](https://docs.conda.io/en/latest/) is recommended.

```
// installing libhdfs3
conda install -c conda-forge libhdfs3

// check the installed library file
ll ~/miniconda/envs/$(YOUR_ENV_NAME)/lib/libhdfs3.so
```

To set up libhdfs3, there are two different ways:
Option1: Overwrite the soft link for libhdfs.so
To install libhdfs3.so, you have to create a soft link for libhdfs.so in your Hadoop directory(`$HADOOP_HOME/lib/native` by default).

```
ln -f -s libhdfs3.so libhdfs.so
```

Option2:
Add env variable to the system
```
export ARROW_LIBHDFS3_DIR="PATH_TO_LIBHDFS3_DIR/"
```

Add following Spark configuration options before running the DataSource to make the library to be recognized:
* `spark.executorEnv.ARROW_LIBHDFS3_DIR = "PATH_TO_LIBHDFS3_DIR/"`
* `spark.executorEnv.LD_LIBRARY_PATH = "PATH_TO_LIBHDFS3_DEPENDENCIES_DIR/"`

Please notes: If you choose to use libhdfs3.so, there are some other dependency libraries you have to installed such as libprotobuf or libcrypto.

### Build and install Intel® Optimized Arrow with Datasets Java API
You have to use a customized Arrow to support for our datasets Java API.

```
// build arrow-cpp
git clone -b arrow-4.0.0-oap https://github.com/oap-project/arrow.git
cd arrow/cpp
mkdir build
cd build
cmake -DARROW_DEPENDENCY_SOURCE=BUNDLED -DARROW_GANDIVA_JAVA=ON -DARROW_GANDIVA=ON -DARROW_PARQUET=ON -DARROW_CSV=ON -DARROW_HDFS=ON -DARROW_BOOST_USE_SHARED=ON -DARROW_JNI=ON -DARROW_DATASET=ON -DARROW_WITH_PROTOBUF=ON -DARROW_WITH_SNAPPY=ON -DARROW_WITH_LZ4=ON -DARROW_FILESYSTEM=ON -DARROW_JSON=ON ..
make

// build and install arrow jvm library
cd ../../java
mvn clean install -P arrow-jni -am -Darrow.cpp.build.dir=$PATH_TO_ARROW_SOURCE_CODE/arrow/cpp/build/release
```

### Build Arrow Data Source Library

```
// Download Arrow Data Source Code
git clone -b <version> https://github.com/oap-project/arrow-data-source.git

// Go to the directory
cd arrow-data-source

// build
mvn clean -DskipTests package

// check built jar library
readlink -f standard/target/spark-arrow-datasource-standard-<version>-jar-with-dependencies.jar
```

### Download Spark 3.1.1

Currently ArrowDataSource works on the Spark 3.1.1 version.

```
wget http://archive.apache.org/dist/spark/spark-3.1.1/spark-3.1.1-bin-hadoop2.7.tgz
tar -xf ./spark-3.1.1-bin-hadoop2.7.tgz
export SPARK_HOME=`pwd`/spark-3.1.1-bin-hadoop2.7
```

If you are new to Apache Spark, please go though [Spark's official deploying guide](https://spark.apache.org/docs/latest/cluster-overview.html) before getting started with ArrowDataSource.

## Get started
### Add extra class pathes to Spark

To enable ArrowDataSource, the previous built jar `spark-arrow-datasource-standard-<version>-jar-with-dependencies.jar` should be added to Spark configuration. Typically the options are:

* `spark.driver.extraClassPath` : Set to load jar file to driver. 
* `spark.executor.extraClassPath` : Set to load jar file to executor.
* `jars` : Set to copy jar file to the executors when using yarn cluster mode.
* `spark.executorEnv.ARROW_LIBHDFS3_DIR` : Optional if you are using a custom libhdfs3.so.
* `spark.executorEnv.LD_LIBRARY_PATH` : Optional if you are using a custom libhdfs3.so.

For Spark Standalone Mode, please set the above value as relative path to the jar file.
For Spark Yarn Cluster Mode, please set the above value as absolute path to the jar file.

Example to run Spark Shell with ArrowDataSource jar file
```
${SPARK_HOME}/bin/spark-shell \
        --verbose \
        --master yarn \
        --driver-memory 10G \
        --conf spark.driver.extraClassPath=$PATH_TO_DATASOURCE_DIR/spark-arrow-datasource-standard-<version>-jar-with-dependencies.jar \
        --conf spark.executor.extraClassPath=$PATH_TO_DATASOURCE_DIR/spark-arrow-datasource-standard-<version>-jar-with-dependencies.jar \
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
```

For more information about these options, please read the official Spark [documentation](https://spark.apache.org/docs/latest/configuration.html#runtime-environment).

### Run a query with ArrowDataSource (Scala)

```scala
val path = "${PATH_TO_YOUR_PARQUET_FILE}"
val df = spark.read
        .option(ArrowOptions.KEY_ORIGINAL_FORMAT, "parquet")
        .option(ArrowOptions.KEY_FILESYSTEM, "hdfs")
        .format("arrow")
        .load(path)
df.createOrReplaceTempView("my_temp_view")
spark.sql("SELECT * FROM my_temp_view LIMIT 10").show(10)
```
### To validate if ArrowDataSource works properly

To validate if ArrowDataSource works, you can go to the DAG to check if ArrowScan has been used from the above example query.

![Image of ArrowDataSource Validation](../docs/image/arrowdatasource_validation.png)


## Work together with ParquetDataSource (experimental)

We provide a customized replacement of Spark's built-in ParquetFileFormat. By so users don't have
to change existing Parquet-based SQL/code and will be able to read Arrow data from Parquet directly.
More importantly, sometimes the feature could be extremely helpful to make ArrowDataSource work correctly
with some 3rd-party storage tools (e.g. [Delta Lake](https://github.com/delta-io/delta)) that are built on top of ParquetDataSource.

To replace built-in ParquetDataSource, the only thing has to be done is to place compiled jar `spark-arrow-datasource-parquet-<version>.jar` into
Spark's library folder.

If you'd like to verify that ParquetDataSource is successfully overwritten by the jar, run following code 
before executing SQL job:
```
ServiceLoaderUtil.ensureParquetFileFormatOverwritten();
```

Note the whole feature is currently **experimental** and only DataSource v1 is supported. V2 support is being planned.

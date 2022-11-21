Currently, the mvn script can automatically fetch and build all dependency libraries incluing Velox and Arrow. Our nightly build still use Velox under oap-project. 

# 1 Prerequisite

<b>Currently Gluten+Velox backend is only tested on Ubuntu20.04 and Ubuntu22.04. Other OS support are still in progress </b>. The final goal is to support several common OS and conda env deployment. 
Velox uses the script setup-ubuntu.sh to install all dependency libraries, but Arrow's dependency libraries isn't installed. Velox also requires ninja for compilation. So we need to install all of them manually:

```shell script
apt install maven build-essential cmake libssl-dev libre2-dev libcurl4-openssl-dev clang lldb lld libz-dev git ninja-build uuid-dev
```

Also, we need to set up the JAVA_HOME env. Currently, java 8 is required and the support for java 11/17 is not ready.
```shell script
export JAVA_HOME=path/to/java/home
export PATH=$JAVA_HOME/bin:$PATH
```

# 2 Build Velox Jar

Since the mvn script calls setup-ubuntu.sh to install dependency libraries, so we need to run it from <b>root</b> group.

The command below clones velox source code from [OAP-project/velox](https://github.com/oap-project/velox) to ep/build-velox/build/velox_ep. Then it applies some patches to Velox build script and builds the velox library.

```shell script
# For spark3.2.x
mvn clean package -Pbackends-velox -Pspark-3.2 -Dbuild_velox_from_source=ON -Dbuild_arrow=ON
# For spark3.3.x
mvn clean package -Pbackends-velox -Pspark-3.3 -Dbuild_velox_from_source=ON -Dbuild_arrow=ON
```
You may compile spark3.2 and spark3.3 at the same time by -Pspark-3.2 -Pspark-3.3

The command generates the Jar file in the directory: package/velox/spark32/target/gluten-spark3.2_2.12-1.0.0-SNAPSHOT-jar-with-dependencies.jar. It's the only jar we need to config to Spark. So as spark3.3.

Refer to [build configurations](GlutenUsage.md) for the list of configurations used by mvn command.

## 2.1 Specify velox home directory

You can also clone the Velox source from [OAP/velox](https://github.com/oap-project/velox) to some other folder then specify it by -Dvelox_home as below.

```shell script
cd $VELOX_HOME
git clone https://github.com/oap-project/velox
cd $GLUTEN_ROOT
#If compiling velox for the first time.
mvn clean package -Pspark-3.2 -DskipTests -Dcheckstyle.skip -Pbackends-velox \
                  -Dbuild_cpp=ON \
                  -Dbuild_velox_backend=ON \
                  -Dvelox_home=${VELOX_HOME} \
                  -Dbuild_arrow=ON \
                  -Dcompile_velox=ON \
                  -Dbuild_type=release

#Just recompile velox without compiling third-party libraries
mvn clean package -Pspark-3.2 -DskipTests -Dcheckstyle.skip -Pbackends-velox \
                  -Dbuild_cpp=ON \
                  -Dbuild_velox_backend=ON \
                  -Dvelox_home=${VELOX_HOME} \
                  -Dbuild_arrow=OFF \
                  -Dbuild_protobuf=OFF \
                  -Dbuild_folly=OFF \
                  -Dcompile_velox=ON \
                  -Dbuild_type=release
```

## 2.2 Arrow home directory

Arrow home can be set as the same of Velox. Without -Darrow_home, arrow is cloned to toos/build/arrow_ep. You can specify the arrow home directory by -Darrow_home and then use -Dbuild_arrow to control arrow build or not. Currently Arrow is also clone from [OAP/Arrow](https://github.com/oap-project/arrow). We will soon switch to upstream Arrow. Currently the shuffle still uses Arrow's IPC interface.

## 2.3 HDFS support

Hadoop hdfs support is ready via the [libhdfs3](https://github.com/apache/hawq/tree/master/depends/libhdfs3) library. The libhdfs3 provides native API for Hadoop I/O without the drawbacks of JNI. It also provides advanced authentatication like Kerberos based. Please note this library has serveral depedencies which may require extra installations on Driver and Worker node.
On Ubuntu 20.04 the required depedencis are libiberty-dev, libxml2-dev, libkrb5-dev, libgsasl7-dev, libuuid1, uuid-dev. The packages can be installed via below command:
```
sudo apt install -y libiberty-dev libxml2-dev libkrb5-dev libgsasl7-dev libuuid1 uuid-dev
```
To build Gluten with HDFS support, below command is provided:
```
mvn clean package -Pbackends-velox -Pspark-3.2 -Pfull-scala-compiler -DskipTests -Dcheckstyle.skip -Dbuild_cpp=ON -Dbuild_velox_backend=ON -Dbuild_velox_from_source=ON -Dbuild_arrow=ON -Dvelox_enable_hdfs=ON
```
Gluten HDFS support requires an extra environment variable "VELOX_HDFS" to indicate the Hdfs URI. e.g. VELOX_HDFS="host:port". If the env variable is missing, Gluten will try to connect with hdfs://localhost:9000

This env should be exported in both Spark driver and worker.
e.g., in Spark local mode:
```
export VELOX_HDFS="hdfshost:9000"
```

If running in Spark Yarn cluster mode, the env variable need to be set on each executor:
```
--conf spark.executorEnv.VELOX_HDFS="hdfshost:9000"
```
One typical deployment on Spark/HDFS cluster is to enable [short-circuit reading](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/ShortCircuitLocalReads.html). Short-circuit reads provide a substantial performance boost to many applications.

By default libhdfs3 does not set the default hdfs domain socket path to support HDFS short-circuit read. If this feature is required in HDFS setup, users may need to setup the domain socket path correctly by patching the libhdfs3 source code or by setting the correct config environment. In Gluten the short-circuit domain socket path is set to "/var/lib/hadoop-hdfs/dn_socket" in [build_velox.sh](https://github.com/oap-project/gluten/blob/main/ep/build-velox/src/build_velox.sh) So we need to make sure the folder existed and user has write access as below script.

```
sudo mkdir -p /var/lib/hadoop-hdfs/
sudo chown <sparkuser>:<sparkuser> /var/lib/hadoop-hdfs/
```

You also need to add configuration to the "hdfs-site.xml" as below:

```
<property>
   <name>dfs.client.read.shortcircuit</name>
   <value>true</value>
</property>

<property>
   <name>dfs.domain.socket.path</name>
   <value>/var/lib/hadoop-hdfs/dn_socket</value>
</property>
```

## 2.4 Yarn Cluster mode

Hadoop Yarn mode is supported. Note libhdfs3 is used to read from HDFS, all its depedencies should be installed on each worker node. Users may requried to setup extra LD_LIBRARY_PATH if the depedencies are not on system's default library path. On Ubuntu 20.04 the dependencies can be installed with below command:
```
sudo apt install -y libiberty-dev libxml2-dev libkrb5-dev libgsasl7-dev libuuid1 uuid-dev
```
## 2.5 AWS S3 support

Velox supports S3 with the open source [AWS C++ SDK](https://github.com/aws/aws-sdk-cpp) and Gluten uses Velox S3 connector to connect with S3.
A new build option for S3(velox_enable_s3) is added. Below command is used to enable this feature
```
mvn clean package -Pbackends-velox -Pspark-3.2 -Pfull-scala-compiler -DskipTests -Dcheckstyle.skip -Dbuild_cpp=ON -Dbuild_velox_backend=ON -Dbuild_velox_from_source=ON -Dbuild_arrow=ON -Dvelox_enable_s3=ON
```
Currently to use S3 connector below configurations are required in spark-defaults.conf
```
spark.hadoop.fs.s3a.impl           org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.s3a.aws.credentials.provider org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider
spark.hadoop.fs.s3a.access.key     xxxx
spark.hadoop.fs.s3a.secret.key     xxxx
spark.hadoop.fs.s3a.endpoint https://s3.us-west-1.amazonaws.com
spark.hadoop.fs.s3a.connection.ssl.enabled true
spark.hadoop.fs.s3a.path.style.access false
```
Note if testing with local S3-like service(Minio/Ceph), users may need to use different configurations for these configurations. E.g., on Minio setup, the "spark.hadoop.fs.s3a.path.style.access" need to set to "true".

# 3 Coverage

Spark3.3 has 387 functions in total. ~240 are commonly used. Velox's functions have two category, Presto and Spark. Presto has 124 functions implemented. Spark has 62 functions. Spark functions are verified to have the same result as Vanilla Spark. Some Presto functions have the same result as Vanilla Spark but some others have different. Gluten prefer to use Spark functions firstly. If it's not in Spark's list but implemented in Presto, we currently offload to Presto one until we noted some result mismatch, then we need to reimplement the function in Spark category. Gluten currently offloads 53 functions.

Here is the operator support list:

![image](https://user-images.githubusercontent.com/47296334/199826049-2ed7235a-f461-499d-917c-a83452b326c7.png)

Union operator is implemented in JVM, needn't to offload to native.

# 4 High-Bandwidth Memory (HBM) support

Gluten supports allocating memory on HBM. This feature is optional and is disabled by default. It requires [Memkind library](http://memkind.github.io/memkind/) to be installed. Please follow memkind's [readme](https://github.com/memkind/memkind#memkind) to build and install all the dependencies and the library. 

To enable this feature in Gluten, users only need to add `-Denable_hbm=ON` to the maven build command. Here's an example:
```
mvn clean package -Pbackends-velox -Pspark-3.2 -Pfull-scala-compiler -DskipTests -Dcheckstyle.skip -Dbuild_cpp=ON -Dbuild_velox_backend=ON -Dbuild_velox_from_source=ON -Dbuild_arrow=ON -Denable_hbm=ON
```

Note that memory allocation fallback is also supported and cannot be turned off. If HBM is unavailable or fills up, the allocator will use default(DDR) memory.

During testing, it is possible that HBM is detected but not being used at runtime. The workaround is to set `MEMKIND_HBW_NODES` enviroment variable in the runtime environment. For the explaination to this variable, please refer to memkind's manual page. This can be set for all executors through spark conf, e.g. `--conf spark.executorEnv.MEMKIND_HBW_NODES=8-15`


# 5 Test TPC-H on Gluten with Velox backend

In Gluten, all 22 queries can be fully offloaded into Velox for computing.  

## 5.1 Data preparation

Considering current Velox does not fully support Decimal and Date data type, the [datagen script](../backends-velox/workload/tpch/gen_data/parquet_dataset/tpch_datagen_parquet.scala) transforms "Decimal-to-Double" and "Date-to-String". As a result, we need to modify the TPCH queries a bit. You can find the [modified TPC-H queries](../backends-velox/workload/tpch/tpch.queries.updated/).

## 5.2 Submit the Spark SQL job

Submit test script from spark-shell. You can find the scala code to [Run TPC-H](../backends-velox/workload/tpch/run_tpch/tpch_parquet.scala) as an example. Please remember to modify the location of TPC-H files as well as TPC-H queries in backends-velox/workload/tpch/run_tpch/tpch_parquet.scala before you run the testing. 

```
var parquet_file_path = "/PATH/TO/TPCH_PARQUET_PATH"
var gluten_root = "/PATH/TO/GLUTEN"
```

Below script shows an example about how to run the testing, you should modify the parameters such as executor cores, memory, offHeap size based on your environment. 

```shell script
export GLUTEN_JAR = /PATH/TO/GLUTEN/backends-velox/target/gluten-spark3.2_2.12-1.0.0-snapshot-jar-with-dependencies.jar 
cat tpch_parquet.scala | spark-shell --name tpch_powertest_velox \
  --master yarn --deploy-mode client \
  --conf spark.plugins=io.glutenproject.GlutenPlugin \
  --conf spark.gluten.sql.columnar.backend.lib=velox \
  --conf spark.driver.extraClassPath=${GLUTEN_JAR} \
  --conf spark.executor.extraClassPath=${GLUTEN_JAR} \
  --conf spark.memory.offHeap.enabled=true \
  --conf spark.memory.offHeap.size=20g \
  --conf spark.gluten.sql.columnar.forceshuffledhashjoin=true \
  --conf spark.shuffle.manager=org.apache.spark.shuffle.sort.ColumnarShuffleManager \
  --num-executors 6 \
  --executor-cores 6 \
  --driver-memory 20g \
  --executor-memory 25g \
  --conf spark.executor.memoryOverhead=5g \
  --conf spark.driver.maxResultSize=32g
```

Refer to [Gluten parameters ](./Configuration.md) for more details of each parameter used by Gluten.

## 5.3 Result
*wholestagetransformer* indicates that the offload works.

![TPC-H Q6](./image/TPC-H_Q6_DAG.png)

## 5.4 Performance

Below table shows the TPC-H Q1 and Q6 Performance in a multiple-thread test (--num-executors 6 --executor-cores 6) for Velox and vanilla Spark.
Both Parquet and ORC datasets are sf1024.

| Query Performance (s) | Velox (ORC) | Vanilla Spark (Parquet) | Vanilla Spark (ORC) |
|---------------- | ----------- | ------------- | ------------- |
| TPC-H Q6 | 13.6 | 21.6  | 34.9 |
| TPC-H Q1 | 26.1 | 76.7 | 84.9 |

# 6 External reference setup

TO ease your first-hand experience of using Gluten, we have set up an external reference cluster. If you are interested, please contact Weiting.Chen@intel.com.

# Velox Backend

Currently, the mvn script can automatically fetch and build all dependency libraries incluing Velox and Arrow. Our nightly build still use Velox under oap-project. 

## Prerequisite

Velox uses the script setup-ubuntu.sh to install all dependency libraries, but Arrow's dependency libraries can't be installed.
Velox also requires ninja for compilation. So we need to install them manually:

```shell script
apt install maven build-essential cmake libssl-dev libre2-dev libcurl4-openssl-dev clang lldb lld libz-dev git ninja-build uuid-dev
```

Also, we need to set up the JAVA_HOME env. Currently, java 8 is required and the support for java 11/17 is not ready.
```shell script
export JAVA_HOME=path/to/java/home
export PATH=$JAVA_HOME/bin:$PATH
```

## Build Velox Jar

Since the mvn script calls setup-ubuntu.sh to install dependency libraries, so we need to run it from root group.

The command below clones velox source code from [OAP-project/velox](https://github.com/oap-project/velox) to tools/build/velox_ep. Then it applies some patches to Velox build script and builds the velox library.

```shell script
# For spark3.2.x
mvn clean package -Pbackends-velox -Pspark-3.2 -Pfull-scala-compiler -DskipTests -Dcheckstyle.skip -Dbuild_cpp=ON -Dbuild_velox=ON -Dbuild_velox_from_source=ON -Dbuild_arrow=ON
# For spark3.3.x
mvn clean package -Pbackends-velox -Pspark-3.3 -Pfull-scala-compiler -DskipTests -Dcheckstyle.skip -Dbuild_cpp=ON -Dbuild_velox=ON -Dbuild_velox_from_source=ON -Dbuild_arrow=ON
```
You may compile spark3.2 and spark3.3 at the same time by -Pspark3.2 -Pspark3.3

The command generates the Jar file in the directory: package/velox/spark32/target/gluten-spark3.2_2.12-1.0.0-SNAPSHOT-jar-with-dependencies.jar. It's the only jar we need to config to Spark. So as spark3.3.

## Velox home directory

You can also clone the Velox source to some other folder then specify it by -Dvelox_home as below. With -Dbuild_velox=ON, the script applies the patches and build the Velox library. With -Dbuild_velox=OFF, script skips the velox build steps and reuse the existed library. It's useful if Velox isn't changed.

```shell script
mvn clean package -Pspark-3.2 -DskipTests -Dcheckstyle.skip -Pbackends-velox -Dbuild_protobuf=OFF -Dbuild_cpp=ON -Dbuild_velox=ON -Dvelox_home=${VELOX_HOME} -Dbuild_arrow=ON -Dcompile_velox=ON
```

## Arrow home directory

Arrow home can be set as the same of Velox. Without -Darrow_home, arrow is cloned to toos/build/arrow_ep. You can specify the arrow home directory by -Darrow_home and then use -Dbuild_arrow to control arrow build or not.

Refer to [build configurations](GlutenUsage.md) for the list of configurations used by mvn command.

## HDFS support

Hadoop hdfs support is ready via the [libhdfs3](https://github.com/apache/hawq/tree/master/depends/libhdfs3) library. The libhdfs3 provides native API for Hadoop I/O without the drawbacks of JNI. It also provides advanced authentatication like Kerberos based. Please note this library has serveral depedencies which may require extra installations on Driver and Worker node.
On Ubuntu 20.04 the required depedencis are libiberty-dev, libxml2-dev, libkrb5-dev, libgsasl7-dev, libuuid1, uuid-dev. The packages can be installed via below command:
```
sudo apt install -y libiberty-dev libxml2-dev libkrb5-dev libgsasl7-dev libuuid1 uuid-dev
```

Gluten HDFS support requires an extra environment variable "VELOX_HDFS" to indicate the Hdfs URI. e.g. VELOX_HDFS="host:port". If the env variable is missing, Gluten will try to connect with hdfs://localhost:9000

This env should be exported in both Spark driver and worker.
e.g., in Spark local mode:
```
export VELOX_HDFS="sr595:9000"
```

If running in Spark Yarn cluster mode, the env variable need to be set on each executor:
```
--conf spark.executorEnv.VELOX_HDFS="sr595:9000"
```
Note by default libhdfs3 does not set the default hdfs domain socket path for HDFS short-circuit read. If this feature is required in HDFS setup, users may need to setup the domain socket path correctly by patching the libhdfs3 source code or by setting the correct config environment. In Gluten the short-circuit domain socket path is set to "/var/lib/hadoop-hdfs/dn_socket" in [build_velox.sh](https://github.com/oap-project/gluten/blob/main/tools/build_velox.sh)

## Yarn Cluster mode

Hadoop Yarn mode is supported. Note libhdfs3 is used to read from HDFS, all its depedencies should be installed on each worker node. Users may requried to setup extra LD_LIBRARY_PATH if the depedencies are not on system's default library path. On Ubuntu 20.04 the dependencies can be installed with below command:
```
sudo apt install -y libiberty-dev libxml2-dev libkrb5-dev libgsasl7-dev libuuid1 uuid-dev
```

## Test TPC-H on Gluten with Velox backend

In Gluten, all 22 queries can be fully offloaded into Velox for computing.  

### Data preparation

Considering current Velox does not fully support Decimal and Date data type, the [datagen script](../backends-velox/workload/tpch/gen_data/parquet_dataset/tpch_datagen_parquet.scala) transforms "Decimal-to-Double" and "Date-to-String". As a result, we need to modify the TPCH queries a bit. You can find the [modified TPC-H queries](../backends-velox/workload/tpch/tpch.queries.updated/).

### Submit the Spark SQL job

Submit test script from spark-shell. You can find the scala code to [Run TPC-H](../backends-velox/workload/tpch/run_tpch/tpch_parquet.scala) as an example. Please remember to modify the location of TPC-H files as well as TPC-H queries in backends-velox/workload/tpch/run_tpch/tpch_parquet.scala before you run the testing. 

```
var parquet_file_path = "/PATH/TO/TPCH_PARQUET_PATH"
var gluten_root = "/PATH/TO/GLUTEN"
```

Below script shows an example about how to run the testing, you should modify the parameters such as executor cores, memory, offHeap size based on your environment. 

```shell script
export gluten_jvm_jar = /PATH/TO/GLUTEN/backends-velox/target/gluten-spark3.2_2.12-1.0.0-snapshot-jar-with-dependencies.jar 
cat tpch_parquet.scala | spark-shell --name tpch_powertest_velox \
  --master yarn --deploy-mode client \
  --conf spark.plugins=io.glutenproject.GlutenPlugin \
  --conf spark.gluten.sql.columnar.backend.lib=velox \
  --conf spark.driver.extraClassPath=${gluten_jvm_jar} \
  --conf spark.executor.extraClassPath=${gluten_jvm_jar} \
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

### Result

![TPC-H Q6](./image/TPC-H_Q6_DAG.png)

### Performance

Below table shows the TPC-H Q1 and Q6 Performance in a multiple-thread test (--num-executors 6 --executor-cores 6) for Velox and vanilla Spark.
Both Parquet and ORC datasets are sf1024.

| Query Performance (s) | Velox (ORC) | Vanilla Spark (Parquet) | Vanilla Spark (ORC) |
|---------------- | ----------- | ------------- | ------------- |
| TPC-H Q6 | 13.6 | 21.6  | 34.9 |
| TPC-H Q1 | 26.1 | 76.7 | 84.9 |

# External reference setup

TO ease your first-hand experience of using Gluten, we have set up an external reference cluster. If you are interested, please contact Weiting.Chen@intel.com.

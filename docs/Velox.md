# Velox Backend

Currently the mvn script can automatically fetch and build all dependency libraries incluing Velox and Arrow. Our nightly build still use Velox under oap-project. 

Velox use the script setup-ubuntu.sh to install all dependency libraries, but Arrow's dependency library can't be installed. So we need to install them manually:

```shell script
apt install maven build-essential cmake libssl-dev libre2-dev libcurl4-openssl-dev clang lldb lld libz-dev
```

Also we need to setup the JAVA_HOME env.
```shell script
export JAVA_HOME=path/to/java/home
export PATH=$JAVA_HOME/bin:$PATH
```


The script to build Velox backend jars:

```shell script
mvn clean package -DskipTests -Dcheckstyle.skip -Pbackends-velox -Dbuild_protobuf=OFF -Dbuild_cpp=ON -Dbuild_velox=ON -Dbuild_velox_from_source=ON -Dbuild_arrow=ON
```

In Gluten, all 22 queries can be fully offloaded into Velox for computing.  

## Test TPC-H Q1 and Q6 on Gluten with Velox backend

### Data preparation

Parquet format still have performance issue in Velox. We use dwrf format instead. Refer to backends-velox/workload/tpch for data generation.

Considering Velox's support for Decimal and Date are not fully ready, the script transforms Decimal to Double and Date to String. As a result, we need to modify the TPCH queries a bit. You can find them here: backends-velox/workload/tpch/tpch.queries.updated

### Submit the Spark SQL job


Submit test script from spark-shell.

```shell script
cat tpch_q6.scala | spark-shell --name tpch_velox_q6 --master yarn --deploy-mode client --conf spark.plugins=io.glutenproject.GlutenPlugin --conf --conf spark.gluten.sql.columnar.backend.lib=velox --conf spark.driver.extraClassPath=${gluten_jvm_jar} --conf spark.executor.extraClassPath=${gluten_jvm_jar} --conf spark.memory.offHeap.size=20g --conf spark.sql.sources.useV1SourceList=avro --num-executors 6 --executor-cores 6 --driver-memory 20g --executor-memory 25g --conf spark.executor.memoryOverhead=5g --conf spark.driver.maxResultSize=32g
```

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

TO ease your first hand experience of using Gluten, we have setup an external reference cluster. If you are interested, please contact Weiting.Chen@intel.com


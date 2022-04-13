### Gluten Usage

#### How to compile Gluten jar

``` shell
git clone -b master https://github.com/oap-project/gluten.git
cd gluten
```

When compiling Gluten, a backend should be enabled for execution.
For example, add below options to enable Velox backend:

```shell script
-Dbuild_velox=ON -Dvelox_home=${VELOX_HOME}
```

The full compiling command would be like:

```shell script
mvn clean package -P full-scala-compiler -DskipTests -Dcheckstyle.skip -Dbuild_cpp=ON -Dbuild_velox=ON -Dvelox_home=${VELOX_HOME}
```

If Arrow has once been installed successfully on your env, and there is no change to Arrow, you can
add below config to disable Arrow compilation each time when compiling Gluten:

```shell script
-Dbuild_arrow=OFF -Darrow_root=${ARROW_LIB_PATH}
```

Based on the different environment, there are some parameters can be set via -D with mvn.

| Parameters | Description | Default Value |
| ---------- | ----------- | ------------- |
| build_cpp | Enable or Disable building CPP library | OFF |
| cpp_tests | Enable or Disable CPP Tests | OFF |
| build_arrow | Build Arrow from Source | ON |
| arrow_root | When build_arrow set to False, arrow_root will be enabled to find the location of your existing arrow library. | /usr/local |
| build_protobuf | Build Protobuf from Source. If set to False, default library path will be used to find protobuf library. |ON |
| build_velox | Enable or Disable building Velox as a backend. | OFF |
| velox_home (only valid when build_velox is ON) | When building Gluten with Velox, the location of Velox should be set. | /root/velox |

When build_arrow set to True, the build_arrow.sh will be launched and compile a custom arrow library from [OAP Arrow](https://github.com/oap-project/arrow/tree/arrow-7.0.0-oap)
If you wish to change any parameters from Arrow, you can change it from the [build_arrow.sh](../tools/build_arrow.sh) script.

#### How to submit Spark Job

##### Key Spark Configurations when using Gluten

```shell script
spark.plugins io.glutenproject.GlutenPlugin
spark.gluten.sql.columnar.backend.lib ${BACKEND}
spark.sql.sources.useV1SourceList avro
spark.memory.offHeap.size 20g
spark.driver.extraClassPath ${GLUTEN_HOME}/jvm/target/gluten-jvm-<version>-snapshot-jar-with-dependencies.jar
spark.executor.extraClassPath ${GLUTEN_HOME}/jvm/target/gluten-jvm-<version>-snapshot-jar-with-dependencies.jar
```

Below is an example of the script to submit Spark SQL query.

cat query.scala
```shell script
// For ORC
val table = spark.read.format("orc").load("${ORC_PATH}")
// For Parquet
val table = spark.read.parquet("${PARQURT_PATH}")
table.createOrReplaceTempView("${TABLE_NAME}")
time{spark.sql("${QUERY}").show}
```

Submit the above script from spark-shell to trigger a Spark Job with certain configurations.

```shell script
cat query.scala | spark-shell --name query --master yarn --deploy-mode client --conf spark.plugins=io.glutenproject.GlutenPlugin --conf spark.gluten.sql.columnar.backend.lib=${BACKEND} --conf spark.driver.extraClassPath=${gluten_jvm_jar} --conf spark.executor.extraClassPath=${gluten_jvm_jar} --conf spark.memory.offHeap.size=20g --conf spark.sql.sources.useV1SourceList=avro --num-executors 6 --executor-cores 6 --driver-memory 20g --executor-memory 25g --conf spark.executor.memoryOverhead=5g --conf spark.driver.maxResultSize=32g
```

# Spark Configurations for Gluten Plugin

There are many configuration could impact the Gluten Plugin performance and can be fine tune in Spark.
You can add these configuration into spark-defaults.conf to enable or disable the setting.

| Parameters | Description | Recommend Setting |
| ---------- | ----------- | --------------- |
| spark.driver.extraClassPath | To add Gluten Plugin jar file in Spark Driver | /path/to/jar_file |
| spark.executor.extraClassPath | To add Gluten Plugin jar file in Spark Executor | /path/to/jar_file |
| spark.executorEnv.LIBARROW_DIR | To set up the location of Arrow library, by default it will search the loation of jar to be uncompressed | /path/to/arrow_library/ |
| spark.executorEnv.CC | To set up the location of gcc | /path/to/gcc/ |
| spark.executor.memory| To set up how much memory to be used for Spark Executor. | |
| spark.memory.offHeap.size| To set up how much memory to be used for Java OffHeap.<br /> Please notice Gluten Plugin will leverage this setting to allocate memory space for native usage even offHeap is disabled. <br /> The value is based on your system and it is recommended to set it larger if you are facing Out of Memory issue in Gluten Plugin | 30G |
| spark.sql.sources.useV1SourceList | Choose to use V1 source | avro |
| spark.sql.join.preferSortMergeJoin | To turn off preferSortMergeJoin in Spark | false |
| spark.plugins | To turn on Gluten Plugin | com.intel.oap.GlutenPlugin |
| spark.shuffle.manager | To turn on Gluten Columnar Shuffle Plugin | org.apache.spark.shuffle.sort.ColumnarShuffleManager |
| spark.gluten.sql.columnar.batchscan | Enable or Disable Columnar Batchscan, default is true | true |
| spark.gluten.sql.columnar.hashagg | Enable or Disable Columnar Hash Aggregate, default is true | true |
| spark.gluten.sql.columnar.projfilter | Enable or Disable Columnar Project and Filter, default is true | true |
| spark.gluten.sql.columnar.codegen.sort | Enable or Disable Columnar Sort, default is true | true |
| spark.gluten.sql.columnar.window | Enable or Disable Columnar Window, default is true | true |
| spark.gluten.sql.columnar.shuffledhashjoin | Enable or Disable ShffuledHashJoin, default is true | true |
| spark.gluten.sql.columnar.sortmergejoin | Enable or Disable Columnar Sort Merge Join, default is true | true |
| spark.gluten.sql.columnar.union | Enable or Disable Columnar Union, default is true | true |
| spark.gluten.sql.columnar.expand | Enable or Disable Columnar Expand, default is true | true |
| spark.gluten.sql.columnar.broadcastexchange | Enable or Disable Columnar Broadcast Exchange, default is true | true |
| spark.gluten.sql.columnar.nanCheck | Enable or Disable Nan Check, default is true | true |
| spark.gluten.sql.columnar.hashCompare | Enable or Disable Hash Compare in HashJoins or HashAgg, default is true | true |
| spark.gluten.sql.columnar.broadcastJoin | Enable or Disable Columnar BradcastHashJoin, default is true | true |
| spark.gluten.sql.columnar.wholestagecodegen | Enable or Disable Columnar WholeStageCodeGen, default is true | true |
| spark.gluten.sql.columnar.preferColumnar | Enable or Disable Columnar Operators, default is false.<br /> This parameter could impact the performance in different case. In some cases, to set false can get some performance boost. | false |
| spark.gluten.sql.columnar.joinOptimizationLevel | Fallback to row operators if there are several continous joins | 6 |
| spark.sql.execution.arrow.maxRecordsPerBatch | Set up the Max Records per Batch | 10000 |
| spark.gluten.sql.columnar.wholestagecodegen.breakdownTime | Enable or Disable metrics in Columnar WholeStageCodeGen | false |
| spark.gluten.sql.columnar.tmp_dir | Set up a folder to store the codegen files | /tmp |
| spark.gluten.sql.columnar.shuffle.customizedCompression.codec | Set up the codec to be used for Columnar Shuffle, default is lz4| lz4 |
| spark.gluten.sql.columnar.numaBinding | Set up NUMABinding, default is false| true |
| spark.gluten.sql.columnar.coreRange | Set up the core range for NUMABinding, only works when numaBinding set to true. <br /> The setting is based on the number of cores in your system. Use 72 cores as an example. | 0-17,36-53 &#124;18-35,54-71 |

Below is an example for spark-default.conf, if you are using conda to install OAP project.

```
##### Columnar Process Configuration

spark.sql.sources.useV1SourceList avro
spark.plugins io.glutenproject.GlutenPlugin
spark.shuffle.manager org.apache.spark.shuffle.sort.ColumnarShuffleManager
spark.gluten.sql.columnar.backend.lib=velox # Valid options: velox, clickhouse, gazelle-cpp
# Options: ${GLUTEN_HOME}/package/velox/spark33/target/gluten-<>-jar-with-dependencies.jar
spark.driver.extraClassPath ${GLUTEN_HOME}/package/velox/spark32/target/gluten-<>-jar-with-dependencies.jar
spark.executor.extraClassPath ${GLUTEN_HOME}/package/velox/spark32/target/gluten-<>-jar-with-dependencies.jar
######
```

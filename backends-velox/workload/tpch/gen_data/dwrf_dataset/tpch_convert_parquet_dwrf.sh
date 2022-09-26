batchsize=20480

export GLUTEN_HOME=/PATH/TO/gluten/
export GLUTEN_JVM_JAR=${GLUTEN_HOME}/backends-velox/target/gluten-1.0.0-SNAPSHOT-jar-with-dependencies.jar
SPARK_HOME=/home/sparkuser/spark/

cat tpch_convert_parquet_dwrf.scala  | ${SPARK_HOME}/bin/spark-shell                            \
 --name convert_parquet_dwrf                                      \
 --master yarn                                             \
 --deploy-mode client                                      \
 --driver-memory 20g                                       \
 --executor-cores 8                                      \
 --num-executors 14                                       \
 --executor-memory 30g                                   \
 --conf spark.plugins=io.glutenproject.GlutenPlugin        \
 --conf spark.gluten.sql.columnar.backend.lib=velox        \
 --conf spark.driver.extraClassPath=${GLUTEN_JVM_JAR}      \
 --conf spark.executor.extraClassPath=${GLUTEN_JVM_JAR}    \
 --conf spark.memory.offHeap.size=30g                      \
 --conf spark.executor.memoryOverhead=5g                   \
 --conf spark.driver.maxResultSize=32g                     \
 --conf spark.sql.autoBroadcastJoinThreshold=-1            \
 --conf spark.sql.parquet.columnarReaderBatchSize=${batchsize} \
 --conf spark.sql.inMemoryColumnarStorage.batchSize=${batchsize} \
 --conf spark.sql.execution.arrow.maxRecordsPerBatch=${batchsize} \
 --conf spark.gluten.sql.columnar.forceshuffledhashjoin=true \
 --conf spark.sql.broadcastTimeout=4800 \
 --conf spark.driver.maxResultSize=4g \
 --conf spark.sql.adaptive.enabled=true \
 --conf spark.sql.shuffle.partitions=112 \
 --conf spark.sql.sources.useV1SourceList=avro             \
 --conf spark.shuffle.manager=org.apache.spark.shuffle.sort.ColumnarShuffleManager \
 --conf spark.sql.files.maxPartitionBytes=1073741824 \

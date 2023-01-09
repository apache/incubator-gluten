GLUTEN_JAR=/PATH_TO_GLUTEN_HOME/package/velox/spark32/target/gluten-spark3.2_2.12-1.0.0-SNAPSHOT-jar-with-dependencies.jar
SPARK_HOME=/PATH_TO_SPARK_HOME/
cat tpch_parquet.scala | ${SPARK_HOME}/bin/spark-shell \
  --master yarn --deploy-mode client \
  --conf spark.plugins=io.glutenproject.GlutenPlugin \
  --conf spark.gluten.sql.columnar.backend.lib=velox \
  --conf spark.driver.extraClassPath=${GLUTEN_JAR} \
  --conf spark.executor.extraClassPath=${GLUTEN_JAR} \
  --conf spark.memory.offHeap.enabled=true \
  --conf spark.memory.offHeap.size=2g \
  --conf spark.gluten.sql.columnar.forceshuffledhashjoin=true \
  --conf spark.shuffle.manager=org.apache.spark.shuffle.sort.ColumnarShuffleManager \
  --num-executors 3 \
  --executor-cores 3 \
  --driver-memory 2g \
  --executor-memory 2g \
  --conf spark.executor.memoryOverhead=2g \
  --conf spark.driver.maxResultSize=2g


batchsize=10240
SPARK_HOME=/home/sparkuser/spark/
spark_sql_perf_jar=/PATH/TO/spark-sql-perf_2.12-0.5.1-SNAPSHOT.jar
cat tpcds_datagen_parquet.scala | ${SPARK_HOME}/bin/spark-shell \
  --num-executors 14 \
  --name tpcds_gen_parquet \
  --executor-memory 25g \
  --executor-cores 8 \
  --master yarn \
  --driver-memory 50g \
  --deploy-mode client \
  --conf spark.executor.memoryOverhead=1g \
  --conf spark.sql.parquet.columnarReaderBatchSize=${batchsize} \
  --conf spark.sql.inMemoryColumnarStorage.batchSize=${batchsize} \
  --conf spark.sql.execution.arrow.maxRecordsPerBatch=${batchsize} \
  --conf spark.sql.broadcastTimeout=4800 \
  --conf spark.driver.maxResultSize=4g \
  --conf spark.sql.sources.useV1SourceList=avro \
  --conf spark.sql.shuffle.partitions=224 \
  --jars ${spark_sql_perf_jar}

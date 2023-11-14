# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

batchsize=10240
SPARK_HOME=/home/sparkuser/spark/
spark_sql_perf_jar=/PATH/TO/spark-sql-perf_2.12-0.5.1-SNAPSHOT.jar
cat tpch_datagen_parquet.scala | ${SPARK_HOME}/bin/spark-shell \
  --num-executors 14 \
  --name tpch_gen_parquet \
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

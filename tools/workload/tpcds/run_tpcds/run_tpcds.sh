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

GLUTEN_JAR=/PATH_TO_GLUTEN_HOME/package/target/<gluten-jar>
SPARK_HOME=/PATH_TO_SPARK_HOME/

cat tpcds_parquet.scala | ${SPARK_HOME}/bin/spark-shell \
  --master yarn --deploy-mode client \
  --conf spark.plugins=org.apache.gluten.GlutenPlugin \
  --conf spark.driver.extraClassPath=${GLUTEN_JAR} \
  --conf spark.executor.extraClassPath=${GLUTEN_JAR} \
  --conf spark.memory.offHeap.enabled=true \
  --conf spark.memory.offHeap.size=2g \
  --conf spark.gluten.sql.columnar.forceShuffledHashJoin=true \
  --conf spark.shuffle.manager=org.apache.spark.shuffle.sort.ColumnarShuffleManager \
  --num-executors 3 \
  --executor-cores 3 \
  --driver-memory 2g \
  --executor-memory 2g \
  --conf spark.executor.memoryOverhead=2g \
  --conf spark.driver.maxResultSize=2g

  # If issues with missing shared dependency libraries are reported at runtime,
  # build a separate Gluten JAR with the third-party shared libraries included,
  # and then set the following configuration to enable Gluten to load the shared libraries.
  # e.g.
  #   --conf spark.gluten.loadLibFromJar=true \
  #   --jars <path_to_Gluten_thirdparty_lib_JAR>

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.glutenproject.execution

import org.apache.commons.io.FileUtils
import org.apache.spark.SparkConf

import java.io.File

class ArrowParquetWriteSuite extends WholeStageTransformerSuite {
  override protected val backend: String = "gazelle_cpp"
  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  protected val rootPath: String = getClass.getResource("/").getPath
  protected val writePath: String = rootPath + "unit-tests-working-home"

  override def beforeAll(): Unit = {
    val basePathDir = new File(writePath)
    if (basePathDir.exists()) {
      FileUtils.forceDelete(basePathDir)
    }
    FileUtils.forceMkdir(basePathDir)

    super.beforeAll()
  }

  protected override def afterAll(): Unit = {
    super.afterAll()
    FileUtils.forceDelete(new File(writePath))
  }

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.files.maxPartitionBytes", "1g")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.memory.offHeap.size", "2g")
      .set("spark.unsafe.exceptionOnMemoryLeak", "false")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
  }

  test("Parquet Write test") {
    val path = getClass.getResource(resourcePath).getFile
    val df = spark.read.format(fileFormat).load(path)
    df.write.mode("append").format(fileFormat).save(writePath)
    val writeDF = spark.read.format(fileFormat).load(writePath)
    checkAnswer(df, writeDF)
  }
}

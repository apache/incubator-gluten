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
package org.apache.gluten.execution.compatibility

import org.apache.gluten.GlutenConfig
import org.apache.gluten.execution.GlutenClickHouseWholeStageTransformerSuite
import org.apache.gluten.execution.hive.ReCreateHiveSession
import org.apache.gluten.utils.UTSystemParameters

import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper

class GlutenClickHouseHiveSuite
  extends GlutenClickHouseWholeStageTransformerSuite
  with ReCreateHiveSession
  with AdaptiveSparkPlanHelper {

  override protected def sparkConf: SparkConf = {
    new SparkConf()
      .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "536870912")
      .set("spark.sql.catalogImplementation", "hive")
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.sql.files.maxPartitionBytes", "1g")
      .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.files.minPartitionNum", "1")
      .set("spark.gluten.sql.columnar.columnartorow", "true")
      .set("spark.gluten.sql.columnar.backend.ch.worker.id", "1")
      .set(GlutenConfig.GLUTEN_LIB_PATH, UTSystemParameters.clickHouseLibPath)
      .set("spark.gluten.sql.columnar.iterator", "true")
      .set("spark.gluten.sql.columnar.hashagg.enablefinal", "true")
      .set("spark.gluten.sql.enable.native.validation", "false")
      .set("spark.gluten.sql.parquet.maxmin.index", "true")
      .set(
        "spark.sql.warehouse.dir",
        getClass.getResource("/").getPath + "tests-working-home/spark-warehouse")
      .set("spark.hive.exec.dynamic.partition.mode", "nonstrict")
      .set("spark.gluten.supported.hive.udfs", "my_add")
      .set("spark.gluten.sql.columnar.backend.ch.runtime_config.use_local_format", "true")
      .set("spark.gluten.sql.columnar.backend.ch.runtime_settings.allow_read_json", "false")
      .set(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.execution.datasources.v2.clickhouse.ClickHouseSparkCatalog")
      .setMaster("local[*]")
  }

  test("GLUTEN-7325: enable fallback to spark for read json") {
    val jarPath = "jars/json-serde-1.3.8-SNAPSHOT-jar-with-dependencies.jar"
    val jarUrl = s"file://$rootPath/$jarPath"
    sql(s"ADD JAR '$jarUrl'")
    withTable("test_7325") {
      val external_path = rootPath + "/text-data/json-without-quote/"
      sql(
        s"""
           | create table test_7325(`apps` string)
           | ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
           | STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
           | OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
           | LOCATION '$external_path'
           |""".stripMargin
      )
      compareResultsAgainstVanillaSpark(
        """
          |select apps from test_7325
          |""".stripMargin,
        true,
        { _ => },
        false
      )
    }
  }
}

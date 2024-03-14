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

import io.glutenproject.GlutenConfig

import org.apache.spark.SparkConf

class VeloxIcebergSuite extends WholeStageTransformerSuite {

  protected val rootPath: String = getClass.getResource("/").getPath
  override protected val backend: String = "velox"
  override protected val resourcePath: String = "/tpch-data-parquet-velox"
  override protected val fileFormat: String = "parquet"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.files.maxPartitionBytes", "1g")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.memory.offHeap.size", "2g")
      .set("spark.unsafe.exceptionOnMemoryLeak", "true")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
      .set(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
      .set("spark.sql.catalog.spark_catalog.type", "hadoop")
      .set("spark.sql.catalog.spark_catalog.warehouse", s"file://$rootPath/tpch-data-iceberg-velox")
  }

  private def isSparkVersionAtleast(version: String): Boolean = {
    val currentVersion = spark.version
    val currentVersionSplit = currentVersion.split("\\.")
    val versionSplit = version.split("\\.")
    currentVersionSplit.zip(versionSplit).foreach {
      case (current, required) =>
        if (current.toInt > required.toInt) {
          return true
        } else if (current.toInt < required.toInt) {
          return false
        }
    }
    true
  }

  test("iceberg transformer exists") {
    spark.sql("""
                |create table iceberg_tb using iceberg as
                |(select 1 as col1, 2 as col2, 3 as col3)
                |""".stripMargin)

    runQueryAndCompare("""
                         |select * from iceberg_tb;
                         |""".stripMargin) {
      checkOperatorMatch[IcebergScanTransformer]
    }
  }

  test("iceberg bucketed join") {
    assume(isSparkVersionAtleast("3.4"))
    val leftTable = "p_str_tb"
    val rightTable = "p_int_tb"
    withTable(leftTable, rightTable) {
      // Partition key of string type.
      withSQLConf(GlutenConfig.GLUTEN_ENABLE_KEY -> "false") {
        // Gluten does not support write iceberg table.
        spark.sql(s"""
                     |create table $leftTable(id int, name string, p string)
                     |using iceberg
                     |partitioned by (bucket(4, id));
                     |""".stripMargin)
        spark.sql(
          s"""
             |insert into table $leftTable values
             |(4, 'a5', 'p4'),
             |(1, 'a1', 'p1'),
             |(2, 'a3', 'p2'),
             |(1, 'a2', 'p1'),
             |(3, 'a4', 'p3');
             |""".stripMargin
        )
      }

      // Partition key of integer type.
      withSQLConf(
        GlutenConfig.GLUTEN_ENABLE_KEY -> "false"
      ) {
        // Gluten does not support write iceberg table.
        spark.sql(s"""
                     |create table $rightTable(id int, name string, p int)
                     |using iceberg
                     |partitioned by (bucket(4, id));
                     |""".stripMargin)
        spark.sql(
          s"""
             |insert into table $rightTable values
             |(3, 'b4', 23),
             |(1, 'b2', 21),
             |(4, 'b5', 24),
             |(2, 'b3', 22),
             |(1, 'b1', 21);
             |""".stripMargin
        )
      }

      withSQLConf(
        "spark.sql.sources.v2.bucketing.enabled" -> "true",
        "spark.sql.requireAllClusterKeysForCoPartition" -> "false",
        "spark.sql.adaptive.enabled" -> "false",
        "spark.sql.iceberg.planning.preserve-data-grouping" -> "true",
        "spark.sql.autoBroadcastJoinThreshold" -> "-1",
        "spark.sql.sources.v2.bucketing.pushPartValues.enabled" -> "true"
      ) {
        runQueryAndCompare(s"""
                              |select s.id, s.name, i.name, i.p
                              | from $leftTable s inner join $rightTable i
                              | on s.id = i.id;
                              |""".stripMargin) {
          df =>
            {
              assert(
                getExecutedPlan(df).count(
                  plan => {
                    plan.isInstanceOf[IcebergScanTransformer]
                  }) == 2)
              getExecutedPlan(df).map {
                case plan if plan.isInstanceOf[IcebergScanTransformer] =>
                  assert(
                    plan.asInstanceOf[IcebergScanTransformer].getKeyGroupPartitioning.isDefined)
                  assert(plan.asInstanceOf[IcebergScanTransformer].getSplitInfos.length == 3)
                case _ => // do nothing
              }
              checkLengthAndPlan(df, 7)
            }
        }
      }
    }
  }

  test("iceberg bucketed join with partition") {
    assume(isSparkVersionAtleast("3.4"))
    val leftTable = "p_str_tb"
    val rightTable = "p_int_tb"
    withTable(leftTable, rightTable) {
      // Partition key of string type.
      withSQLConf(GlutenConfig.GLUTEN_ENABLE_KEY -> "false") {
        // Gluten does not support write iceberg table.
        spark.sql(s"""
                     |create table $leftTable(id int, name string, p int)
                     |using iceberg
                     |partitioned by (bucket(4, id), p);
                     |""".stripMargin)
        spark.sql(
          s"""
             |insert into table $leftTable values
             |(4, 'a5', 2),
             |(1, 'a1', 1),
             |(2, 'a3', 1),
             |(1, 'a2', 1),
             |(3, 'a4', 2);
             |""".stripMargin
        )
      }

      // Partition key of integer type.
      withSQLConf(
        GlutenConfig.GLUTEN_ENABLE_KEY -> "false"
      ) {
        // Gluten does not support write iceberg table.
        spark.sql(s"""
                     |create table $rightTable(id int, name string, p int)
                     |using iceberg
                     |partitioned by (bucket(4, id), p);
                     |""".stripMargin)
        spark.sql(
          s"""
             |insert into table $rightTable values
             |(3, 'b4', 2),
             |(1, 'b2', 1),
             |(4, 'b5', 2),
             |(2, 'b3', 1),
             |(1, 'b1', 1);
             |""".stripMargin
        )
      }

      withSQLConf(
        "spark.sql.sources.v2.bucketing.enabled" -> "true",
        "spark.sql.requireAllClusterKeysForCoPartition" -> "false",
        "spark.sql.adaptive.enabled" -> "false",
        "spark.sql.iceberg.planning.preserve-data-grouping" -> "true",
        "spark.sql.autoBroadcastJoinThreshold" -> "-1",
        "spark.sql.sources.v2.bucketing.pushPartValues.enabled" -> "true"
      ) {
        runQueryAndCompare(s"""
                              |select s.id, s.name, i.name, i.p
                              | from $leftTable s inner join $rightTable i
                              | on s.id = i.id and s.p = i.p;
                              |""".stripMargin) {
          df =>
            {
              assert(
                getExecutedPlan(df).count(
                  plan => {
                    plan.isInstanceOf[IcebergScanTransformer]
                  }) == 2)
              getExecutedPlan(df).map {
                case plan if plan.isInstanceOf[IcebergScanTransformer] =>
                  assert(
                    plan.asInstanceOf[IcebergScanTransformer].getKeyGroupPartitioning.isDefined)
                  assert(plan.asInstanceOf[IcebergScanTransformer].getSplitInfos.length == 3)
                case _ => // do nothing
              }
              checkLengthAndPlan(df, 7)
            }
        }
      }
    }
  }

  test("iceberg bucketed join with partition filter") {
    assume(isSparkVersionAtleast("3.4"))
    val leftTable = "p_str_tb"
    val rightTable = "p_int_tb"
    withTable(leftTable, rightTable) {
      // Partition key of string type.
      withSQLConf(GlutenConfig.GLUTEN_ENABLE_KEY -> "false") {
        // Gluten does not support write iceberg table.
        spark.sql(s"""
                     |create table $leftTable(id int, name string, p int)
                     |using iceberg
                     |partitioned by (bucket(4, id), p);
                     |""".stripMargin)
        spark.sql(
          s"""
             |insert into table $leftTable values
             |(4, 'a5', 2),
             |(1, 'a1', 1),
             |(2, 'a3', 1),
             |(1, 'a2', 1),
             |(3, 'a4', 2);
             |""".stripMargin
        )
      }

      // Partition key of integer type.
      withSQLConf(
        GlutenConfig.GLUTEN_ENABLE_KEY -> "false"
      ) {
        // Gluten does not support write iceberg table.
        spark.sql(s"""
                     |create table $rightTable(id int, name string, p int)
                     |using iceberg
                     |partitioned by (bucket(4, id), p);
                     |""".stripMargin)
        spark.sql(
          s"""
             |insert into table $rightTable values
             |(3, 'b4', 2),
             |(1, 'b2', 1),
             |(4, 'b5', 2),
             |(2, 'b3', 1),
             |(1, 'b1', 1);
             |""".stripMargin
        )
      }

      withSQLConf(
        "spark.sql.sources.v2.bucketing.enabled" -> "true",
        "spark.sql.requireAllClusterKeysForCoPartition" -> "false",
        "spark.sql.adaptive.enabled" -> "false",
        "spark.sql.iceberg.planning.preserve-data-grouping" -> "true",
        "spark.sql.autoBroadcastJoinThreshold" -> "-1",
        "spark.sql.sources.v2.bucketing.pushPartValues.enabled" -> "true"
      ) {
        runQueryAndCompare(s"""
                              |select s.id, s.name, i.name, i.p
                              | from $leftTable s inner join $rightTable i
                              | on s.id = i.id
                              | where s.p = 1 and i.p = 1;
                              |""".stripMargin) {
          df =>
            {
              assert(
                getExecutedPlan(df).count(
                  plan => {
                    plan.isInstanceOf[IcebergScanTransformer]
                  }) == 2)
              getExecutedPlan(df).map {
                case plan if plan.isInstanceOf[IcebergScanTransformer] =>
                  assert(
                    plan.asInstanceOf[IcebergScanTransformer].getKeyGroupPartitioning.isDefined)
                  assert(plan.asInstanceOf[IcebergScanTransformer].getSplitInfos.length == 1)
                case _ => // do nothing
              }
              checkLengthAndPlan(df, 5)
            }
        }
      }
    }
  }
}

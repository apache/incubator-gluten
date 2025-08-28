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
package org.apache.gluten.execution.enhanced

import org.apache.gluten.execution.{IcebergSuite, VeloxIcebergAppendDataExec, VeloxIcebergOverwriteByExpressionExec, VeloxIcebergReplaceDataExec}
import org.apache.gluten.tags.EnhancedFeaturesTest

import org.apache.spark.sql.execution.CommandResultExec

@EnhancedFeaturesTest
class VeloxIcebergSuite extends IcebergSuite {

  test("iceberg insert") {
    withTable("iceberg_tb2") {
      spark.sql("""
                  |create table if not exists iceberg_tb2(a int) using iceberg
                  |""".stripMargin)
      val df = spark.sql("""
                           |insert into table iceberg_tb2 values(1098)
                           |""".stripMargin)
      assert(
        df.queryExecution.executedPlan
          .asInstanceOf[CommandResultExec]
          .commandPhysicalPlan
          .isInstanceOf[VeloxIcebergAppendDataExec])
      val selectDf = spark.sql("""
                                 |select * from iceberg_tb2;
                                 |""".stripMargin)
      val result = selectDf.collect()
      assert(result.length == 1)
      assert(result(0).get(0) == 1098)
    }
  }

  // TODO: support later
  ignore("iceberg insert partition table identity transform") {
    withTable("iceberg_tb2") {
      spark.sql("""
                  |create table if not exists iceberg_tb2(a int, b int)
                  |using iceberg
                  |partitioned by (a);
                  |""".stripMargin)
      val df = spark.sql("""
                           |insert into table iceberg_tb2 values(1098, 189)
                           |""".stripMargin)
      assert(
        df.queryExecution.executedPlan
          .asInstanceOf[CommandResultExec]
          .commandPhysicalPlan
          .isInstanceOf[VeloxIcebergAppendDataExec])
      val selectDf = spark.sql("""
                                 |select * from iceberg_tb2;
                                 |""".stripMargin)
      val result = selectDf.collect()
      assert(result.length == 1)
      assert(result(0).get(0) == 1098)
      assert(result(0).get(1) == 189)
    }
  }

  test("iceberg read cow table - delete") {
    withTable("iceberg_cow_tb") {
      spark.sql("""
                  |create table iceberg_cow_tb (
                  |  id int,
                  |  name string,
                  |  p string
                  |) using iceberg
                  |tblproperties (
                  |  'format-version' = '2',
                  |  'write.delete.mode' = 'copy-on-write',
                  |  'write.update.mode' = 'copy-on-writ',
                  |  'write.merge.mode' = 'copy-on-writ'
                  |);
                  |""".stripMargin)

      // Insert some test rows.
      spark.sql("""
                  |insert into table iceberg_cow_tb
                  |values (1, 'a1', 'p1'), (2, 'a2', 'p1'), (3, 'a3', 'p2'),
                  |       (4, 'a4', 'p1'), (5, 'a5', 'p2'), (6, 'a6', 'p1');
                  |""".stripMargin)

      // Delete row.
      val df = spark.sql(
        """
          |delete from iceberg_cow_tb where name = 'a1';
          |""".stripMargin
      )
      assert(
        df.queryExecution.executedPlan
          .asInstanceOf[CommandResultExec]
          .commandPhysicalPlan
          .isInstanceOf[VeloxIcebergReplaceDataExec])
      val selectDf = spark.sql("""
                                 |select * from iceberg_cow_tb;
                                 |""".stripMargin)
      val result = selectDf.collect()
      assert(result.length == 5)
    }
  }

  test("iceberg insert overwrite") {
    withTable("iceberg_tb2") {
      spark.sql("""
                  |create table if not exists iceberg_tb2(a int) using iceberg
                  |""".stripMargin)

      spark.sql("insert into table iceberg_tb2 values (1)")

      // Overwrite table
      val df = spark.sql("""
                           |insert overwrite table iceberg_tb2 values (2)
                           |""".stripMargin)
      assert(
        df.queryExecution.executedPlan
          .asInstanceOf[CommandResultExec]
          .commandPhysicalPlan
          .isInstanceOf[VeloxIcebergOverwriteByExpressionExec])

      val selectDf = spark.sql("""
                                 |select * from iceberg_tb2;
                                 |""".stripMargin)
      val result = selectDf.collect()
      assert(result.length == 1)
      assert(result(0).get(0) == 2)
    }
  }
}

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
package org.apache.spark.sql.execution.datasources

import io.glutenproject.execution.SortExecTransformer

import org.apache.spark.sql.{GlutenSQLTestsBaseTrait, GlutenTestConstants}
import org.apache.spark.sql.catalyst.expressions.{Ascending, AttributeReference, NullsFirst, SortOrder}
import org.apache.spark.sql.execution.SortExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StringType}

class GlutenV1WriteCommandSuite extends V1WriteCommandSuite with GlutenSQLTestsBaseTrait {

  test(
    GlutenTestConstants.GLUTEN_TEST +
      "SPARK-41914: v1 write with AQE and in-partition sorted - non-string partition column") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
      withPlannedWrite {
        enabled =>
          withTable("t") {
            sql("""
                  |CREATE TABLE t(b INT, value STRING) USING PARQUET
                  |PARTITIONED BY (key INT)
                  |""".stripMargin)
            executeAndCheckOrdering(hasLogicalSort = true, orderingMatched = true) {
              sql("""
                    |INSERT INTO t
                    |SELECT b, value, key
                    |FROM testData JOIN testData2 ON key = a
                    |SORT BY key, value
                    |""".stripMargin)
            }

            // inspect the actually executed plan (that is different to executeAndCheckOrdering)
            assert(FileFormatWriter.executedPlan.isDefined)
            val executedPlan = FileFormatWriter.executedPlan.get

            val plan = if (enabled) {
              assert(executedPlan.isInstanceOf[WriteFilesExec])
              executedPlan.asInstanceOf[WriteFilesExec].child
            } else {
              executedPlan.transformDown { case a: AdaptiveSparkPlanExec => a.executedPlan }
            }

            // assert the outer most sort in the executed plan
            assert(
              plan
                .collectFirst {
                  case s: SortExec => s
                  case ns: SortExecTransformer => ns
                }
                .exists {
                  case SortExec(
                        Seq(
                          SortOrder(
                            AttributeReference("key", IntegerType, _, _),
                            Ascending,
                            NullsFirst,
                            _),
                          SortOrder(
                            AttributeReference("value", StringType, _, _),
                            Ascending,
                            NullsFirst,
                            _)
                        ),
                        false,
                        _,
                        _) =>
                    true
                  case SortExecTransformer(
                        Seq(
                          SortOrder(
                            AttributeReference("key", IntegerType, _, _),
                            Ascending,
                            NullsFirst,
                            _),
                          SortOrder(
                            AttributeReference("value", StringType, _, _),
                            Ascending,
                            NullsFirst,
                            _)
                        ),
                        false,
                        _,
                        _) =>
                    true
                  case _ => false
                },
              plan
            )
          }
      }
    }
  }

  test(
    GlutenTestConstants.GLUTEN_TEST +
      "SPARK-41914: v1 write with AQE and in-partition sorted - string partition column") {
    withPlannedWrite {
      enabled =>
        withTable("t") {
          sql("""
                |CREATE TABLE t(key INT, b INT) USING PARQUET
                |PARTITIONED BY (value STRING)
                |""".stripMargin)
          executeAndCheckOrdering(
            hasLogicalSort = true,
            orderingMatched = true,
            hasEmpty2Null = enabled) {
            sql("""
                  |INSERT INTO t
                  |SELECT key, b, value
                  |FROM testData JOIN testData2 ON key = a
                  |SORT BY value, key
                  |""".stripMargin)
          }

          // inspect the actually executed plan (that is different to executeAndCheckOrdering)
          assert(FileFormatWriter.executedPlan.isDefined)
          val executedPlan = FileFormatWriter.executedPlan.get

          val plan = if (enabled) {
            assert(executedPlan.isInstanceOf[WriteFilesExec])
            executedPlan.asInstanceOf[WriteFilesExec].child
          } else {
            executedPlan.transformDown { case a: AdaptiveSparkPlanExec => a.executedPlan }
          }

          // assert the outer most sort in the executed plan
          assert(
            plan
              .collectFirst {
                case s: SortExec => s
                case ns: SortExecTransformer => ns
              }
              .exists {
                case SortExec(
                      Seq(
                        SortOrder(
                          AttributeReference("value", StringType, _, _),
                          Ascending,
                          NullsFirst,
                          _),
                        SortOrder(
                          AttributeReference("key", IntegerType, _, _),
                          Ascending,
                          NullsFirst,
                          _)
                      ),
                      false,
                      _,
                      _) =>
                  true
                case SortExecTransformer(
                      Seq(
                        SortOrder(
                          AttributeReference("value", StringType, _, _),
                          Ascending,
                          NullsFirst,
                          _),
                        SortOrder(
                          AttributeReference("key", IntegerType, _, _),
                          Ascending,
                          NullsFirst,
                          _)
                      ),
                      false,
                      _,
                      _) =>
                  true
                case _ => false
              },
            plan
          )
        }
    }
  }
}

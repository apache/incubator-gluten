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
package org.apache.gluten.execution.tpcds

import org.apache.gluten.execution.{CHShuffledHashJoinExecTransformer, CHSortMergeJoinExecTransformer, GlutenClickHouseTPCDSAbstractSuite}
import org.apache.gluten.test.FallbackUtil

import org.apache.spark.SparkConf

class GlutenClickHouseTPCDSParquetSortMergeJoinSuite extends GlutenClickHouseTPCDSAbstractSuite {

  override protected def excludedTpcdsQueries: Set[String] = Set(
    "q72", // cause CI Pipeline to fail due to memory usage
    // fallback due to left semi/anti/existence join
    "q8",
    "q10",
    "q14a",
    "q14b",
    "116",
    "q23a",
    "q23b",
    "q35",
    "q38",
    "q51",
    "q69",
    "q70",
    "q78",
    "q87",
    "q95",
    "q97"
  ) ++ super.excludedTpcdsQueries

  /** Run Gluten + ClickHouse Backend with SortShuffleManager */
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "sort")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
      .set("spark.memory.offHeap.size", "6g")
      .set("spark.gluten.sql.columnar.forceShuffledHashJoin", "false")
      .setMaster("local[2]")
  }

  executeTPCDSTest(false)

  test("sort merge join: inner join") {
    withSQLConf("spark.sql.autoBroadcastJoinThreshold" -> "-1") {
      val testSql =
        """SELECT  count(*) cnt
          |FROM item i join item j on j.i_category = i.i_category
          |where
          |i.i_current_price > 1.0 """.stripMargin
      compareResultsAgainstVanillaSpark(
        testSql,
        compareResult = true,
        df => {
          val smjTransformers = df.queryExecution.executedPlan.collect {
            case f: CHSortMergeJoinExecTransformer => f
          }
          assert(smjTransformers.size == 1)
        }
      )
    }
  }

  test("sort merge join: left outer join") {
    withSQLConf("spark.sql.autoBroadcastJoinThreshold" -> "-1") {
      val testSql =
        """SELECT  count(*) cnt
          |FROM item i left outer join item j on j.i_category = i.i_category
        """.stripMargin
      compareResultsAgainstVanillaSpark(
        testSql,
        compareResult = true,
        df => {
          val smjTransformers = df.queryExecution.executedPlan.collect {
            case f: CHSortMergeJoinExecTransformer => f
          }
          assert(smjTransformers.size == 1)
        }
      )
    }
  }

  test("sort merge join: right outer join") {
    withSQLConf("spark.sql.autoBroadcastJoinThreshold" -> "-1") {
      val testSql =
        """SELECT  count(*) cnt
          |FROM item i right outer join item j on j.i_category = i.i_category
        """.stripMargin
      compareResultsAgainstVanillaSpark(
        testSql,
        compareResult = true,
        df => {
          val smjTransformers = df.queryExecution.executedPlan.collect {
            case f: CHSortMergeJoinExecTransformer => f
          }
          assert(smjTransformers.size == 1)
        }
      )
    }
  }

  test("sort merge join: left semi join should be replaced with hash join") {
    withSQLConf("spark.sql.autoBroadcastJoinThreshold" -> "-1") {
      val testSql =
        """SELECT  count(*) cnt
          |FROM item i left semi join item j on j.i_category = i.i_category
          |where
          |i.i_current_price > 1.0 """.stripMargin
      val df = spark.sql(testSql)
      val smjTransformers = df.queryExecution.executedPlan.collect {
        case f: CHSortMergeJoinExecTransformer => f
      }
      val hashJoinTransformers = df.queryExecution.executedPlan.collect {
        case f: CHShuffledHashJoinExecTransformer => f
      }
      assert(smjTransformers.size == 0)
      assert(hashJoinTransformers.size > 0)
      assert(!FallbackUtil.hasFallback(df.queryExecution.executedPlan))
    }
  }

  test("sort merge join: left anti join should be replace with hash join") {
    withSQLConf("spark.sql.autoBroadcastJoinThreshold" -> "-1") {
      val testSql =
        """SELECT  count(*) cnt
          |FROM item i left anti join item j on j.i_category = i.i_category
          |where
          |i.i_current_price > 1.0 """.stripMargin
      val df = spark.sql(testSql)
      val smjTransformers = df.queryExecution.executedPlan.collect {
        case f: CHSortMergeJoinExecTransformer => f
      }
      val hashJoinTransformers = df.queryExecution.executedPlan.collect {
        case f: CHShuffledHashJoinExecTransformer => f
      }
      assert(smjTransformers.size == 0)
      assert(hashJoinTransformers.size > 0)
      assert(!FallbackUtil.hasFallback(df.queryExecution.executedPlan))
    }
  }

  val createItem: String =
    """CREATE TABLE myitem (
      |  i_current_price DECIMAL(7,2),
      |  i_category STRING)
      |USING parquet""".stripMargin

  val insertItem: String =
    """insert into myitem values
      |(null,null),
      |(null,null),
      |(0.63,null),
      |(0.74,null),
      |(null,null),
      |(90.72,'Books'),
      |(99.89,'Books'),
      |(99.41,'Books')
      |""".stripMargin

  test("sort merge join: full outer join") {
    withTable("myitem") {
      withSQLConf("spark.sql.autoBroadcastJoinThreshold" -> "-1") {
        spark.sql(createItem)
        spark.sql(insertItem)
        val testSql =
          """SELECT  count(*) cnt
            |FROM myitem i full outer join myitem j on j.i_category = i.i_category
          """.stripMargin
        compareResultsAgainstVanillaSpark(
          testSql,
          compareResult = true,
          df => {
            val smjTransformers = df.queryExecution.executedPlan.collect {
              case f: CHSortMergeJoinExecTransformer => f
            }
            assert(smjTransformers.size == 1)
          }
        )
      }
    }
  }

  test("sort merge join: nulls smallest") {
    withTable("myitem") {
      withSQLConf(
        "spark.sql.autoBroadcastJoinThreshold" -> "-1",
        "spark.sql.shuffle.partitions" -> "3") {
        spark.sql(createItem)
        spark.sql(insertItem)
        val testSql =
          """SELECT  count(*) cnt
            |FROM myitem i
            |where
            |i.i_current_price > 1.0 *
            |  (SELECT avg(j.i_current_price)
            |  FROM myitem j
            |  WHERE j.i_category = i.i_category
            | ) """.stripMargin
        spark.sql(testSql).explain()
        spark.sql(testSql).show()
        compareResultsAgainstVanillaSpark(
          testSql,
          compareResult = true,
          df => {
            val smjTransformers = df.queryExecution.executedPlan.collect {
              case f: CHSortMergeJoinExecTransformer => f
            }
            assert(smjTransformers.size == 1)
          }
        )
      }
    }

  }

}

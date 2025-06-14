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
package org.apache.spark.sql

import org.apache.spark.sql.execution.{ColumnarCollapseTransformStages, CostMode, ExtendedMode, FormattedMode, SimpleMode}
import org.apache.spark.sql.functions.{avg, col, count, sum}
import org.apache.spark.sql.internal.SQLConf

class GlutenExplainSuite extends ExplainSuite with GlutenSQLTestsBaseTrait {

  import testImplicits._

  override def withNormalizedExplain(queryText: String)(f: String => Unit): Unit = {
    val output = new java.io.ByteArrayOutputStream()
    Console.withOut(output) {
      sql(queryText).show(false)
    }
    val normalizedOutput = output.toString.replaceAll("#\\d+", "#x")
    f(normalizedOutput)
  }

  // Actual query is executed in native but creation of data frame is in Spark so
  // 2 WholeStageCodegen
  testGluten("SPARK-33853: explain codegen - check presence of subquery") {
    withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true") {
      withTempView("df") {
        val df1 = spark.range(1, 100)
        df1.createTempView("df")

        val sqlText = "EXPLAIN CODEGEN SELECT (SELECT min(id) FROM df)"
        val expectedText = "Found 2 WholeStageCodegen subtrees."

        withNormalizedExplain(sqlText) {
          normalizedOutput => assert(normalizedOutput.contains(expectedText))
        }
      }
    }
  }

  // Path is spark-warehouse
  testGluten("explain formatted - check presence of subquery in case of DPP") {
    withTable("df1", "df2") {
      withSQLConf(
        SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
        SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "false",
        SQLConf.EXCHANGE_REUSE_ENABLED.key -> "true",
        "spark.native.enabled" -> "false",
        SQLConf.DYNAMIC_PARTITION_PRUNING_FALLBACK_FILTER_RATIO.key -> "0.5"
      ) {
        spark
          .range(1000)
          .select(col("id"), col("id").as("k"))
          .write
          .partitionBy("k")
          .format("parquet")
          .mode("overwrite")
          .saveAsTable("df1")

        spark
          .range(100)
          .select(col("id"), col("id").as("k"))
          .write
          .partitionBy("k")
          .format("parquet")
          .mode("overwrite")
          .saveAsTable("df2")

        val sqlText =
          """
            |EXPLAIN FORMATTED SELECT df1.id, df2.k
            |FROM df1 JOIN df2 ON df1.k = df2.k AND df2.id < 2
            |""".stripMargin

        val expected_pattern1 =
          "Subquery:1 Hosting operator id = 1 Hosting Expression = k#xL IN dynamicpruning#x"
        val expected_pattern2 =
          "PartitionFilters: \\[isnotnull\\(k#xL\\), dynamicpruningexpression\\(k#xL " +
            "IN dynamicpruning#x\\)\\]"
        val expected_pattern3 =
          "Location: InMemoryFileIndex \\[\\S*spark-warehouse" +
            "/df2/\\S*, ... 99 entries\\]"
        val expected_pattern4 =
          "Location: InMemoryFileIndex \\[\\S*spark-warehouse" +
            "/df1/\\S*, ... 999 entries\\]"
        withNormalizedExplain(sqlText) {
          normalizedOutput =>
            assert(expected_pattern1.r.findAllMatchIn(normalizedOutput).length == 1)
            assert(expected_pattern2.r.findAllMatchIn(normalizedOutput).length == 1)
            assert(expected_pattern3.r.findAllMatchIn(normalizedOutput).length == 1)
            assert(expected_pattern4.r.findAllMatchIn(normalizedOutput).length == 1)
        }
      }
    }
  }

  // No codegen in plan
  testGluten("Support ExplainMode in Dataset.explain") {
    val df1 = Seq((1, 2), (2, 3)).toDF("k", "v1")
    val df2 = Seq((2, 3), (1, 1)).toDF("k", "v2")
    val testDf = df1.join(df2, "k").groupBy("k").agg(count("v1"), sum("v1"), avg("v2"))

    val simpleExplainOutput = getNormalizedExplain(testDf, SimpleMode)
    assert(simpleExplainOutput.startsWith("== Physical Plan =="))
    Seq("== Parsed Logical Plan ==", "== Analyzed Logical Plan ==", "== Optimized Logical Plan ==")
      .foreach(planType => assert(!simpleExplainOutput.contains(planType)))
    checkKeywordsExistsInExplain(
      testDf,
      ExtendedMode,
      "== Parsed Logical Plan ==" ::
        "== Analyzed Logical Plan ==" ::
        "== Optimized Logical Plan ==" ::
        "== Physical Plan ==" ::
        Nil: _*)
    checkKeywordsExistsInExplain(
      testDf,
      CostMode,
      "Statistics(sizeInBytes=" ::
        Nil: _*)
    checkKeywordsExistsInExplain(
      testDf,
      FormattedMode,
      "+- LocalTableScan (1)" ::
        Nil: _*)
  }

  // Need to reset WholeStageCodegenTransformer stage ID
  testGluten("SPARK-31504: Output fields in formatted Explain should have determined order") {
    withTempPath {
      path =>
        spark
          .range(10)
          .selectExpr("id as a", "id as b", "id as c", "id as d", "id as e")
          .write
          .mode("overwrite")
          .parquet(path.getAbsolutePath)

        ColumnarCollapseTransformStages.transformStageCounter.set(0)
        val df1 = spark.read.parquet(path.getAbsolutePath)
        val df2 = spark.read.parquet(path.getAbsolutePath)
        val ex1 = getNormalizedExplain(df1, FormattedMode)
        ColumnarCollapseTransformStages.transformStageCounter.set(0)
        val ex2 = getNormalizedExplain(df2, FormattedMode)
        assert(ex1 === ex2)
    }
  }
}

class GlutenExplainSuiteAE extends ExplainSuiteAE with GlutenSQLTestsBaseTrait {

  import testImplicits._

  // Rewritten with native operators
  testGluten("SPARK-35884: Explain Formatted") {
    val df1 = Seq((1, 2), (2, 3)).toDF("k", "v1")
    val df2 = Seq((2, 3), (1, 1)).toDF("k", "v2")
    val testDf = df1.join(df2, "k").groupBy("k").agg(count("v1"), sum("v1"), avg("v2"))
    // trigger the final plan for AQE
    testDf.collect()
    checkKeywordsExistsInExplain(
      testDf,
      FormattedMode,
      """
        |(12) BroadcastQueryStage
        |Output [2]: [k#x, v2#x]
        |Arguments: 0""".stripMargin,
      """
        |(21) ShuffleQueryStage
        |Output [5]: [k#x, count#xL, sum#xL, sum#x, count#xL]
        |Arguments: 1""".stripMargin,
      """
        |(22) AQEShuffleRead
        |Input [5]: [k#x, count#xL, sum#xL, sum#x, count#xL]
        |Arguments: coalesced
        |""".stripMargin,
      """
        |(15) BroadcastHashJoinExecTransformer
        |Left keys [1]: [k#x]
        |Right keys [1]: [k#x]
        |Join type: Inner
        |Join condition: None
        |""".stripMargin,
      """
        |(20) ColumnarExchange
        |Input [6]: [hash_partition_key#x, k#x, count#xL, sum#xL, sum#x, count#xL]
        |""".stripMargin,
      """
        |(37) AdaptiveSparkPlan
        |Output [4]: [k#x, count(v1)#xL, sum(v1)#xL, avg(v2)#x]
        |Arguments: isFinalPlan=true
        |""".stripMargin
    )
    checkKeywordsNotExistsInExplain(testDf, FormattedMode, "unknown")
  }

  // Rewritten with native operators
  testGluten("SPARK-35884: Explain formatted with subquery") {
    withTempView("t1", "t2") {
      spark
        .range(100)
        .select(($"id" % 10).as("key"), $"id".as("value"))
        .createOrReplaceTempView("t1")
      spark.range(10).createOrReplaceTempView("t2")
      val query =
        """
          |SELECT key, value FROM t1
          |JOIN t2 ON t1.key = t2.id
          |WHERE value > (SELECT MAX(id) FROM t2)
          |""".stripMargin
      val df = sql(query).toDF()
      df.collect()
      checkKeywordsExistsInExplain(
        df,
        FormattedMode,
        """
          |(5) FilterExecTransformer
          |Input [1]: [id#xL]
          |Arguments: ((id#xL > Subquery subquery#x, [id=#x]) AND isnotnull((id#xL % 10)))
          |""".stripMargin,
        """
          |(10) BroadcastQueryStage [codegen id : 2]
          |Output [1]: [id#xL]
          |Arguments: 0""".stripMargin,
        """
          |(23) AdaptiveSparkPlan
          |Output [2]: [key#xL, value#xL]
          |Arguments: isFinalPlan=true
          |""".stripMargin,
        """
          |Subquery:1 Hosting operator id = 5 Hosting Expression = Subquery subquery#x, [id=#x]
          |""".stripMargin,
        """
          |(31) ShuffleQueryStage
          |Output [1]: [max#xL]
          |Arguments: 0""".stripMargin,
        """
          |(40) AdaptiveSparkPlan
          |Output [1]: [max(id)#xL]
          |Arguments: isFinalPlan=true
          |""".stripMargin
      )
      checkKeywordsNotExistsInExplain(df, FormattedMode, "unknown")
    }
  }

  // Hosting operator id is different
  testGluten(
    "SPARK-38232: Explain formatted does not collect subqueries under query stage in AQE") {
    withTable("t") {
      sql("CREATE TABLE t USING PARQUET AS SELECT 1 AS c")
      val expected =
        "Subquery:1 Hosting operator id = 5 Hosting Expression = Subquery subquery#x, [id=#x]"
      val df = sql("SELECT count(s) FROM (SELECT (SELECT c FROM t) as s)")
      df.collect()
      withNormalizedExplain(df, FormattedMode)(output => assert(output.contains(expected)))
    }
  }

  // Stats are different
  testGluten("SPARK-38322: Support query stage show runtime statistics in formatted explain mode") {
    val df = Seq(1, 2).toDF("c").distinct()
    val statistics = "Statistics(sizeInBytes=128.0 B, rowCount=2)"

    checkKeywordsNotExistsInExplain(df, FormattedMode, statistics)

    df.collect()
    checkKeywordsExistsInExplain(df, FormattedMode, statistics)
  }

  // Plan contains VeloxColumnarToRowExec
  testGluten("SPARK-36795: Node IDs should not be duplicated when InMemoryRelation present") {
    withTempView("t1", "t2") {
      Seq(1).toDF("k").write.saveAsTable("t1")
      Seq(1).toDF("key").write.saveAsTable("t2")
      spark.sql("SELECT * FROM t1").persist()
      val query = "SELECT * FROM (SELECT * FROM t1) join t2 " +
        "ON k = t2.key"
      val df = sql(query).toDF()
      df.collect()
      val inMemoryRelationRegex = """InMemoryRelation \(([0-9]+)\)""".r
      val columnarToRowRegex = """VeloxColumnarToRowExec \(([0-9]+)\)""".r
      val explainString = getNormalizedExplain(df, FormattedMode)
      val inMemoryRelationNodeId = inMemoryRelationRegex.findAllIn(explainString).group(1)
      val columnarToRowNodeId = columnarToRowRegex.findAllIn(explainString).group(1)

      assert(inMemoryRelationNodeId != columnarToRowNodeId)
    }
  }
}

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

import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.utils.FallbackUtil

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, GlutenQueryTest, Row}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, ShuffleQueryStageExec}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{DoubleType, StructType}

import java.io.File

import scala.io.Source
import scala.reflect.ClassTag

abstract class WholeStageTransformerSuite extends GlutenQueryTest with SharedSparkSession {

  protected val backend: String
  protected val resourcePath: String
  protected val fileFormat: String
  protected val logLevel: String = "WARN"

  protected var TPCHTables: Map[String, DataFrame] = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    sparkContext.setLogLevel(logLevel)
  }

  override protected def afterAll(): Unit = {
    if (TPCHTables != null) {
      TPCHTables.keys.foreach(v => spark.sessionState.catalog.dropTempView(v))
    }
    super.afterAll()
  }

  protected def createTPCHNotNullTables(): Unit = {
    TPCHTables = Seq(
      "customer",
      "lineitem",
      "nation",
      "orders",
      "part",
      "partsupp",
      "region",
      "supplier").map {
      table =>
        val tableDir = getClass.getResource(resourcePath).getFile
        val tablePath = new File(tableDir, table).getAbsolutePath
        val tableDF = spark.read.format(fileFormat).load(tablePath)
        tableDF.createOrReplaceTempView(table)
        (table, tableDF)
    }.toMap
  }

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.plugins", "io.glutenproject.GlutenPlugin")
      .set("spark.default.parallelism", "1")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "1024MB")
  }

  protected def compareResultStr(
      sqlNum: String,
      result: Array[Row],
      queriesResults: String): Unit = {
    val resultStr = new StringBuffer()
    resultStr.append(result.length).append("\n")
    result.foreach(r => resultStr.append(r.mkString("|-|")).append("\n"))
    val queryResultStr =
      Source.fromFile(new File(queriesResults + "/" + sqlNum + ".out"), "UTF-8").mkString
    assert(queryResultStr.equals(resultStr.toString))
  }

  protected def compareDoubleResult(
      sqlNum: String,
      result: Array[Row],
      schema: StructType,
      queriesResults: String): Unit = {
    val queryResults =
      Source.fromFile(new File(queriesResults + "/" + sqlNum + ".out"), "UTF-8").getLines()
    var recordCnt = queryResults.next().toInt
    assert(result.size == recordCnt)
    for (row <- result) {
      assert(queryResults.hasNext)
      val result = queryResults.next().split("\\|-\\|")
      var i = 0
      row.schema.foreach(
        s => {
          s match {
            case d if d.dataType == DoubleType =>
              // handle null value
              if (row.isNullAt(i)) {
                assert(result(i).equals("null"))
              } else {
                val v1 = row.getDouble(i)
                assert(!result(i).equals("null"))
                val v2 = result(i).toDouble
                assert(Math.abs(v1 - v2) < 0.00001)
              }
            case _ =>
              // handle null value
              if (row.isNullAt(i)) {
                assert(result(i).equals("null"))
              } else {
                val v1 = row.get(i)
                assert(!result(i).equals("null"))
                val v2 = result(i)
                assert(v1.toString.equals(v2))
              }
          }
          i += 1
        })
    }
  }

  protected def runTPCHQuery(
      queryNum: Int,
      tpchQueries: String,
      queriesResults: String,
      compareResult: Boolean = true,
      noFallBack: Boolean = true)(customCheck: DataFrame => Unit): Unit = {
    val sqlNum = "q" + "%02d".format(queryNum)
    val sqlFile = tpchQueries + "/" + sqlNum + ".sql"
    val sqlStr = Source.fromFile(new File(sqlFile), "UTF-8").mkString
    val df = spark.sql(sqlStr)
    val result = df.collect()
    if (compareResult) {
      val schema = df.schema
      if (schema.exists(_.dataType == DoubleType)) {
        compareDoubleResult(sqlNum, result, schema, queriesResults)
      } else {
        compareResultStr(sqlNum, result, queriesResults)
      }
    } else {
      df.collect()
    }
    WholeStageTransformerSuite.checkFallBack(df, noFallBack)
    customCheck(df)
  }

  protected def runSql(sql: String, noFallBack: Boolean = true)(
      customCheck: DataFrame => Unit): Seq[Row] = {
    val df = spark.sql(sql)
    val result = df.collect()
    WholeStageTransformerSuite.checkFallBack(df, noFallBack)
    customCheck(df)
    result
  }

  def checkLengthAndPlan(df: DataFrame, len: Int = 100): Unit = {
    assert(df.collect().length == len)
    val executedPlan = getExecutedPlan(df)
    assert(executedPlan.exists(plan => plan.find(_.isInstanceOf[TransformSupport]).isDefined))
  }

  /**
   * Get all the children plan of plans.
   * @param plans:
   *   the input plans.
   * @return
   */
  def getChildrenPlan(plans: Seq[SparkPlan]): Seq[SparkPlan] = {
    if (plans.isEmpty) {
      return Seq()
    }

    val inputPlans: Seq[SparkPlan] = plans.map {
      case stage: ShuffleQueryStageExec => stage.plan
      case plan => plan
    }

    var newChildren: Seq[SparkPlan] = Seq()
    inputPlans.foreach {
      plan =>
        newChildren = newChildren ++ getChildrenPlan(plan.children)
        // To avoid duplication of WholeStageCodegenXXX and its children.
        if (!plan.nodeName.startsWith("WholeStageCodegen")) {
          newChildren = newChildren :+ plan
        }
    }
    newChildren
  }

  /**
   * Get the executed plan of a data frame.
   * @param df:
   *   dataframe.
   * @return
   *   A sequence of executed plans.
   */
  def getExecutedPlan(df: DataFrame): Seq[SparkPlan] = {
    df.queryExecution.executedPlan match {
      case exec: AdaptiveSparkPlanExec =>
        getChildrenPlan(Seq(exec.executedPlan))
      case plan =>
        getChildrenPlan(Seq(plan))
    }
  }

  /**
   * Check whether the executed plan of a dataframe contains the expected plan.
   * @param df:
   *   the input dataframe.
   * @param tag:
   *   class of the expected plan.
   * @tparam T:
   *   type of the expected plan.
   */
  def checkOperatorMatch[T <: TransformSupport](df: DataFrame)(implicit tag: ClassTag[T]): Unit = {
    val executedPlan = getExecutedPlan(df)
    assert(executedPlan.exists(plan => plan.getClass == tag.runtimeClass))
  }

  /**
   * run a query with native engine as well as vanilla spark then compare the result set for
   * correctness check
   */
  protected def compareResultsAgainstVanillaSpark(
      sqlStr: String,
      compareResult: Boolean = true,
      customCheck: DataFrame => Unit,
      noFallBack: Boolean = true,
      cache: Boolean = false): DataFrame = {
    var expected: Seq[Row] = null
    withSQLConf(vanillaSparkConfs(): _*) {
      val df = spark.sql(sqlStr)
      expected = df.collect()
    }
    val df = spark.sql(sqlStr)
    if (cache) {
      df.cache()
    }
    try {
      if (compareResult) {
        checkAnswer(df, expected)
      } else {
        df.collect()
      }
    } finally {
      if (cache) {
        df.unpersist()
      }
    }
    WholeStageTransformerSuite.checkFallBack(df, noFallBack)
    customCheck(df)
    df
  }

  protected def runQueryAndCompare(
      sqlStr: String,
      compareResult: Boolean = true,
      noFallBack: Boolean = true,
      cache: Boolean = false)(customCheck: DataFrame => Unit): DataFrame = {
    compareResultsAgainstVanillaSpark(sqlStr, compareResult, customCheck, noFallBack, cache)
  }

  /**
   * run a query with native engine as well as vanilla spark then compare the result set for
   * correctness check
   *
   * @param queryNum
   * @param tpchQueries
   * @param customCheck
   */
  protected def compareTPCHQueryAgainstVanillaSpark(
      queryNum: Int,
      tpchQueries: String,
      customCheck: DataFrame => Unit,
      noFallBack: Boolean = true): Unit = {
    val sqlNum = "q" + "%02d".format(queryNum)
    val sqlFile = tpchQueries + "/" + sqlNum + ".sql"
    val sqlStr = Source.fromFile(new File(sqlFile), "UTF-8").mkString
    compareResultsAgainstVanillaSpark(sqlStr, compareResult = true, customCheck, noFallBack)
  }

  protected def vanillaSparkConfs(): Seq[(String, String)] = {
    List(("spark.gluten.enabled", "false"))
  }
}

object WholeStageTransformerSuite extends Logging {

  /** Check whether the sql is fallback */
  def checkFallBack(
      df: DataFrame,
      noFallBack: Boolean = true,
      skipAssert: Boolean = false): Unit = {
    if (BackendsApiManager.isCHBackend) {
      // When noFallBack is true, it means there is no fallback plan,
      // otherwise there must be some fallback plans.
      val isFallBack = FallbackUtil.isFallback(df.queryExecution.executedPlan)
      if (!skipAssert) {
        assert(
          !isFallBack == noFallBack,
          s"FallBack $noFallBack check error: ${df.queryExecution.executedPlan}")
      } else {
        logWarning(s"FallBack $noFallBack check error: ${df.queryExecution.executedPlan}")
      }
    }
  }
}

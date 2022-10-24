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

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, GlutenQueryTest, Row}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{DoubleType, StructType}

import scala.io.Source

abstract class WholeStageTransformerSuite extends GlutenQueryTest with SharedSparkSession {

  protected val backend: String
  protected val resourcePath: String
  protected val fileFormat: String

  protected var TPCHTables: Map[String, DataFrame] = _

  override def beforeAll(): Unit = {
    super.beforeAll()
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
      "supplier").map { table =>
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
      row.schema.foreach(s => {
        s match {
          case d if d.dataType == DoubleType =>
            val v1 = row.getDouble(i)
            val v2 = result(i).toDouble
            assert(Math.abs(v1 - v2) < 0.00001)
          case _ =>
            val v1 = row.get(i).toString
            val v2 = result(i)
            assert(v1.equals(v2))
        }
        i += 1
      })
    }
  }

  protected def runTPCHQuery(
      queryNum: Int,
      tpchQueries: String,
      queriesResults: String,
      compareResult: Boolean = true)(customCheck: DataFrame => Unit): Unit = {
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
    }
    customCheck(df)
  }


  protected def runSql(sql: String)
                            (customCheck: DataFrame => Unit): Seq[Row] = {
    val df = spark.sql(sql)
    val result = df.collect()
    customCheck(df)
    result
  }

  /**
   * run a query with native engine as well as vanilla spark
   * then compare the result set for correctness check
   */
  protected def compareResultsAgainstVanillaSpark(
      sqlStr: String,
      compareResult: Boolean = true,
      customCheck: DataFrame => Unit): Seq[Row] = {
    var expected: Seq[Row] = null;
    withSQLConf(vanillaSparkConfs(): _*) {
      val df = spark.sql(sqlStr)
      df.show(false)
      expected = df.collect()
    }
    val df = spark.sql(sqlStr)
    df.show(false)
    if (compareResult) {
      checkAnswer(df, expected)
    }
    customCheck(df)
    df.collect()
  }

  protected def runQueryAndCompare(
      sqlStr: String,
      compareResult: Boolean = true)(customCheck: DataFrame => Unit): Seq[Row] = {
    compareResultsAgainstVanillaSpark(sqlStr, compareResult, customCheck)
  }

  /**
   * run a query with native engine as well as vanilla spark
   * then compare the result set for correctness check
   *
   * @param queryNum
   * @param tpchQueries
   * @param customCheck
   */
  protected def compareTPCHQueryAgainstVanillaSpark(
      queryNum: Int,
      tpchQueries: String,
      customCheck: DataFrame => Unit): Unit = {
    val sqlNum = "q" + "%02d".format(queryNum)
    val sqlFile = tpchQueries + "/" + sqlNum + ".sql"
    val sqlStr = Source.fromFile(new File(sqlFile), "UTF-8").mkString
    compareResultsAgainstVanillaSpark(sqlStr, compareResult = true, customCheck)
  }

  protected def vanillaSparkConfs(): Seq[(String, String)] = {
    List(("spark.gluten.sql.enable.native.engine", "false"))
  }
}


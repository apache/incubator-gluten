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

package io.glutenproject.e2e.tpc.h

import io.glutenproject.e2e.tpc.TpcRunner
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.{GlutenSparkSessionSwitcher, GlutenTestUtils, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkFunSuite}
import org.scalactic.source
import org.scalatest.Tag

import java.util.Objects
import scala.collection.JavaConverters._

abstract class TpchSuite extends SparkFunSuite {

  private var sessionSwitcher: GlutenSparkSessionSwitcher = null
  private var runner: TpcRunner = null

  private val typeModifiers: java.util.List[TypeModifier] = new java.util.ArrayList[TypeModifier]()

  protected def defineTypeModifier(typeModifier: TypeModifier): Unit = {
    typeModifiers.add(typeModifier)
  }

  def testConf(): SparkConf

  def queryResource(): String

  override def beforeAll(): Unit = {
    super.beforeAll()
    runner = new TpcRunner(queryResource(), TpchSuite.TPCH_WRITE_PATH)
    LogManager.getRootLogger.setLevel(Level.WARN)
    sessionSwitcher = new GlutenSparkSessionSwitcher()
    sessionSwitcher.registerSession("test", testConf())
    sessionSwitcher.registerSession("baseline", TpchSuite.baselineConf)
    sessionSwitcher.useSession("baseline") // use vanilla spark to generate data
    val dataGen = new TpchDataGen(sessionSwitcher.spark(), 0.1D, TpchSuite.TPCH_WRITE_PATH,
      typeModifiers.asScala.toArray)
    dataGen.gen()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    sessionSwitcher.stopActiveSession()
  }

  protected def testIfEnabled(testName: String, testTags: Tag*)
    (testFun: => Any /* Assertion */)(implicit pos: source.Position): Unit = {
    val str = System.getenv("GLUTEN_INTEGRATION_TEST_ENABLED")
    val enabled = !Objects.isNull(str) && str.toBoolean
    if (!enabled) {
      // scalastyle:off println
      println("!!! TEST SKIPPED !!!")
      // scalastyle:on println
      return
    }
    test(testName, testTags: _*)(testFun)(pos)
  }

  testIfEnabled("q1") {
    runTpchQuery("q1")
  }

  testIfEnabled("q2") {
    runTpchQuery("q2")
  }

  testIfEnabled("q3") {
    runTpchQuery("q3")
  }

  testIfEnabled("q4") {
    runTpchQuery("q4")
  }

  testIfEnabled("q5") {
    runTpchQuery("q5")
  }

  testIfEnabled("q6") {
    runTpchQuery("q6")
  }

  testIfEnabled("q7") {
    runTpchQuery("q7")
  }

  testIfEnabled("q8") {
    runTpchQuery("q8")
  }

  testIfEnabled("q9") {
    runTpchQuery("q9")
  }

  testIfEnabled("q10") {
    runTpchQuery("q10")
  }

  testIfEnabled("q11") {
    runTpchQuery("q11")
  }

  testIfEnabled("q12") {
    runTpchQuery("q12")
  }

  testIfEnabled("q13") {
    runTpchQuery("q13")
  }

  testIfEnabled("q14") {
    runTpchQuery("q14")
  }

  testIfEnabled("q15") {
    runTpchQuery("q15")
  }

  testIfEnabled("q16") {
    runTpchQuery("q16")
  }

  testIfEnabled("q17") {
    runTpchQuery("q17")
  }

  testIfEnabled("q18") {
    runTpchQuery("q18")
  }

  testIfEnabled("q19") {
    runTpchQuery("q19")
  }

  testIfEnabled("q20") {
    runTpchQuery("q20")
  }

  testIfEnabled("q21") {
    runTpchQuery("q21")
  }

  testIfEnabled("q22") {
    runTpchQuery("q22")
  }

  private def runTpchQuery(id: String): Unit = {
    // scalastyle:off println
    println(s"Running query: $id...")
    // scalastyle:on println
    sessionSwitcher.useSession("baseline")
    runner.createTables(sessionSwitcher.spark())
    val expected = runner.runTpcQuery(sessionSwitcher.spark(), id, explain = true)
    sessionSwitcher.useSession("test")
    runner.createTables(sessionSwitcher.spark())
    val result = runner.runTpcQuery(sessionSwitcher.spark(), id, explain = true)
    val error = GlutenTestUtils.compareAnswers(result, expected, sort = true)
    if (error.isEmpty) {
      // scalastyle:off println
      println(s"Successfully ran query $id, result check was passed. " +
        s"Returned row count: ${result.length}, expected: ${expected.length}")
      // scalastyle:on println
      return
    }
    // scalastyle:off println
    println(s"Query $id failed with error:")
    // scalastyle:on println
    fail(error.get)
  }

  private def runTextQuery(query: String): Unit = {
    sessionSwitcher.useSession("test")
    runner.createTables(sessionSwitcher.spark())
    val result = TpchSuite.runTextQuery(sessionSwitcher.spark(), query, explain = true)
    sessionSwitcher.useSession("baseline")
    runner.createTables(sessionSwitcher.spark())
    val expected = TpchSuite.runTextQuery(sessionSwitcher.spark(), query, explain = true)
    val error = GlutenTestUtils.compareAnswers(result, expected, sort = true)
    if (error.isEmpty) {
      return
    }
    fail(error.get)
  }
}

object TpchSuite {
  private val TPCH_WRITE_PATH = "/tmp/tpch-generated"

  private val baselineConf = VANILLA_CONF

  private def runTextQuery(spark: SparkSession, sql: String, explain: Boolean = false): Seq[Row] = {
    val df = spark.sql(sql)
    if (explain) {
      df.explain(extended = true)
    }
    df.collect()
  }
}

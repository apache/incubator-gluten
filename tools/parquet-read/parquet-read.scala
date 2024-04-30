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
import org.apache.gluten.exception.GlutenException
import org.apache.gluten.execution.BasicScanExecTransformer
import org.apache.spark.sql.{DataFrame, Row}

import org.apache.spark.internal.Logging
import org.apache.spark.rpc.GlutenDriverEndpoint
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.execution.SQLExecution
import scala.reflect.runtime.universe
import org.apache.spark.sql.internal.SQLConf

import java.io.File
import java.nio.file.{Files, Paths, StandardCopyOption}
import scala.reflect.classTag
import org.apache.spark.sql.execution.{SQLExecution, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, ShuffleQueryStageExec}

import java.util.TimeZone
import scala.collection.JavaConverters._
import org.apache.spark.sql.AnalysisException


//val fileName: String ="/path/to/file/"

val failureFolder: String = "/home/sparkuser/parquet-read-failures"

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

def getExecutedPlan(df: DataFrame): Seq[SparkPlan] = {
  df.queryExecution.executedPlan match {
    case exec: AdaptiveSparkPlanExec =>
      getChildrenPlan(Seq(exec.executedPlan))
    case plan =>
      getChildrenPlan(Seq(plan))
  }
}

def prepareRow(row: Row): Row = {
  Row.fromSeq(row.toSeq.map {
    case null => null
    case bd: java.math.BigDecimal => BigDecimal(bd)
    // Equality of WrappedArray differs for AnyVal and AnyRef in Scala 2.12.2+
    case seq: Seq[_] =>
      seq.map {
        case b: java.lang.Byte => b.byteValue
        case s: java.lang.Short => s.shortValue
        case i: java.lang.Integer => i.intValue
        case l: java.lang.Long => l.longValue
        case f: java.lang.Float => f.floatValue
        case d: java.lang.Double => d.doubleValue
        case x => x
      }
    // Convert array to Seq for easy equality check.
    case b: Array[_] => b.toSeq
    case r: Row => prepareRow(r)
    case o => o
  })
}

def prepareAnswer(answer: Seq[Row]): Seq[Row] = {
  // Converts data to types that we can do equality comparison using Scala collections.
  // For BigDecimal type, the Scala type has a better definition of equality test (similar to
  // Java's java.math.BigDecimal.compareTo).
  // For binary arrays, we convert it to Seq to avoid of calling java.util.Arrays.equals for
  // equality test.
  answer.map(prepareRow)
}

def genError(expected: Seq[Row], actual: Seq[Row]): String = {
  val getRowType: Option[Row] => String = row =>
    row
      .map(
        row =>
          if (row.schema == null) {
            "struct<>"
          } else {
            s"${row.schema.catalogString}"
          })
      .getOrElse("struct<>")

  s"""
     |== Results ==
     |${
    sideBySide(
      s"== Correct Answer - ${expected.size} ==" +:
        getRowType(expected.headOption) +:
        prepareAnswer(expected).map(_.toString()),
      s"== Gluten Answer - ${actual.size} ==" +:
        getRowType(actual.headOption) +:
        prepareAnswer(actual).map(_.toString())
    ).mkString("\n")
  }
""".stripMargin
}

def isNaNOrInf(num: Double): Boolean = {
  num.isNaN || num.isInfinite || num.isNegInfinity || num.isPosInfinity
}

def compare(obj1: Any, obj2: Any): Boolean = (obj1, obj2) match {
  case (null, null) => true
  case (null, _) => false
  case (_, null) => false
  case (a: Array[_], b: Array[_]) =>
    a.length == b.length && a.zip(b).forall { case (l, r) => compare(l, r) }
  case (a: Map[_, _], b: Map[_, _]) =>
    a.size == b.size && a.keys.forall {
      aKey => b.keys.find(bKey => compare(aKey, bKey)).exists(bKey => compare(a(aKey), b(bKey)))
    }
  case (a: Iterable[_], b: Iterable[_]) =>
    a.size == b.size && a.zip(b).forall { case (l, r) => compare(l, r) }
  case (a: Product, b: Product) =>
    compare(a.productIterator.toSeq, b.productIterator.toSeq)
  case (a: Row, b: Row) =>
    compare(a.toSeq, b.toSeq)
  // 0.0 == -0.0, turn float/double to bits before comparison, to distinguish 0.0 and -0.0.
  case (a: Double, b: Double) =>
    if (isNaNOrInf(a) || isNaNOrInf(b)) {
      java.lang.Double.doubleToRawLongBits(a) == java.lang.Double.doubleToRawLongBits(b)
    } else {
      Math.abs(a - b) < 0.00001
    }
  case (a: Float, b: Float) =>
    java.lang.Float.floatToRawIntBits(a) == java.lang.Float.floatToRawIntBits(b)
  case (a: BigDecimal, b: BigDecimal) =>
    val maxDeviation: BigDecimal = BigDecimal(1, math.max(a.scale, b.scale))
    (a - b).abs.compare(maxDeviation) <= 0
  case (a, b) => a == b
}

def sameRows(
              expectedAnswer: Seq[Row],
              sparkAnswer: Seq[Row],
              sortedColIdxes: Seq[Int],
              colLength: Int): Option[String] = {
  val expected = prepareAnswer(expectedAnswer)
  val actual = prepareAnswer(sparkAnswer)

  // if answer is fully sorted or there are unknown sorted cols, we can just compare the answer
  if (sortedColIdxes.contains(-1) || sortedColIdxes.length == colLength) {
    if (!compare(expected, actual)) {
      return Some(genError(expected, actual))
    }
    return None
  }

  // if answer is not fully sorted, we should sort the answer first, then compare them
  val sortedExpected = expected.sortBy(_.toString())
  val sortedActual = actual.sortBy(_.toString())
  if (!compare(sortedExpected, sortedActual)) {
    return Some(genError(sortedExpected, sortedActual))
  }

  // if answer is absolutely not sorted, the compare above is enough
  if (sortedColIdxes.isEmpty) {
    return None
  }

  // if answer is partially sorted, we should compare the sorted part
  val expectedPart = expected.map(row => sortedColIdxes.map(row.get))
  val actualPart = actual.map(row => sortedColIdxes.map(row.get))
  if (!compare(expectedPart, actualPart)) {
    return Some(genError(expected, actual))
  }
  None
}

def getOuterSortedColIdxes(df: DataFrame): Seq[Int] = {

  def findOuterSortColExprIds(plan: LogicalPlan): Seq[Long] = {
    plan match {
      case s: Sort =>
        s.order.map(_.child match {
          case a: Attribute => a.exprId.id
          case _ => -1
        })
      case _: Join => Nil
      case _: Subquery => Nil
      case _: SubqueryAlias => Nil
      case _ => plan.children.flatMap(child => findOuterSortColExprIds(child))
    }
  }

  val plan = df.queryExecution.optimizedPlan
  val projectColIds: Seq[Long] = plan.output.map(_.exprId.id)
  val sortedColIds: Seq[Long] = findOuterSortColExprIds(plan)

  sortedColIds.map(projectColIds.indexOf(_))
}

def getNextExecutionId: String = {
  val classMirror = universe.runtimeMirror(SQLExecution.getClass.getClassLoader)
  val staticMirror = classMirror.staticModule(SQLExecution.getClass.getName)
  val moduleMirror = classMirror.reflectModule(staticMirror)
  val objectMirror = classMirror.reflect(moduleMirror.instance)
  val strInObjSymbol = objectMirror.symbol.typeSignature
    .member(universe.TermName("nextExecutionId"))
    .asMethod
  val nextExecutionId = objectMirror.reflectMethod(strInObjSymbol)
  val newid = nextExecutionId.apply().toString
  newid
}
def getErrorMessageInCheckAnswer(
                                  df: DataFrame,
                                  expectedAnswer: Seq[Row],
                                  checkToRDD: Boolean = true): Option[String] = {
  if (checkToRDD) {
    val executionId = getNextExecutionId
    SQLExecution.withExecutionId(df.sparkSession, executionId) {
      df.rdd.count() // Also attempt to deserialize as an RDD [SPARK-15791]
    }
    GlutenDriverEndpoint.invalidateResourceRelation(executionId)
  }

  val sparkAnswer =
    try df.collect().toSeq
    catch {
      case e: Exception =>
        val errorMessage =
          s"""
             |Exception thrown while executing query:
             |${df.queryExecution}
             |== Exception ==
             |$e
             |${org.apache.spark.sql.catalyst.util.stackTraceToString(e)}
      """.stripMargin
        return Some(errorMessage)
    }

  val sortedColIdxes = getOuterSortedColIdxes(df)
  sameRows(expectedAnswer, sparkAnswer, sortedColIdxes, df.schema.length).map {
    results =>
      s"""
         |Results do not match for query:
         |Timezone: ${TimeZone.getDefault}
         |Timezone Env: ${sys.env.getOrElse("TZ", "")}
         |
         |${df.queryExecution}
         |== Results ==
         |$results
   """.stripMargin
  }
}

def checkAnswer(df: DataFrame, expectedAnswer: Seq[Row], checkToRDD: Boolean = true): Boolean = {
  getErrorMessageInCheckAnswer(df, expectedAnswer, checkToRDD) match {
    case Some(errorMessage) => false
    case None => true
  }
}

def withSQLConf(pairs: (String, String)*)(f: => Unit): Unit = {
  val conf = SQLConf.get
  val (keys, values) = pairs.unzip
  val currentValues = keys.map { key =>
    if (conf.contains(key)) {
      Some(conf.getConfString(key))
    } else {
      None
    }
  }
  (keys, values).zipped.foreach { (k, v) =>
    if (SQLConf.isStaticConfigKey(k)) {
      throw new GlutenException(s"Cannot modify the value of a static config: $k")
    }
    conf.setConfString(k, v)
  }
  try f finally {
    keys.zip(currentValues).foreach {
      case (key, Some(value)) => conf.setConfString(key, value)
      case (key, None) => conf.unsetConf(key)
    }
  }
}

def runQueryAndCompareWithoutFailure(sqlStr: String): (DataFrame, Boolean) = {
  var expected: Seq[Row] = null
  withSQLConf("spark.gluten.enabled"->"false") {
    val df = spark.sql(sqlStr)
    expected = df.collect()
  }
  spark.conf.set("spark.gluten.sql.complexType.scan.fallback.enabled", "false");
  val df = spark.sql(sqlStr)
  (df, checkAnswer(df, expected))
}

var errorCode:Int = 0
val file = new File(fileName)
// scalastyle: off
println("Testing file: "+ file)
// scalastyle: on
val df = spark.read.parquet(file.getAbsolutePath)
df.createOrReplaceTempView("test_table")
try {
  val (res, isResultMatch) = runQueryAndCompareWithoutFailure("select * from test_table")
  if (isResultMatch) {
    val executedPlan = getExecutedPlan(res)
    if (!executedPlan.exists(plan => classTag[BasicScanExecTransformer].runtimeClass.isInstance(plan))) {
      // scalastyle: off
      println("Read fuzzer test: fallbacked.")
      // scalastyle: on
      errorCode = 1
      }
      } else {
        // scalastyle: off
        println("Read fuzzer test: result mismatch")
        // scalastyle: on
        errorCode = 2
      }
    } catch {
      case e: Throwable =>
        // scalastyle: off
        println("Read fuzzer test: runtime exception")
        println(e)
        // scalastyle: on
        errorCode = 3
    }
    
System.exit(errorCode)


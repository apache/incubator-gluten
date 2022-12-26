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

import java.io.File
import scala.collection.mutable.ArrayBuffer
import io.glutenproject.GlutenConfig
import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.execution.ProjectExecTransformer
import io.glutenproject.test.TestStats
import io.glutenproject.utils.SystemParameters
import org.apache.commons.io.FileUtils
import org.scalactic.source.Position
import org.scalatest.{Args, Status, Tag}
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.analysis.ResolveTimeZone
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.catalyst.optimizer.{ConstantFolding, ConvertToLocalRelation, NullPropagation}
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

trait GlutenTestsTrait extends GlutenTestsCommonTrait {

  protected override def beforeAll(): Unit = {
    // prepare working paths
    val basePathDir = new File(basePath)
    if (basePathDir.exists()) {
      FileUtils.forceDelete(basePathDir)
    }
    FileUtils.forceMkdir(basePathDir)
    FileUtils.forceMkdir(new File(warehouse))
    FileUtils.forceMkdir(new File(metaStorePathAbsolute))
    super.beforeAll()
    initializeSession()
    _spark.sparkContext.setLogLevel("WARN")
  }

  protected override def afterAll(): Unit = {
    try {
      super.afterAll()
    } finally {
      try {
        if (_spark != null) {
          try {
            _spark.sessionState.catalog.reset()
          } finally {
            _spark.stop()
            _spark = null
          }
        }
      } finally {
        SparkSession.clearActiveSession()
        SparkSession.clearDefaultSession()
      }
    }
    print("Test suite: " + this.getClass.getSimpleName +
      "; Suite test number: " + TestStats.suiteTestNumber +
      "; OffloadGluten number: " + TestStats.offloadGlutenTestNumber + "\n")
    TestStats.reset()
  }

  protected def initializeSession(): Unit = {
    if (_spark == null) {
      val sparkBuilder = SparkSession
        .builder()
        .appName("Gluten-UT")
        .master(s"local[2]")
        .config(SQLConf.OPTIMIZER_EXCLUDED_RULES.key, ConvertToLocalRelation.ruleName)
        .config("spark.driver.memory", "1G")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.files.maxPartitionBytes", "134217728")
        .config("spark.memory.offHeap.enabled", "true")
        .config("spark.memory.offHeap.size", "1024MB")
        .config("spark.plugins", "io.glutenproject.GlutenPlugin")
        .config("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
        .config("spark.sql.warehouse.dir", warehouse)
        // Avoid static evaluation for literal input by spark catalyst.
        .config("spark.sql.optimizer.excludedRules", ConstantFolding.ruleName + ","  +
            NullPropagation.ruleName)
        // Avoid the code size overflow error in Spark code generation.
        .config("spark.sql.codegen.wholeStage", "false")

      _spark = if (BackendsApiManager.getBackendName.equalsIgnoreCase(
        GlutenConfig.GLUTEN_CLICKHOUSE_BACKEND)) {
        sparkBuilder
          .config("spark.io.compression.codec", "LZ4")
          .config("spark.gluten.sql.columnar.backend.ch.worker.id", "1")
          .config("spark.gluten.sql.columnar.backend.ch.use.v2", "false")
          .config("spark.gluten.sql.enable.native.validation", "false")
          .config("spark.sql.files.openCostInBytes", "134217728")
          .config(GlutenConfig.GLUTEN_LIB_PATH, SystemParameters.getClickHouseLibPath)
          .config("spark.unsafe.exceptionOnMemoryLeak", "true")
          .getOrCreate()
      } else {
        sparkBuilder
          .config("spark.unsafe.exceptionOnMemoryLeak", "false")
          .getOrCreate()
      }
    }
  }

  protected var _spark: SparkSession = null

  override protected def checkEvaluation(expression: => Expression,
                                         expected: Any,
                                         inputRow: InternalRow = EmptyRow): Unit = {
    val resolver = ResolveTimeZone
    val expr = resolver.resolveTimeZones(expression)
    assert(expr.resolved)

    val catalystValue = CatalystTypeConverters.convertToCatalyst(expected)

    if(canConvertToDataFrame(inputRow)) {
      glutenCheckExpression(expr, catalystValue, inputRow)
    } else {
      checkEvaluationWithoutCodegen(expr, catalystValue, inputRow)
      checkEvaluationWithMutableProjection(expr, catalystValue, inputRow)
      if (GenerateUnsafeProjection.canSupport(expr.dataType)) {
        checkEvaluationWithUnsafeProjection(expr, catalystValue, inputRow)
      }
      checkEvaluationWithOptimization(expr, catalystValue, inputRow)
    }
  }

  def checkDataTypeSupported(expr: Expression): Boolean = {
    GlutenTestConstants.SUPPORTED_DATA_TYPE.acceptsType(expr.dataType)
  }

  def glutenCheckExpression(expression: Expression,
                            expected: Any,
                            inputRow: InternalRow, justEvalExpr: Boolean = false): Unit = {
    val df = if (inputRow != EmptyRow && inputRow != InternalRow.empty) {
      convertInternalRowToDataFrame(inputRow)
    } else {
      val schema = StructType(
        StructField("a", IntegerType, nullable = true) :: Nil)
      val empData = Seq(Row(1))
      _spark.createDataFrame(_spark.sparkContext.parallelize(empData), schema)
    }
    val resultDF = df.select(Column(expression))
    val result = if (justEvalExpr) {
      try {
        expression.eval(inputRow)
      } catch {
        case e: Exception => fail(s"Exception evaluating $expression", e)
      }
    } else {
      resultDF.collect()
    }
    if (checkDataTypeSupported(expression) &&
        expression.children.forall(checkDataTypeSupported)) {
      val projectTransformer = resultDF.queryExecution.executedPlan.collect {
        case p: ProjectExecTransformer => p
      }
      if (projectTransformer.size == 1) {
        print("Offload to native backend in the test.\n")
      } else {
        print("Not supported in native backend, fall back to vanilla spark in the test.\n")
      }
    } else {
      print("Has unsupported data type, fall back to vanilla spark.\n")
    }
    checkResult(result, expected, expression)
  }

  def canConvertToDataFrame(inputRow: InternalRow): Boolean = {
    if (inputRow == EmptyRow || inputRow == InternalRow.empty) {
      return true
    }
    if (!inputRow.isInstanceOf[GenericInternalRow]) {
      return false
    }
    val values = inputRow.asInstanceOf[GenericInternalRow].values
    for (value <- values) {
      value match {
        case _: MapData => return false
        case _: ArrayData => return false
        case _: InternalRow => return false
        case _ =>
      }
    }
    true
  }


  def convertInternalRowToDataFrame(inputRow: InternalRow): DataFrame = {
    val structFileSeq = new ArrayBuffer[StructField]()
    val values = inputRow match {
      case genericInternalRow: GenericInternalRow =>
        genericInternalRow.values
      case _ => throw new UnsupportedOperationException("Unsupported InternalRow.")
    }
    val inputValues = values.map {
      case boolean: java.lang.Boolean =>
        structFileSeq.append(StructField("bool", BooleanType, boolean == null))
      case short: java.lang.Short =>
        structFileSeq.append(StructField("i16", ShortType, short == null))
      case byte: java.lang.Byte =>
        structFileSeq.append(StructField("i8", ByteType, byte == null))
      case integer: java.lang.Integer =>
        structFileSeq.append(StructField("i32", IntegerType, integer == null))
      case long: java.lang.Long =>
        structFileSeq.append(StructField("i64", LongType, long == null))
      case float: java.lang.Float =>
        structFileSeq.append(StructField("fp32", FloatType, float == null))
      case double: java.lang.Double =>
        structFileSeq.append(StructField("fp64", DoubleType, double == null))
      case utf8String: UTF8String =>
        structFileSeq.append(StructField("str", StringType, utf8String == null))
      case byteArr: Array[Byte] =>
        structFileSeq.append(StructField("vbin", BinaryType, byteArr == null))
      case decimal: Decimal =>
        structFileSeq.append(StructField("dec",
          DecimalType(decimal.precision, decimal.scale), decimal == null))
      case _ =>
        // for null
        structFileSeq.append(StructField("n", IntegerType, nullable = true))
    }
    _spark.internalCreateDataFrame(_spark.sparkContext.parallelize(Seq(inputRow)),
      StructType(structFileSeq))
  }
}

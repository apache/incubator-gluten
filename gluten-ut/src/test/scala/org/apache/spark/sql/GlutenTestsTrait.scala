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

import org.apache.commons.io.FileUtils
import org.scalactic.source.Position
import org.scalatest.Tag

import io.glutenproject.GlutenConfig
import io.glutenproject.execution.ProjectExecTransformer
import io.glutenproject.utils.SystemParameters

import org.apache.spark.SparkFunSuite
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.analysis.ResolveTimeZone
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

trait GlutenTestsTrait extends SparkFunSuite with ExpressionEvalHelper with GlutenTestsBaseTrait {

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
        .config("spark.memory.offHeap.size", "2g")
        .config("spark.plugins", "io.glutenproject.GlutenPlugin")
        .config("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
        .config(GlutenConfig.GLUTEN_LOAD_NATIVE, "true")
        .config("spark.gluten.sql.columnar.backend.lib", SystemParameters.getGlutenBackend())
        .config("spark.sql.warehouse.dir", warehouse)

      _spark = if (SystemParameters.getGlutenBackend().equalsIgnoreCase(
        GlutenConfig.GLUTEN_CLICKHOUSE_BACKEND)) {
        sparkBuilder
          .config(GlutenConfig.GLUTEN_LOAD_ARROW, "false")
          .config("spark.io.compression.codec", "LZ4")
          .config("spark.gluten.sql.columnar.backend.ch.worker.id", "1")
          .config("spark.gluten.sql.columnar.backend.ch.use.v2", "false")
          .config("spark.gluten.sql.enable.native.validation", "false")
          .config("spark.sql.files.openCostInBytes", "134217728")
          .config(GlutenConfig.GLUTEN_LIB_PATH, SystemParameters.getClickHouseLibPath())
          .getOrCreate()
      } else {
        sparkBuilder
          .config("spark.unsafe.exceptionOnMemoryLeak", "false")
          .getOrCreate()
      }
    }
  }

  protected var _spark: SparkSession = null

  override protected def test(testName: String,
                              testTags: Tag*)(testFun: => Any)(implicit pos: Position): Unit = {
    if (whiteBlackCheck(testName)) {
      super.test(testName, testTags: _*)(testFun)
    }
  }

  override protected def checkEvaluation(expression: => Expression,
                                         expected: Any,
                                         inputRow: InternalRow = EmptyRow): Unit = {
    val serializer = new JavaSerializer(_spark.sparkContext.getConf).newInstance
    val resolver = ResolveTimeZone
    val expr = resolver.resolveTimeZones(expression)
    assert(expr.resolved)

    val catalystValue = CatalystTypeConverters.convertToCatalyst(expected)
    glutenCheckExpression(expr, catalystValue, inputRow)
  }

  def glutenCheckExpression(expression: Expression,
                            expected: Any,
                            inputRow: InternalRow): Unit = {
    val df = if (inputRow != EmptyRow) {
      convertInternalRowToDataFrame(inputRow)
    } else {
      val schema = StructType(
        StructField("a", IntegerType, true) :: Nil)
      val empData = Seq(Row(1))
      _spark.createDataFrame(_spark.sparkContext.parallelize(empData), schema)
    }
    val resultDF = df.select(Column(expression))
    val result = resultDF.collect()
    resultDF.explain(false)
    if (GlutenTestConstants.SUPPORTED_DATA_TYPE.contains(expression.dataType)) {
      val projectTransformer = resultDF.queryExecution.executedPlan.collect {
        case p: ProjectExecTransformer => p
      }
      assert(projectTransformer.size == 1)
    }
    checkResult(result, expected, expression)
  }

  def convertInternalRowToDataFrame(inputRow: InternalRow): DataFrame = {
    val structFileSeq = new ArrayBuffer[StructField]()
    val values = inputRow match {
      case genericInternalRow: GenericInternalRow =>
        genericInternalRow.values
      case _ => throw new UnsupportedOperationException("Unsupported InternalRow.")
    }
    val inputValues = values.map {
      _ match {
        case utf8String: UTF8String =>
          structFileSeq.append(StructField("s", StringType, utf8String == null))
        case byteArr: Array[Byte] =>
          structFileSeq.append(StructField("a", BinaryType, byteArr == null))
        case integer: java.lang.Integer =>
          structFileSeq.append(StructField("i", IntegerType, integer == null))
        case long: java.lang.Long =>
          structFileSeq.append(StructField("l", LongType, long == null))
        case double: java.lang.Double =>
          structFileSeq.append(StructField("d", DoubleType, double == null))
        case short: java.lang.Short =>
          structFileSeq.append(StructField("sh", ShortType, short == null))
        case byte: java.lang.Byte =>
          structFileSeq.append(StructField("b", ByteType, byte == null))
        case boolean: java.lang.Boolean =>
          structFileSeq.append(StructField("t", BooleanType, boolean == null))
        case _ =>
          // for null
          structFileSeq.append(StructField("n", IntegerType, true))
      }
    }
    _spark.internalCreateDataFrame(_spark.sparkContext.parallelize(Seq(inputRow)),
      StructType(structFileSeq))
  }
}

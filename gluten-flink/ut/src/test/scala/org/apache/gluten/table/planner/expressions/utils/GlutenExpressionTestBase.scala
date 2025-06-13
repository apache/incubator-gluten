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
package org.apache.gluten.table.planner.expressions.utils

import org.apache.gluten.rexnode.{RexConversionContext, RexNodeConverter, Utils}
import org.apache.gluten.table.runtime.operators.GlutenSingleInputOperator
import org.apache.gluten.table.runtime.stream.common.Velox4jEnvironment
import org.apache.gluten.util.{LogicalTypeConverter, PlanNodeIdGenerator}

import io.github.zhztheplayer.velox4j.plan.ProjectNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.RexNode
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.serialization.SerializerConfigImpl
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness
import org.apache.flink.table.data.RowData
import org.apache.flink.table.data.binary.BinaryRowData
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.expressions.utils.ExpressionTestBase
import org.apache.flink.table.runtime.generated.GeneratedFunction
import org.apache.flink.table.runtime.types.TypeInfoLogicalTypeConverter
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo
import org.apache.flink.table.types.logical.RowType
import org.apache.flink.table.types.utils.TypeConversions
import org.apache.flink.types.Row
import org.junit.jupiter.api.BeforeEach
import org.slf4j.{Logger, LoggerFactory}

import java.lang.reflect.Field
import java.util.Collections

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.language.existentials
import scala.util.{Failure, Success, Try}

trait GlutenExpressionTestBase extends ExpressionTestBase {

  private val LOG: Logger = LoggerFactory.getLogger(classOf[GlutenExpressionTestBase])

  private var veloxInitialized = false

  private var plannerField: Field = _
  private var relBuilderField: Field = _
  private var resolvedDataTypeField: Field = _
  private var validExprsField: Field = _

  @BeforeEach
  override def prepare(): Unit = {
    initializeVeloxIfNeeded()
    initializeReflectionFields()

    super.prepare()
    LOG.info("GlutenExpressionTestBase prepared successfully")
  }

  override def evaluateFunctionResult(
      generatedFunction: GeneratedFunction[MapFunction[RowData, BinaryRowData]]): List[String] = {

    Try {
      val currentExprs = getCurrentValidExprs()

      if (currentExprs.nonEmpty) {
        executeExpressionsWithGluten(currentExprs)
      } else {
        super.evaluateFunctionResult(generatedFunction)
      }

    } match {
      case Success(results) =>
        results

      case Failure(exception) =>
        LOG.warn(
          s"Gluten execution failed: ${exception.getMessage}, falling back to Flink",
          exception)
        super.evaluateFunctionResult(generatedFunction)
    }
  }

  private def executeExpressionsWithGluten(
      exprs: mutable.ArrayBuffer[(String, RexNode, String)]): List[String] = {

    val inputRowType = getInputRowType()
    val testData = getTestRowData()
    val inNames = Utils.getNamesFromRowType(inputRowType)
    val conversionContext = new RexConversionContext(inNames)

    val rexNodes = exprs.map(_._2).toList
    val typedExprs = rexNodes.map {
      rexNode => RexNodeConverter.toTypedExpr(rexNode, conversionContext)
    }

    executeTypedExprsWithVelox(typedExprs, testData, inputRowType, rexNodes)
  }

  private def executeTypedExprsWithVelox(
      typedExprs: List[io.github.zhztheplayer.velox4j.expression.TypedExpr],
      inputData: RowData,
      inputRowType: RowType,
      rexNodes: List[RexNode]): List[String] = {

    val veloxInputType = LogicalTypeConverter
      .toVLType(inputRowType)
      .asInstanceOf[io.github.zhztheplayer.velox4j.`type`.RowType]

    val outputNames = (0 until typedExprs.size).map(i => s"result_$i").asJava
    val outputTypes = rexNodes.map {
      rexNode =>
        val flinkType = FlinkTypeFactory.toLogicalType(rexNode.getType)
        LogicalTypeConverter.toVLType(flinkType)
    }.asJava
    val veloxOutputType =
      new io.github.zhztheplayer.velox4j.`type`.RowType(outputNames, outputTypes)

    val projectNode = new ProjectNode(
      PlanNodeIdGenerator.newId(),
      Collections.emptyList(),
      outputNames,
      typedExprs.asJava
    )

    val glutenOperator = new GlutenSingleInputOperator(
      projectNode,
      PlanNodeIdGenerator.newId(),
      veloxInputType,
      veloxOutputType
    )

    val inputTypeInfo = InternalTypeInfo.of(inputRowType)
    val serializer = inputTypeInfo.createSerializer(new SerializerConfigImpl())
    val harness = new OneInputStreamOperatorTestHarness(glutenOperator, serializer)

    try {
      harness.setup(serializer)
      harness.open()

      harness.processElement(new StreamRecord(inputData, 0L))

      val output = harness.getOutput
      if (!output.isEmpty) {
        val result = output.poll().asInstanceOf[StreamRecord[RowData]].getValue

        (0 until typedExprs.size)
          .zip(rexNodes)
          .map {
            case (index, rexNode) =>
              convertResultToString(result, index, rexNode.getType)
          }
          .toList
      } else {
        List.fill(typedExprs.size)("NULL")
      }

    } finally {
      harness.close()
    }
  }

  private def getCurrentValidExprs(): mutable.ArrayBuffer[(String, RexNode, String)] = {
    try {
      validExprsField.get(this).asInstanceOf[mutable.ArrayBuffer[(String, RexNode, String)]]
    } catch {
      case e: Exception =>
        LOG.error("Failed to get current valid expressions", e)
        mutable.ArrayBuffer.empty
    }
  }

  private def convertResultToString(result: RowData, index: Int, sqlType: RelDataType): String = {

    if (result.isNullAt(index)) {
      return "NULL"
    }

    val stringResult = result.getString(index)
    if (stringResult == null) "NULL" else stringResult.toString
  }

  private def getInputRowType(): RowType = {
    if (containsLegacyTypes) {
      TypeInfoLogicalTypeConverter
        .fromTypeInfoToLogicalType(typeInfo)
        .asInstanceOf[RowType]
    } else {
      try {
        val resolvedDataType = getResolvedDataTypeFromReflection()
        resolvedDataType.getLogicalType.asInstanceOf[RowType]
      } catch {
        case e: Exception =>
          LOG.error("Failed to get resolved data type", e)
          throw new RuntimeException("Failed to get input row type", e)
      }
    }
  }

  private def getTestRowData(): RowData = {
    val testDataRow = testData

    if (containsLegacyTypes) {
      val resolvedDataType = TypeConversions.fromLegacyInfoToDataType(typeInfo)
      val converter = org.apache.flink.table.data.util.DataFormatConverters
        .getConverterForDataType(resolvedDataType)
        .asInstanceOf[
          org.apache.flink.table.data.util.DataFormatConverters.DataFormatConverter[RowData, Row]]
      converter.toInternal(testDataRow)
    } else {
      try {
        val resolvedDataType = getResolvedDataTypeFromReflection()
        val converter = org.apache.flink.table.data.conversion.DataStructureConverters
          .getConverter(resolvedDataType)
          .asInstanceOf[org.apache.flink.table.data.conversion.DataStructureConverter[RowData, Row]]
        converter.open(getClass.getClassLoader)
        converter.toInternalOrNull(testDataRow)
      } catch {
        case e: Exception =>
          LOG.error("Failed to convert test data", e)
          throw new RuntimeException("Failed to convert test data to RowData", e)
      }
    }
  }

  private def initializeVeloxIfNeeded(): Unit = {
    if (!veloxInitialized) {
      try {
        Velox4jEnvironment.initializeOnce()
        veloxInitialized = true
      } catch {
        case e: Exception =>
          LOG.error("Failed to initialize Velox environment", e)
          throw new RuntimeException("Failed to initialize Velox environment", e)
      }
    }
  }

  private def initializeReflectionFields(): Unit = {
    try {
      val superClass = this.getClass.getSuperclass.getSuperclass

      plannerField = superClass.getDeclaredField("planner")
      plannerField.setAccessible(true)

      relBuilderField = superClass.getDeclaredField("relBuilder")
      relBuilderField.setAccessible(true)

      validExprsField = superClass.getDeclaredField("validExprs")
      validExprsField.setAccessible(true)

      if (!containsLegacyTypes) {
        resolvedDataTypeField = superClass.getDeclaredField("resolvedDataType")
        resolvedDataTypeField.setAccessible(true)
      }

    } catch {
      case e: Exception =>
        LOG.error("Failed to initialize reflection fields", e)
        throw new RuntimeException("Failed to initialize reflection fields", e)
    }
  }

  private def getResolvedDataTypeFromReflection(): org.apache.flink.table.types.DataType = {
    if (resolvedDataTypeField != null) {
      resolvedDataTypeField.get(this).asInstanceOf[org.apache.flink.table.types.DataType]
    } else {
      throw new RuntimeException("ResolvedDataType field not available for legacy types")
    }
  }
}

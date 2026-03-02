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
package org.apache.gluten.expression.aggregate

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{ApproximatePercentile, DeclarativeAggregate}
import org.apache.spark.sql.catalyst.trees.TernaryLike
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._

/**
 * Velox-compatible DeclarativeAggregate for approx_percentile.
 *
 * Unlike Spark's ApproximatePercentile (which uses QuantileSummaries/GK algorithm with BinaryType
 * intermediate data), this implementation uses KLL sketch with a 9-field StructType intermediate
 * that is fully compatible with Velox's approx_percentile accumulator layout:
 *
 * 0: percentiles - Array(Double) 1: percentilesIsArray - Boolean 2: accuracy - Double 3: k -
 * Integer (KLL parameter) 4: n - Long (total count) 5: minValue - childType 6: maxValue - childType
 * 7: items - Array(childType) 8: levels - Array(Integer)
 *
 * Because aggBufferAttributes has 9 fields (> 1), the existing VeloxIntermediateData.Type default
 * branch (aggBufferAttributes.size > 1) will match automatically, meaning:
 *   - No special handling needed in HashAggregateExecTransformer
 *   - extractStruct / rowConstruct projections work out of the box
 *   - Partial fallback (Velox partial -> Spark final) is supported
 *
 * This follows the same pattern as VeloxCollectList/VeloxCollectSet.
 */
case class VeloxApproximatePercentile(
    child: Expression,
    percentageExpression: Expression,
    accuracyExpression: Expression)
  extends DeclarativeAggregate
  with TernaryLike[Expression] {

  override def first: Expression = child
  override def second: Expression = percentageExpression
  override def third: Expression = accuracyExpression

  override def prettyName: String = "velox_approx_percentile"

  // Mark as lazy so that expressions are not evaluated during tree transformation.
  private lazy val accuracy: Double = accuracyExpression.eval() match {
    case null => ApproximatePercentile.DEFAULT_PERCENTILE_ACCURACY.toDouble
    case num: Number => num.doubleValue()
  }

  private lazy val (returnPercentileArray, percentages): (Boolean, Array[Double]) =
    percentageExpression.eval() match {
      case null => (false, null)
      case num: Double => (false, Array(num))
      case arrayData: ArrayData => (true, arrayData.toDoubleArray())
    }

  override def checkInputDataTypes(): TypeCheckResult = {
    // Delegate to Spark's ApproximatePercentile for validation
    ApproximatePercentile(child, percentageExpression, accuracyExpression)
      .checkInputDataTypes()
  }

  override def nullable: Boolean = true

  override def dataType: DataType = {
    if (returnPercentileArray) ArrayType(child.dataType, containsNull = false)
    else child.dataType
  }

  // --- The 9 aggBuffer attributes matching Velox KLL sketch intermediate type ---

  private lazy val percentilesBuf: AttributeReference =
    AttributeReference("percentiles", ArrayType(DoubleType))()
  private lazy val percentilesIsArrayBuf: AttributeReference =
    AttributeReference("percentilesIsArray", BooleanType)()
  private lazy val accuracyBuf: AttributeReference =
    AttributeReference("accuracy", DoubleType)()
  private lazy val kBuf: AttributeReference =
    AttributeReference("k", IntegerType)()
  private lazy val nBuf: AttributeReference =
    AttributeReference("n", LongType)()
  private lazy val minValueBuf: AttributeReference =
    AttributeReference("minValue", child.dataType)()
  private lazy val maxValueBuf: AttributeReference =
    AttributeReference("maxValue", child.dataType)()
  private lazy val itemsBuf: AttributeReference =
    AttributeReference("items", ArrayType(child.dataType))()
  private lazy val levelsBuf: AttributeReference =
    AttributeReference("levels", ArrayType(IntegerType))()

  override def aggBufferAttributes: Seq[AttributeReference] = Seq(
    percentilesBuf,
    percentilesIsArrayBuf,
    accuracyBuf,
    kBuf,
    nBuf,
    minValueBuf,
    maxValueBuf,
    itemsBuf,
    levelsBuf
  )

  // --- Initial values: create an empty KLL sketch ---

  private lazy val percentilesLiteral: Literal = {
    if (percentages == null) Literal.create(null, ArrayType(DoubleType))
    else
      Literal.create(
        new org.apache.spark.sql.catalyst.util.GenericArrayData(
          percentages.map(_.asInstanceOf[Any])),
        ArrayType(DoubleType))
  }

  override lazy val initialValues: Seq[Expression] = Seq(
    percentilesLiteral, // percentiles
    Literal.create(returnPercentileArray, BooleanType), // percentilesIsArray
    Literal.create(accuracy, DoubleType), // accuracy
    Literal.create(KllSketchFieldIndex.DEFAULT_K, IntegerType), // k
    Literal.create(0L, LongType), // n
    Literal.create(null, child.dataType), // minValue
    Literal.create(null, child.dataType), // maxValue
    Literal.create(
      new org.apache.spark.sql.catalyst.util.GenericArrayData(Array.empty[Any]),
      ArrayType(child.dataType)
    ), // items
    Literal.create(
      new org.apache.spark.sql.catalyst.util.GenericArrayData(Array(0, 0)),
      ArrayType(IntegerType)
    ) // levels
  )

  // --- Update expressions: add a value to the sketch ---

  override lazy val updateExpressions: Seq[Expression] = {
    // When input is null, keep buffer unchanged; otherwise call KllSketchAdd
    val structExpr = CreateStruct(aggBufferAttributes)
    val updated = If(
      IsNull(child),
      structExpr,
      KllSketchAdd(structExpr, child, child.dataType)
    )
    // Extract fields from the updated struct back to individual buffer attributes
    aggBufferAttributes.zipWithIndex.map {
      case (attr, idx) =>
        GetStructField(updated, idx, Some(attr.name))
    }
  }

  // --- Merge expressions: merge two sketches ---

  override lazy val mergeExpressions: Seq[Expression] = {
    val leftStruct = CreateStruct(aggBufferAttributes.map(_.left))
    val rightStruct = CreateStruct(aggBufferAttributes.map(_.right))
    val merged = KllSketchMerge(leftStruct, rightStruct, child.dataType)
    aggBufferAttributes.zipWithIndex.map {
      case (attr, idx) =>
        GetStructField(merged, idx, Some(attr.name))
    }
  }

  // --- Evaluate expression: extract percentiles from the sketch ---

  override lazy val evaluateExpression: Expression = {
    val structExpr = CreateStruct(aggBufferAttributes)
    KllSketchEval(structExpr, returnPercentileArray, dataType, child.dataType)
  }

  override def defaultResult: Option[Literal] = Option(Literal.create(null, dataType))

  override protected def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression): VeloxApproximatePercentile =
    copy(child = newFirst, percentageExpression = newSecond, accuracyExpression = newThird)
}

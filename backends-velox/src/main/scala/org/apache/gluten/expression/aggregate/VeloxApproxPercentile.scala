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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.ApproximatePercentile
import org.apache.spark.sql.catalyst.expressions.aggregate.DeclarativeAggregate
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.trees.TernaryLike
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.types._

import java.util

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
    new ApproximatePercentile(child, percentageExpression, accuracyExpression)
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
    if (percentages == null) {
      Literal.create(null, ArrayType(DoubleType))
    } else {
      Literal.create(
        new org.apache.spark.sql.catalyst.util.GenericArrayData(
          percentages.map(_.asInstanceOf[Any])),
        ArrayType(DoubleType))
    }
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

/**
 * KLL sketch field indices matching Velox's ApproxPercentileIntermediateTypeChildIndex.
 *
 * The intermediate StructType has 9 fields: 0: percentiles - Array(Double) 1: percentilesIsArray -
 * Boolean 2: accuracy - Double 3: k - Integer (KLL parameter) 4: n - Long (total count) 5: minValue
 * \- childType 6: maxValue - childType 7: items - Array(childType) 8: levels - Array(Integer)
 */
object KllSketchFieldIndex {
  val PERCENTILES = 0
  val PERCENTILES_IS_ARRAY = 1
  val ACCURACY = 2
  val K = 3
  val N = 4
  val MIN_VALUE = 5
  val MAX_VALUE = 6
  val ITEMS = 7
  val LEVELS = 8
  val NUM_FIELDS = 9

  /** Build the StructType for KLL sketch intermediate data. */
  def intermediateStructType(childType: DataType): StructType = StructType(
    Array(
      StructField("percentiles", ArrayType(DoubleType), nullable = true),
      StructField("percentilesIsArray", BooleanType, nullable = true),
      StructField("accuracy", DoubleType, nullable = true),
      StructField("k", IntegerType, nullable = true),
      StructField("n", LongType, nullable = true),
      StructField("minValue", childType, nullable = true),
      StructField("maxValue", childType, nullable = true),
      StructField("items", ArrayType(childType), nullable = true),
      StructField("levels", ArrayType(IntegerType), nullable = true)
    ))

  /** Default KLL k parameter (same as Velox default). */
  val DEFAULT_K: Int = 200
}

/**
 * Helper object encapsulating the core KLL sketch algorithm logic.
 *
 * This is a simplified implementation that runs on the Spark side during fallback. The struct
 * layout is fully compatible with Velox's KLL sketch intermediate type, enabling partial fallback
 * (Velox partial -> Spark final).
 *
 * The algorithm stores all inserted values in the items array (level 0). When the items array grows
 * too large, a compaction step is performed to reduce memory usage while maintaining approximate
 * quantile guarantees.
 */
object KllSketchHelper {

  /**
   * Create an empty KLL sketch as an InternalRow (struct).
   *
   * @param percentiles
   *   Array of percentile values
   * @param isArray
   *   Whether the percentile argument is an array
   * @param accuracy
   *   The accuracy parameter (maps to relativeError = 1/accuracy)
   * @param childType
   *   The data type of values being aggregated
   */
  def createEmpty(
      percentiles: ArrayData,
      isArray: Boolean,
      accuracy: Double,
      childType: DataType): InternalRow = {
    val k = KllSketchFieldIndex.DEFAULT_K
    InternalRow(
      percentiles, // percentiles
      isArray, // percentilesIsArray
      accuracy, // accuracy
      k, // k
      0L, // n
      null, // minValue
      null, // maxValue
      new GenericArrayData(Array.empty[Any]), // items
      new GenericArrayData(Array(0, 0)) // levels: [0, 0] means 1 level with 0 items
    )
  }

  /**
   * Add a value to the KLL sketch. Returns a new InternalRow representing the updated sketch.
   *
   * @param sketch
   *   The current sketch as InternalRow
   * @param value
   *   The value to add
   * @param childType
   *   The data type of the value
   */
  def add(sketch: InternalRow, value: Any, childType: DataType): InternalRow = {
    if (value == null) return sketch

    val n = sketch.getLong(KllSketchFieldIndex.N)
    val k = sketch.getInt(KllSketchFieldIndex.K)
    val items = sketch.getArray(KllSketchFieldIndex.ITEMS)
    val levels = sketch.getArray(KllSketchFieldIndex.LEVELS)

    val doubleValue = toDouble(value, childType)

    // Update min/max
    val oldMin = if (sketch.isNullAt(KllSketchFieldIndex.MIN_VALUE)) {
      doubleValue
    } else {
      math.min(
        toDouble(sketch.get(KllSketchFieldIndex.MIN_VALUE, childType), childType),
        doubleValue)
    }
    val oldMax = if (sketch.isNullAt(KllSketchFieldIndex.MAX_VALUE)) {
      doubleValue
    } else {
      math.max(
        toDouble(sketch.get(KllSketchFieldIndex.MAX_VALUE, childType), childType),
        doubleValue)
    }

    // Append value to items (level 0)
    val newItemsArr = new Array[Any](items.numElements() + 1)
    var i = 0
    while (i < items.numElements()) {
      newItemsArr(i) = items.get(i, childType)
      i += 1
    }
    newItemsArr(items.numElements()) = fromDouble(doubleValue, childType)

    // Update levels: increment the last element (end of level 0)
    val newLevelsArr = new Array[Int](levels.numElements())
    i = 0
    while (i < levels.numElements()) {
      newLevelsArr(i) = levels.getInt(i)
      i += 1
    }
    newLevelsArr(newLevelsArr.length - 1) += 1

    // Compaction: if level 0 is too large (> 2*k), compact
    var finalItems = newItemsArr
    var finalLevels = newLevelsArr
    val level0Size = newLevelsArr(newLevelsArr.length - 1) - newLevelsArr(0)
    if (level0Size > 2 * k) {
      val compacted = compactLevel0(finalItems, finalLevels, k, childType)
      finalItems = compacted._1
      finalLevels = compacted._2
    }

    InternalRow(
      sketch.getArray(KllSketchFieldIndex.PERCENTILES),
      sketch.getBoolean(KllSketchFieldIndex.PERCENTILES_IS_ARRAY),
      sketch.getDouble(KllSketchFieldIndex.ACCURACY),
      k,
      n + 1,
      fromDouble(oldMin, childType),
      fromDouble(oldMax, childType),
      new GenericArrayData(finalItems),
      new GenericArrayData(finalLevels.map(_.asInstanceOf[Any]))
    )
  }

  /** Merge two KLL sketches. Returns a new InternalRow representing the merged sketch. */
  def merge(left: InternalRow, right: InternalRow, childType: DataType): InternalRow = {
    if (left == null || left.getLong(KllSketchFieldIndex.N) == 0) return right
    if (right == null || right.getLong(KllSketchFieldIndex.N) == 0) return left

    val leftN = left.getLong(KllSketchFieldIndex.N)
    val rightN = right.getLong(KllSketchFieldIndex.N)
    val k = math.max(left.getInt(KllSketchFieldIndex.K), right.getInt(KllSketchFieldIndex.K))

    // Merge min/max
    val leftMin = toDouble(left.get(KllSketchFieldIndex.MIN_VALUE, childType), childType)
    val rightMin = toDouble(right.get(KllSketchFieldIndex.MIN_VALUE, childType), childType)
    val leftMax = toDouble(left.get(KllSketchFieldIndex.MAX_VALUE, childType), childType)
    val rightMax = toDouble(right.get(KllSketchFieldIndex.MAX_VALUE, childType), childType)
    val mergedMin = math.min(leftMin, rightMin)
    val mergedMax = math.max(leftMax, rightMax)

    // Merge items: concatenate all items from both sketches
    val leftItems = left.getArray(KllSketchFieldIndex.ITEMS)
    val rightItems = right.getArray(KllSketchFieldIndex.ITEMS)
    val leftLevels = left.getArray(KllSketchFieldIndex.LEVELS)
    val rightLevels = right.getArray(KllSketchFieldIndex.LEVELS)

    val totalItemsSize = leftItems.numElements() + rightItems.numElements()
    val mergedItems = new Array[Any](totalItemsSize)
    var idx = 0
    var i = 0
    while (i < leftItems.numElements()) {
      mergedItems(idx) = leftItems.get(i, childType)
      idx += 1
      i += 1
    }
    i = 0
    while (i < rightItems.numElements()) {
      mergedItems(idx) = rightItems.get(i, childType)
      idx += 1
      i += 1
    }

    // Merge levels: combine level structures
    // Simple approach: put all items into a single level
    val mergedLevels = Array(0, totalItemsSize)

    // Compact if necessary
    var finalItems = mergedItems
    var finalLevels = mergedLevels
    if (totalItemsSize > 2 * k) {
      val compacted = compactLevel0(finalItems, finalLevels, k, childType)
      finalItems = compacted._1
      finalLevels = compacted._2
    }

    // Use left's percentiles/accuracy settings
    InternalRow(
      left.getArray(KllSketchFieldIndex.PERCENTILES),
      left.getBoolean(KllSketchFieldIndex.PERCENTILES_IS_ARRAY),
      left.getDouble(KllSketchFieldIndex.ACCURACY),
      k,
      leftN + rightN,
      fromDouble(mergedMin, childType),
      fromDouble(mergedMax, childType),
      new GenericArrayData(finalItems),
      new GenericArrayData(finalLevels.map(_.asInstanceOf[Any]))
    )
  }

  /**
   * Evaluate percentiles from a KLL sketch.
   *
   * @param sketch
   *   The sketch as InternalRow
   * @param childType
   *   The data type of values
   * @return
   *   The percentile value(s) - either a single value or an ArrayData
   */
  def eval(sketch: InternalRow, childType: DataType): Any = {
    val n = sketch.getLong(KllSketchFieldIndex.N)
    if (n == 0) return null

    val percentiles = sketch.getArray(KllSketchFieldIndex.PERCENTILES)
    val isArray = sketch.getBoolean(KllSketchFieldIndex.PERCENTILES_IS_ARRAY)
    val items = sketch.getArray(KllSketchFieldIndex.ITEMS)

    // Collect all items and sort them as doubles for quantile estimation
    val numItems = items.numElements()
    val doubles = new Array[Double](numItems)
    var i = 0
    while (i < numItems) {
      doubles(i) = toDouble(items.get(i, childType), childType)
      i += 1
    }
    util.Arrays.sort(doubles)

    // Compute percentiles
    val numPercentiles = percentiles.numElements()
    val results = new Array[Any](numPercentiles)
    i = 0
    while (i < numPercentiles) {
      val p = percentiles.getDouble(i)
      val rank = math.min((p * numItems).toInt, numItems - 1)
      results(i) = fromDouble(doubles(math.max(0, rank)), childType)
      i += 1
    }

    if (results.isEmpty) {
      null
    } else if (isArray) {
      new GenericArrayData(results)
    } else {
      results(0)
    }
  }

  /** Compact level 0 by sorting and taking every other element. */
  private def compactLevel0(
      items: Array[Any],
      levels: Array[Int],
      k: Int,
      childType: DataType): (Array[Any], Array[Int]) = {
    // Sort the items by their double values
    val doubles = items.map(v => toDouble(v, childType))
    val indices = doubles.indices.toArray.map(Integer.valueOf)
    util.Arrays.sort(
      indices,
      (a: Integer, b: Integer) => java.lang.Double.compare(doubles(a), doubles(b)))

    // Keep every other element (simple compaction)
    val half = (items.length + 1) / 2
    val compactedItems = new Array[Any](half)
    var i = 0
    while (i < half) {
      compactedItems(i) = items(indices(i * 2))
      i += 1
    }

    (compactedItems, Array(0, half))
  }

  /** Convert a value to Double for comparison/sorting. */
  private def toDouble(value: Any, dataType: DataType): Double = {
    if (value == null) return Double.NaN
    dataType match {
      case DoubleType => value.asInstanceOf[Double]
      case FloatType => value.asInstanceOf[Float].toDouble
      case IntegerType | DateType => value.asInstanceOf[Int].toDouble
      case LongType => value.asInstanceOf[Long].toDouble
      case ShortType => value.asInstanceOf[Short].toDouble
      case ByteType => value.asInstanceOf[Byte].toDouble
      case _: DecimalType =>
        value.asInstanceOf[Decimal].toDouble
      case _ => value.asInstanceOf[Number].doubleValue()
    }
  }

  /** Convert a Double back to the original data type. */
  private def fromDouble(value: Double, dataType: DataType): Any = {
    dataType match {
      case DoubleType => value
      case FloatType => value.toFloat
      case IntegerType | DateType => value.toInt
      case LongType => value.toLong
      case ShortType => value.toShort
      case ByteType => value.toByte
      case dt: DecimalType => Decimal(value, dt.precision, dt.scale)
      case _ => value
    }
  }
}

/**
 * Expression that adds a value to a KLL sketch. Used as the update expression in
 * VeloxApproximatePercentile's DeclarativeAggregate.
 *
 * @param sketch
 *   The current sketch (struct expression)
 * @param value
 *   The value to add
 * @param childType
 *   The data type of the value being aggregated
 */
case class KllSketchAdd(sketch: Expression, value: Expression, childType: DataType)
  extends BinaryExpression {

  override def left: Expression = sketch
  override def right: Expression = value
  override def dataType: DataType = sketch.dataType
  override def nullable: Boolean = false

  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): Expression =
    copy(sketch = newLeft, value = newRight)

  override def eval(input: InternalRow): Any = {
    val sketchRow = left.eval(input).asInstanceOf[InternalRow]
    val v = right.eval(input)
    if (v == null) return sketchRow
    KllSketchHelper.add(sketchRow, v, childType)
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    throw new UnsupportedOperationException("KllSketchAdd does not support codegen")
}

/**
 * Expression that merges two KLL sketches. Used as the merge expression in
 * VeloxApproximatePercentile's DeclarativeAggregate.
 *
 * @param left
 *   The left sketch
 * @param right
 *   The right sketch
 * @param childType
 *   The data type of the values being aggregated
 */
case class KllSketchMerge(left: Expression, right: Expression, childType: DataType)
  extends BinaryExpression {

  override def dataType: DataType = left.dataType
  override def nullable: Boolean = false

  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): Expression =
    copy(left = newLeft, right = newRight)

  override def eval(input: InternalRow): Any = {
    val leftRow = left.eval(input).asInstanceOf[InternalRow]
    val rightRow = right.eval(input).asInstanceOf[InternalRow]
    if (leftRow == null) return rightRow
    if (rightRow == null) return leftRow
    KllSketchHelper.merge(leftRow, rightRow, childType)
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    throw new UnsupportedOperationException("KllSketchMerge does not support codegen")
}

/**
 * Expression that evaluates percentiles from a KLL sketch. Used as the evaluate expression in
 * VeloxApproximatePercentile's DeclarativeAggregate.
 *
 * @param sketch
 *   The sketch expression
 * @param returnArray
 *   Whether to return an array of percentiles
 * @param resultType
 *   The result data type
 */
case class KllSketchEval(
    sketch: Expression,
    returnArray: Boolean,
    resultType: DataType,
    childType: DataType)
  extends UnaryExpression {

  override def child: Expression = sketch
  override def dataType: DataType = resultType
  override def nullable: Boolean = true

  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(sketch = newChild)

  override def eval(input: InternalRow): Any = {
    val sketchRow = child.eval(input).asInstanceOf[InternalRow]
    if (sketchRow == null) return null
    KllSketchHelper.eval(sketchRow, childType)
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    throw new UnsupportedOperationException("KllSketchEval does not support codegen")
}

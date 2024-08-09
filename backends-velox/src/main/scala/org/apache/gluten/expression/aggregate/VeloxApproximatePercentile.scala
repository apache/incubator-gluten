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
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.catalyst.expressions.aggregate.{ApproximatePercentile, ImperativeAggregate}
import org.apache.spark.sql.catalyst.trees.TernaryLike
import org.apache.spark.sql.types.{ArrayType, BooleanType, DataType, DoubleType, IntegerType, LongType, StructField, StructType}

case class VeloxApproximatePercentile(
    child: Expression,
    percentageExpression: Expression,
    accuracyExpression: Expression,
    override val mutableAggBufferOffset: Int,
    override val inputAggBufferOffset: Int)
  extends ImperativeAggregate
  with TernaryLike[Expression] {

  private val delegate = ApproximatePercentile(
    child,
    percentageExpression,
    accuracyExpression,
    mutableAggBufferOffset,
    inputAggBufferOffset)

  private lazy val aggBufferDataType: DataType = {
    val childType = child.dataType
    StructType(
      Array(
        StructField("col1", ArrayType(DoubleType)),
        StructField("col2", BooleanType, false),
        StructField("col3", IntegerType, false),
        StructField("col4", IntegerType, false),
        StructField("col5", LongType, false),
        StructField("col6", childType, false),
        StructField("col7", childType, false),
        StructField("col8", ArrayType(childType)),
        StructField("col9", ArrayType(IntegerType))
      ))
  }
  override def aggBufferAttributes: Seq[AttributeReference] =
    List(AttributeReference("buffer", aggBufferDataType)())
  final override def aggBufferSchema: StructType =
    StructType(
      aggBufferAttributes.map(a => StructField(a.name, a.dataType, a.nullable, a.metadata)))
  final override lazy val inputAggBufferAttributes: Seq[AttributeReference] =
    aggBufferAttributes.map(_.newInstance())

  override def first: Expression = child
  override def second: Expression = percentageExpression
  override def third: Expression = accuracyExpression

  override def checkInputDataTypes(): TypeCheckResult = delegate.checkInputDataTypes()

  override def nullable: Boolean = delegate.nullable

  override def dataType: DataType = delegate.dataType

  override def prettyName: String = "velox_percentile_approx"

  override def eval(input: InternalRow): Any = {
    throw new UnsupportedOperationException("eval")
  }

  override def initialize(mutableAggBuffer: InternalRow): Unit = {
    throw new UnsupportedOperationException("initialize")
  }

  override def merge(mutableAggBuffer: InternalRow, inputAggBuffer: InternalRow): Unit = {
    throw new UnsupportedOperationException("merge")
  }

  override def update(mutableAggBuffer: InternalRow, inputRow: InternalRow): Unit = {
    throw new UnsupportedOperationException("update")
  }

  override def withNewMutableAggBufferOffset(newOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newOffset)

  override def withNewInputAggBufferOffset(newOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newOffset)

  override protected def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression): VeloxApproximatePercentile =
    copy(child = newFirst, percentageExpression = newSecond, accuracyExpression = newThird)
}

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
package org.apache.spark.sql.catalyst.expressions.aggregate

import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.trees.TernaryLike
import org.apache.spark.sql.types.DataType
import org.apache.spark.util.TaskResources
import org.apache.spark.util.sketch.BloomFilter

/**
 * Velox's bloom-filter implementation uses different algorithms internally comparing to vanilla
 * Spark so produces different intermediate aggregate data. Thus we use different filter function /
 * agg function types for Velox's version to distinguish from vanilla Spark's implementation.
 */
case class VeloxBloomFilterAggregate(
    child: Expression,
    estimatedNumItemsExpression: Expression,
    numBitsExpression: Expression,
    override val mutableAggBufferOffset: Int,
    override val inputAggBufferOffset: Int)
  extends TypedImperativeAggregate[BloomFilter]
  with TernaryLike[Expression] {

  private val delegate = SparkShimLoader.getSparkShims.newBloomFilterAggregate[BloomFilter](
    child,
    estimatedNumItemsExpression,
    numBitsExpression,
    mutableAggBufferOffset,
    inputAggBufferOffset)

  override def prettyName: String = "velox_bloom_filter_agg"

  override def first: Expression = child

  override def second: Expression = estimatedNumItemsExpression

  override def third: Expression = numBitsExpression

  override def checkInputDataTypes(): TypeCheckResult = delegate.checkInputDataTypes()

  override def nullable: Boolean = delegate.nullable

  override def dataType: DataType = delegate.dataType

  override protected def withNewChildrenInternal(
      newChild: Expression,
      newEstimatedNumItemsExpression: Expression,
      newNumBitsExpression: Expression): VeloxBloomFilterAggregate = {
    copy(
      child = newChild,
      estimatedNumItemsExpression = newEstimatedNumItemsExpression,
      numBitsExpression = newNumBitsExpression)
  }

  override def createAggregationBuffer(): BloomFilter = {
    if (!TaskResources.inSparkTask()) {
      throw new UnsupportedOperationException("velox_bloom_filter_agg is not evaluable on Driver")
    }
    ???
  }

  override def update(buffer: BloomFilter, input: InternalRow): BloomFilter =
    throw new UnsupportedOperationException()

  override def merge(buffer: BloomFilter, input: BloomFilter): BloomFilter =
    throw new UnsupportedOperationException()

  override def eval(buffer: BloomFilter): Any = throw new UnsupportedOperationException()

  override def serialize(buffer: BloomFilter): Array[Byte] =
    throw new UnsupportedOperationException()

  override def deserialize(storageFormat: Array[Byte]): BloomFilter =
    throw new UnsupportedOperationException()

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    throw new UnsupportedOperationException()

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    throw new UnsupportedOperationException()

}

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
package org.apache.spark.sql.catalyst.expressions
import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.DataType

/**
 * Velox's bloom-filter implementation uses different algorithms internally comparing to vanilla
 * Spark so produces different intermediate aggregate data. Thus we use different filter function /
 * agg function types for Velox's version to distinguish from vanilla Spark's implementation.
 */
case class VeloxBloomFilterMightContain(
    bloomFilterExpression: Expression,
    valueExpression: Expression)
  extends BinaryExpression {

  private val delegate =
    SparkShimLoader.getSparkShims.newMightContain(bloomFilterExpression, valueExpression)

  override def prettyName: String = "velox_might_contain"

  override def left: Expression = bloomFilterExpression

  override def right: Expression = valueExpression

  override def nullable: Boolean = delegate.nullable

  override def dataType: DataType = delegate.dataType

  override def checkInputDataTypes(): TypeCheckResult = delegate.checkInputDataTypes()

  override protected def withNewChildrenInternal(
      newBloomFilterExpression: Expression,
      newValueExpression: Expression): VeloxBloomFilterMightContain =
    copy(bloomFilterExpression = newBloomFilterExpression, valueExpression = newValueExpression)

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    throw new UnsupportedOperationException()
}

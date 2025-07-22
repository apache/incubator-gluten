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
package org.apache.gluten.expression

import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.utils.VeloxBloomFilter

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression}
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.types.DataType

import io.netty.util.internal.PlatformDependent

import java.nio.ByteBuffer

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

  private lazy val bloomFilterData: ByteBuffer = {
    val bytes = bloomFilterExpression.eval().asInstanceOf[Array[Byte]]
    if (bytes == null) {
      null
    } else {
      val buffer = ByteBuffer.allocateDirect(bytes.length)
      buffer.put(bytes)
    }
  }

  override def eval(input: InternalRow): Any = {
    if (bloomFilterData == null) {
      return null
    }
    val value = valueExpression.eval(input)
    if (value == null) {
      return null
    }
    VeloxBloomFilter.mightContainLongOnSerializedBloom(bloomFilterData, value.asInstanceOf[Long])
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    if (bloomFilterData == null) {
      return ev.copy(isNull = TrueLiteral, value = JavaCode.defaultLiteral(dataType))
    }
    ctx.addReferenceObj(
      "bloomFilterData",
      bloomFilterData
    ) // This field keeps the direct buffer data alive.
    val bfAddr = ctx.addReferenceObj(
      "bloomFilterAddress",
      PlatformDependent.directBufferAddress(bloomFilterData))

    val valueEval = valueExpression.genCode(ctx)
    val code =
      code"""
      ${valueEval.code}
      boolean ${ev.isNull} = ${valueEval.isNull};
      ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
      if (!${ev.isNull}) {
        ${ev.value} = ${classOf[VeloxBloomFilter].getName}.mightContainLongOnSerializedBloom(
        (Long) $bfAddr, (Long)${valueEval.value});
      }"""
    ev.copy(code = code)
  }
}

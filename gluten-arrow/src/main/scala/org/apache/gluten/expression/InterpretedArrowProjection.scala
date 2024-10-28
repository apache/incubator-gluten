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

import org.apache.gluten.vectorized.ArrowColumnarRow

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReferences
import org.apache.spark.sql.catalyst.expressions.aggregate.NoOp
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BinaryType, DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * A [[ArrowProjection]] that is calculated by calling `eval` on each of the specified expressions.
 *
 * @param expressions
 *   a sequence of expressions that determine the value of each column of the output row.
 */
class InterpretedArrowProjection(expressions: Seq[Expression]) extends ArrowProjection {
  def this(expressions: Seq[Expression], inputSchema: Seq[Attribute]) =
    this(bindReferences(expressions, inputSchema))

  private[this] val subExprEliminationEnabled = SQLConf.get.subexpressionEliminationEnabled
  private[this] val exprs = prepareExpressions(expressions, subExprEliminationEnabled)

  override def initialize(partitionIndex: Int): Unit = {
    initializeExprs(exprs, partitionIndex)
  }

  private[this] val validExprs = expressions.zipWithIndex.filter {
    case (NoOp, _) => false
    case _ => true
  }

  private[this] var mutableRow: ArrowColumnarRow = null

  override def currentValue: ArrowColumnarRow = mutableRow

  override def target(row: ArrowColumnarRow): ArrowProjection = {
    mutableRow = row
    this
  }

  private def getStringWriter(ordinal: Int, dt: DataType): (ArrowColumnarRow, Any) => Unit =
    dt match {
      case StringType => (input, v) => input.setUTF8String(ordinal, v.asInstanceOf[UTF8String])
      case BinaryType => (input, v) => input.setBinary(ordinal, v.asInstanceOf[Array[Byte]])
      case _ => (input, v) => input.update(ordinal, v)
    }

  private[this] val fieldWriters: Array[Any => Unit] = validExprs.map {
    case (e, i) =>
      val writer = if (e.dataType.isInstanceOf[StringType] || e.dataType.isInstanceOf[BinaryType]) {
        getStringWriter(i, e.dataType)
      } else InternalRow.getWriter(i, e.dataType)
      if (!e.nullable) { (v: Any) => writer(mutableRow, v) }
      else {
        (v: Any) =>
          {
            if (v == null) {
              mutableRow.setNullAt(i)
            } else {
              writer(mutableRow, v)
            }
          }
      }
  }.toArray

  override def apply(input: InternalRow): ArrowColumnarRow = {
    if (subExprEliminationEnabled) {
      runtime.setInput(input)
    }

    var i = 0
    while (i < validExprs.length) {
      val (_, ordinal) = validExprs(i)
      // Store the result into buffer first, to make the projection atomic (needed by aggregation)
      fieldWriters(i)(exprs(ordinal).eval(input))
      i += 1
    }
    mutableRow
  }
}

/** Helper functions for creating an [[InterpretedArrowProjection]]. */
object InterpretedArrowProjection {

  /** Returns a [[ArrowProjection]] for given sequence of bound Expressions. */
  def createProjection(exprs: Seq[Expression]): ArrowProjection = {
    new InterpretedArrowProjection(exprs)
  }
}

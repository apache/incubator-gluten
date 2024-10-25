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
import org.apache.spark.sql.catalyst.expressions.{Attribute, BoundReference, Expression, ExpressionsEvaluator}
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReferences
import org.apache.spark.sql.types.{DataType, StructType}

// Not thread safe.
abstract class ArrowProjection extends (InternalRow => ArrowColumnarRow) with ExpressionsEvaluator {
  def currentValue: ArrowColumnarRow

  /** Uses the given row to store the output of the projection. */
  def target(row: ArrowColumnarRow): ArrowProjection
}

/** The factory object for `ArrowProjection`. */
object ArrowProjection {

  /**
   * Returns an ArrowProjection for given StructType.
   *
   * CAUTION: the returned projection object is *not* thread-safe.
   */
  def create(schema: StructType): ArrowProjection = create(schema.fields.map(_.dataType))

  /**
   * Returns an ArrowProjection for given Array of DataTypes.
   *
   * CAUTION: the returned projection object is *not* thread-safe.
   */
  def create(fields: Array[DataType]): ArrowProjection = {
    create(fields.zipWithIndex.map(x => BoundReference(x._2, x._1, nullable = true)))
  }

  /** Returns an ArrowProjection for given sequence of bound Expressions. */
  def create(exprs: Seq[Expression]): ArrowProjection = {
    InterpretedArrowProjection.createProjection(exprs)
  }

  def create(expr: Expression): ArrowProjection = create(Seq(expr))

  /**
   * Returns an ArrowProjection for given sequence of Expressions, which will be bound to
   * `inputSchema`.
   */
  def create(exprs: Seq[Expression], inputSchema: Seq[Attribute]): ArrowProjection = {
    create(bindReferences(exprs, inputSchema))
  }
}

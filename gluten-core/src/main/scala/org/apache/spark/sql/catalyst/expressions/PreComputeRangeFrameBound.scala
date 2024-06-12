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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.{DataType, Metadata}

/**
 * Represents a pre-compute boundary for range frame when boundary is non-SpecialFrameBoundary,
 * since Velox doesn't support constant offset for range frame. It acts like the original boundary
 * which is foldable and generate the same result when eval is invoked so that if the WindowExec
 * fallback to Vanilla Spark it can still work correctly.
 * @param child
 *   The alias to pre-compute projection column
 * @param originalBound
 *   The original boundary which is a foldable expression
 */
case class PreComputeRangeFrameBound(child: Alias, originalBound: Expression)
  extends UnaryExpression
  with NamedExpression {

  override def foldable: Boolean = true

  override def eval(input: InternalRow): Any = originalBound.eval(input)

  override def genCode(ctx: CodegenContext): ExprCode = originalBound.genCode(ctx)

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    originalBound.genCode(ctx)

  override def name: String = child.name

  override def exprId: ExprId = child.exprId

  override def qualifier: Seq[String] = child.qualifier

  override def newInstance(): NamedExpression =
    PreComputeRangeFrameBound(child.newInstance().asInstanceOf[Alias], originalBound)

  override lazy val resolved: Boolean = originalBound.resolved

  override def dataType: DataType = child.dataType

  override def nullable: Boolean = child.nullable

  override def metadata: Metadata = child.metadata

  override def toAttribute: Attribute = child.toAttribute

  override def toString: String = child.toString

  override def hashCode(): Int = child.hashCode()

  override def equals(other: Any): Boolean = other match {
    case a: PreComputeRangeFrameBound =>
      child.equals(a.child)
    case _ => false
  }

  override def sql: String = child.sql

  override protected def withNewChildInternal(newChild: Expression): PreComputeRangeFrameBound =
    copy(child = newChild.asInstanceOf[Alias])

}

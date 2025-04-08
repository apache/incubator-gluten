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

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.exception.GlutenException
import org.apache.gluten.expression.CHFlattenedExpression.genFlattenedExpression

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.DataType

abstract class CHFlattenedExpression(children: Seq[Expression], name: String) extends Expression {

  def this() = {
    this(Seq.empty[Expression], "")
  }

  override def toString: String = s"$name(${children.mkString(", ")})"

  override def eval(input: InternalRow): Any = null

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = null

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    genFlattenedExpression(dataType, newChildren, name, nullable) match {
      case Some(f) => f
      case None =>
        throw new GlutenException(
          String.format(
            "Logical error, the function %s should not be a clickhouse Flattened function.",
            name))
    }
}

case class FlattenedAnd(
    dataType: DataType,
    children: Seq[Expression],
    name: String,
    nullable: Boolean)
  extends CHFlattenedExpression(children, name) {}

case class FlattenedOr(
    dataType: DataType,
    children: Seq[Expression],
    name: String,
    nullable: Boolean)
  extends CHFlattenedExpression(children, name) {}

object CHFlattenedExpression {

  def sigAnd: Sig = Sig[FlattenedAnd]("FlattenedAnd")
  def sigOr: Sig = Sig[FlattenedOr]("FlattenedOr")

  def supported(name: String): Boolean = {
    GlutenConfig.get.getSupportedFlattenedExpressions.split(",").exists(p => p.equals(name))
  }

  def genFlattenedExpression(
      dataType: DataType,
      children: Seq[Expression],
      name: String,
      nullable: Boolean): Option[CHFlattenedExpression] = name match {
    case "and" => Option.apply(FlattenedAnd(dataType, children, name, nullable))
    case "or" => Option.apply(FlattenedOr(dataType, children, name, nullable))
    case _ => Option.empty
  }

}

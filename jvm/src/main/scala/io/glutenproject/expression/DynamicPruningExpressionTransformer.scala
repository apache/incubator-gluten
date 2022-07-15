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

package io.glutenproject.expression

import io.glutenproject.substrait.expression.{ExpressionBuilder, ExpressionNode}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.execution.InSubqueryExec
import org.apache.spark.sql.types._

class DynamicPruningExpressionTransformer(
                                 query: DynamicPruningExpression)
  extends Expression with ExpressionTransformer {
  override def children: Seq[Expression] = Nil

  override def toString: String = query.toString

  override def eval(input: InternalRow): Any = query.eval(input)

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = query.child.genCode(ctx)

  override def canEqual(that: Any): Boolean = query.canEqual(that)

  override def productArity: Int = query.productArity

  override def productElement(n: Int): Any = query.productElement(n)

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    if (query.child.isInstanceOf[InSubqueryExec]) {
      val subquery = query.child.asInstanceOf[InSubqueryExec]
      val rows = subquery.plan.executeCollect()
      val result = if (subquery.plan.output.length > 1) {
        rows.asInstanceOf[Array[Any]]
      } else {
        rows.map(_.get(0, subquery.child.dataType))
      }
      val inSetExpression = InSet(subquery.child, result.toSet)
      ExpressionConverter.replaceWithExpressionTransformer(inSetExpression, subquery.plan.output)
        .asInstanceOf[ExpressionTransformer]
        .doTransform(args)
    } else if (query.child.isInstanceOf[Literal]) {
      val lit = query.child.asInstanceOf[Literal]
      ExpressionBuilder.makeLiteral(lit.value, lit.dataType, lit.nullable)
    } else {
      throw new UnsupportedOperationException("Can not support DPP with non Broadcast join.")
    }
  }

  override def dataType: DataType = query.dataType

  override def nullable: Boolean = true
}

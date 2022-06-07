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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.execution.ScalarSubquery
import org.apache.spark.sql.types._

class ScalarSubqueryTransformer(
  query: ScalarSubquery)
  extends Expression with ExpressionTransformer {
  override def dataType: DataType = query.dataType
  override def children: Seq[Expression] = Nil
  override def nullable: Boolean = true
  override def toString: String = query.toString
  override def eval(input: InternalRow): Any = query.eval(input)
  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = query.doGenCode(ctx, ev)
  override def canEqual(that: Any): Boolean = query.canEqual(that)
  override def productArity: Int = query.productArity
  override def productElement(n: Int): Any = query.productElement(n)

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    // the first column in first row from `query`.
    val rows = query.plan.executeCollect()
    if (rows.length > 1) {
      sys.error(s"more than one row returned by a subquery used as an expression:\n${query.plan}")
    }
    val result: AnyRef = if (rows.length == 1) {
      assert(rows(0).numFields == 1,
        s"Expects 1 field, but got ${rows(0).numFields}; something went wrong in analysis")
      rows(0).get(0, dataType)
    } else {
      // If there is no rows returned, the result should be null.
      null
    }
    ExpressionBuilder.makeLiteral(result, dataType, nullable)
  }
}

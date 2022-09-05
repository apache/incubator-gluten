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
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.ScalarSubquery
import org.apache.spark.sql.execution.{ExecSubqueryExpression, BaseSubqueryExec}
import org.apache.spark.sql.types._

class ScalarSubqueryTransformer(plan: BaseSubqueryExec, exprId: ExprId,
                                query: ScalarSubquery)
  extends ScalarSubquery(plan, exprId) with ExpressionTransformer {

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
    ExpressionBuilder.makeLiteral(result, dataType, result == null)
  }
  override def eval(input: InternalRow): Any = {
    throw new UnsupportedOperationException(s"This operator doesn't support eval().")
  }
  override def updateResult(): Unit = {
    throw new UnsupportedOperationException(s"This operator doesn't support updateResult().")
  }
  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    throw new UnsupportedOperationException(s"This operator doesn't support doGenCode().")
  }
  override def withNewPlan(query: BaseSubqueryExec): ScalarSubquery = copy(plan = query)
  override def dataType: DataType = plan.schema.fields.head.dataType
  override def nullable: Boolean = true
}

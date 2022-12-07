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

import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.execution.{GlutenColumnarToRowExecBase, WholeStageTransformerExec}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.spark.sql.types.DecimalType

object ExpressionConverter extends Logging {

  def replaceWithExpressionTransformer(expr: Expression,
      attributeSeq: Seq[Attribute]): Expression = {
    if (!BackendsApiManager.getValidatorApiInstance.doValidate(expr)) {
      throw new UnsupportedOperationException(s"Not supported: $expr.")
    }
    expr match {
      case a: Alias =>
        logInfo(s"${expr.getClass} ${expr} is supported")
        BackendsApiManager.getSparkPlanExecApiInstance.genAliasTransformer(
          replaceWithExpressionTransformer(a.child, attributeSeq),
          a.name,
          a.exprId,
          a.qualifier,
          a.explicitMetadata)
      case a: AttributeReference =>
        logInfo(s"${expr.getClass} ${expr} is supported")
        if (attributeSeq == null) {
          throw new UnsupportedOperationException(s"attributeSeq should not be null.")
        }
        val bindReference =
          BindReferences.bindReference(expr, attributeSeq, allowFailures = true)
        if (bindReference == expr) {
          // This means bind failure.
          throw new UnsupportedOperationException(s"${expr} attribute binding failed.")
        } else {
          val b = bindReference.asInstanceOf[BoundReference]
          new AttributeReferenceTransformer(
            a.name, b.ordinal, a.dataType, b.nullable, a.metadata)(a.exprId, a.qualifier)
        }
      case l: Literal =>
        logInfo(s"${expr.getClass} ${expr} is supported")
        new LiteralTransformer(l)
      case b: BinaryArithmetic =>
        logInfo(s"${expr.getClass} ${expr} is supported")
        BinaryArithmeticTransformer.create(
          replaceWithExpressionTransformer(
            b.left,
            attributeSeq),
          replaceWithExpressionTransformer(
            b.right,
            attributeSeq),
          expr)
      case b: BoundReference =>
        logInfo(s"${expr.getClass} ${expr} is supported")
        new BoundReferenceTransformer(b.ordinal, b.dataType, b.nullable)
      case r: Round =>
        logInfo(s"${expr.getClass} ${expr} is supported")
        RoundOperatorTransformer.create(
          replaceWithExpressionTransformer(
            r.child,
            attributeSeq),
          replaceWithExpressionTransformer(
            r.scale,
            attributeSeq),
          expr)
      case r: BRound =>
        logInfo(s"${expr.getClass} ${expr} is supported")
        RoundOperatorTransformer.create(
          replaceWithExpressionTransformer(
            r.child,
            attributeSeq),
          replaceWithExpressionTransformer(
            r.scale,
            attributeSeq),
          expr)
      case b: BinaryOperator =>
        logInfo(s"${expr.getClass} ${expr} is supported")
        BinaryOperatorTransformer.create(
          replaceWithExpressionTransformer(
            b.left,
            attributeSeq),
          replaceWithExpressionTransformer(
            b.right,
            attributeSeq),
          expr)
      case b: BinaryExpression =>
        logInfo(s"${expr.getClass} ${expr} is supported")
        BinaryExpressionTransformer.create(
          replaceWithExpressionTransformer(
            b.left,
            attributeSeq),
          replaceWithExpressionTransformer(
            b.right,
            attributeSeq),
          expr)
      case i: If =>
        logInfo(s"${expr.getClass} ${expr} is supported")
        IfOperatorTransformer.create(
          replaceWithExpressionTransformer(
            i.predicate,
            attributeSeq),
          replaceWithExpressionTransformer(
            i.trueValue,
            attributeSeq),
          replaceWithExpressionTransformer(
            i.falseValue,
            attributeSeq),
          expr)
      case cw: CaseWhen =>
        logInfo(s"${expr.getClass} ${expr} is supportedn.")
        val colBranches = cw.branches.map { expr => {
          (
            replaceWithExpressionTransformer(
              expr._1,
              attributeSeq),
            replaceWithExpressionTransformer(
              expr._2,
              attributeSeq))
        }
        }
        val colElseValue = cw.elseValue.map { expr => {
          replaceWithExpressionTransformer(
            expr,
            attributeSeq)
        }
        }
        logDebug(s"colBanches: $colBranches")
        logDebug(s"colElseValue: $colElseValue")
        CaseWhenOperatorTransformer.create(colBranches, colElseValue, expr)
      case g: Greatest =>
        logInfo(s"${expr.getClass} ${expr} is supported")
        ComplexTypeMergeingOperatorTransformer.createGreatestOrLeast(
          g.children.map(
            child => replaceWithExpressionTransformer(child, attributeSeq)), g)
      case l: Least =>
        logInfo(s"${expr.getClass} ${expr} is supported")
        ComplexTypeMergeingOperatorTransformer.createGreatestOrLeast(
          l.children.map(
            child => replaceWithExpressionTransformer(child, attributeSeq)), l)
      case c: Coalesce =>
        logInfo(s"${expr.getClass} ${expr} is supported")
        val exprs = c.children.map { expr =>
          replaceWithExpressionTransformer(
            expr,
            attributeSeq)
        }
        CoalesceExpressionTransformer.create(exprs, expr)
      case i: In =>
        logInfo(s"${expr.getClass} ${expr} is supported")
        InExpressionTransformer.create(
          replaceWithExpressionTransformer(
            i.value,
            attributeSeq),
          i.list,
          expr)
      case i: InSet =>
        logInfo(s"${expr.getClass} ${expr} is supported")
        InSetOperatorTransformer.create(
          replaceWithExpressionTransformer(
            i.child,
            attributeSeq),
          i.hset,
          expr)
      case t: TernaryExpression =>
        logInfo(s"${expr.getClass} ${expr} is supported.")
        TernaryExpressionTransformer.create(
          replaceWithExpressionTransformer(
            t.first,
            attributeSeq),
          replaceWithExpressionTransformer(
            t.second,
            attributeSeq),
          replaceWithExpressionTransformer(
            t.third,
            attributeSeq),
          expr)
      case l: LeafExpression =>
        logInfo(s"${expr.getClass} $expr is supported")
        LeafOperatorTransformer.create(l)
      case t: QuaternaryExpression =>
        logInfo(s"${expr.getClass} ${expr} is supported.")
        QuaternaryExpressionTransformer.create(
          replaceWithExpressionTransformer(
            t.first,
            attributeSeq),
          replaceWithExpressionTransformer(
            t.second,
            attributeSeq),
          replaceWithExpressionTransformer(
            t.third,
            attributeSeq),
          replaceWithExpressionTransformer(
            t.fourth,
            attributeSeq),
          expr)
      case u: UnaryExpression =>
        logInfo(s"${expr.getClass} $expr is supported")
        if (!u.isInstanceOf[CheckOverflow] || !u.child.isInstanceOf[Divide]) {
          UnaryOperatorTransformer.create(
            replaceWithExpressionTransformer(
              u.child,
              attributeSeq),
            expr)
        } else {
          // CheckOverflow[Divide]: pass resType to Divide to avoid precision loss.
          val divide = u.child.asInstanceOf[Divide]
          val columnarDivide = BinaryArithmeticTransformer.createDivide(
            replaceWithExpressionTransformer(
              divide.left,
              attributeSeq),
            replaceWithExpressionTransformer(
              divide.right,
              attributeSeq),
            divide,
            u.dataType.asInstanceOf[DecimalType])
          UnaryOperatorTransformer.create(
            columnarDivide,
            expr)
        }
      case s: org.apache.spark.sql.execution.ScalarSubquery =>
        logInfo(s"${expr.getClass} ${expr} is supported")
        new ScalarSubqueryTransformer(s.plan, s.exprId, s)
      case c: Concat =>
        logInfo(s"${expr.getClass} ${expr} is supported")
        val exprs = c.children.map { expr =>
          replaceWithExpressionTransformer(
            expr,
            attributeSeq)
        }
        ConcatExpressionTransformer.create(exprs, expr)
      case m: Murmur3Hash =>
        logInfo(s"${expr.getClass} ${expr} is supported")
        val exprs = m.children.map { expr =>
          replaceWithExpressionTransformer(
            expr,
            attributeSeq)
        }
        new Murmur3HashTransformer(exprs, expr)
      case l: StringTrim =>
        if (l.trimStr != None) {
          // todo: to be remove when Velox support this argument
          throw new UnsupportedOperationException(s"not supported yet.")
        }
        logInfo(s"${expr.getClass} ${expr} is supported")
        TrimOperatorTransformer.create(
          replaceWithExpressionTransformer(l.srcStr, attributeSeq),
          expr)
      case l: StringTrimLeft =>
        if (l.trimStr != None) {
          throw new UnsupportedOperationException(s"not supported yet.")
        }
        logInfo(s"${expr.getClass} ${expr} is supported")
        TrimOperatorTransformer.create(
          replaceWithExpressionTransformer(l.srcStr, attributeSeq),
          expr)
      case r: StringTrimRight =>
        if (r.trimStr != None) {
          throw new UnsupportedOperationException(s"not supported yet.")
        }
        logInfo(s"${expr.getClass} ${expr} is supported")
        TrimOperatorTransformer.create(
          replaceWithExpressionTransformer(r.srcStr, attributeSeq),
          expr)
      case expr =>
        logDebug(s"${expr.getClass} or ${expr} is not currently supported.")
        throw new UnsupportedOperationException(
          s"${expr.getClass} or ${expr} is not currently supported.")
    }
  }

  def containsSubquery(expr: Expression): Boolean =
    expr match {
      case a: AttributeReference =>
        return false
      case lit: Literal =>
        return false
      case b: BoundReference =>
        return false
      case u: UnaryExpression =>
        containsSubquery(u.child)
      case b: BinaryOperator =>
        containsSubquery(b.left) || containsSubquery(b.right)
      case i: If =>
        containsSubquery(i.predicate) || containsSubquery(i.trueValue) || containsSubquery(
          i.falseValue)
      case cw: CaseWhen =>
        cw.branches.exists(p => containsSubquery(p._1) || containsSubquery(p._2)) ||
          cw.elseValue.exists(containsSubquery)
      case c: Coalesce =>
        c.children.exists(containsSubquery)
      case i: In =>
        containsSubquery(i.value)
      case ss: Substring =>
        containsSubquery(ss.str) || containsSubquery(ss.pos) || containsSubquery(ss.len)
      case oaps: io.glutenproject.expression.ScalarSubqueryTransformer =>
        return true
      case s: org.apache.spark.sql.execution.ScalarSubquery =>
        return true
      case c: Concat =>
        c.children.exists(containsSubquery)
      case b: BinaryExpression =>
        containsSubquery(b.left) || containsSubquery(b.right)
      case expr =>
        logDebug(s"${expr.getClass} | ${expr} is not currently supported.")
        throw new UnsupportedOperationException(
          s" --> ${expr.getClass} | ${expr} is not currently supported.")
    }

  /**
   * Transform BroadcastExchangeExec to ColumnarBroadcastExchangeExec in DynamicPruningExpression.
   *
   * @param partitionFilters
   * @return
   */
  def transformDynamicPruningExpr(partitionFilters: Seq[Expression]): Seq[Expression] = {

    def convertBroadcastExchangeToColumnar(exchange: BroadcastExchangeExec)
    : ColumnarBroadcastExchangeExec = {
      val newChild = exchange.child match {
        // get WholeStageTransformerExec directly
        case c2r: GlutenColumnarToRowExecBase => c2r.child
        // in case of fallbacking
        case codeGen: WholeStageCodegenExec =>
          if (codeGen.child.isInstanceOf[ColumnarToRowExec]) {
            val wholeStageTransformerExec = exchange.find(
              _.isInstanceOf[WholeStageTransformerExec])
            if (wholeStageTransformerExec.nonEmpty) {
              wholeStageTransformerExec.get
            } else {
              BackendsApiManager.getSparkPlanExecApiInstance.genRowToColumnarExec(codeGen)
            }
          } else {
            BackendsApiManager.getSparkPlanExecApiInstance.genRowToColumnarExec(codeGen)
          }
      }
      ColumnarBroadcastExchangeExec(exchange.mode, newChild)
    }

    partitionFilters.map {
      case dynamicPruning: DynamicPruningExpression =>
        dynamicPruning.transform {
          // Lookup inside subqueries for duplicate exchanges
          case in: InSubqueryExec => in.plan match {
            case _: SubqueryBroadcastExec =>
              val newIn = in.plan.transform {
                case exchange: BroadcastExchangeExec =>
                  convertBroadcastExchangeToColumnar(exchange)
              }.asInstanceOf[SubqueryBroadcastExec]
              val transformSubqueryBroadcast = ColumnarSubqueryBroadcastExec(
                newIn.name, newIn.index, newIn.buildKeys, newIn.child)
              in.copy(plan = transformSubqueryBroadcast.asInstanceOf[BaseSubqueryExec])
            case _: ReusedSubqueryExec if in.plan.child.isInstanceOf[SubqueryBroadcastExec] =>
              val newIn = in.plan.child.transform {
                case exchange: BroadcastExchangeExec =>
                  convertBroadcastExchangeToColumnar(exchange)
              }.asInstanceOf[SubqueryBroadcastExec]
              val transformSubqueryBroadcast = ColumnarSubqueryBroadcastExec(
                newIn.name, newIn.index, newIn.buildKeys, newIn.child)
              in.copy(plan = ReusedSubqueryExec(transformSubqueryBroadcast))
            case _ => in
          }
        }
      case e: Expression => e
    }
  }
}

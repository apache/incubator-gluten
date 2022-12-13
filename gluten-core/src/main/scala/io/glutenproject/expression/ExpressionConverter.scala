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
import org.apache.spark.sql.catalyst.optimizer.NormalizeNaNAndZero
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec

object ExpressionConverter extends Logging {

  def replaceWithExpressionTransformer(expr: Expression,
      attributeSeq: Seq[Attribute]): ExpressionTransformer = {
    // Check whether Gluten supports this expression
    val substraitExprName = ExpressionMappings.scalar_functions_map.get(expr.getClass)
    if (substraitExprName.isEmpty) {
      throw new UnsupportedOperationException(s"Not supported: $expr.")
    }
    // Check whether each backend supports this expression
    if (!BackendsApiManager.getValidatorApiInstance.doExprValidate(substraitExprName.get, expr)) {
      throw new UnsupportedOperationException(s"Not supported: $expr.")
    }
    expr match {
      case c: CreateArray =>
        val children = c.children.map(child =>
          replaceWithExpressionTransformer(child, attributeSeq))
        new CreateArrayTransformer(substraitExprName.get, children, true, c)
      case e: Explode =>
        new ExplodeTransformer(substraitExprName.get,
          replaceWithExpressionTransformer(e.child, attributeSeq), e)
      case a: Alias =>
        BackendsApiManager.getSparkPlanExecApiInstance.genAliasTransformer(
          substraitExprName.get,
          replaceWithExpressionTransformer(a.child, attributeSeq),
          a)
      case a: AttributeReference =>
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
            a.name, b.ordinal, a.dataType, b.nullable, a.exprId, a.qualifier, a.metadata)
        }
      case b: BoundReference =>
        new BoundReferenceTransformer(b.ordinal, b.dataType, b.nullable)
      case l: Literal =>
        new LiteralTransformer(l)
      case i: If =>
        new IfTransformer(
          replaceWithExpressionTransformer(
            i.predicate,
            attributeSeq),
          replaceWithExpressionTransformer(
            i.trueValue,
            attributeSeq),
          replaceWithExpressionTransformer(
            i.falseValue,
            attributeSeq),
          i)
      case cw: CaseWhen =>
        new CaseWhenTransformer(
          cw.branches.map { expr => {
            (
              replaceWithExpressionTransformer(
                expr._1,
                attributeSeq),
              replaceWithExpressionTransformer(
                expr._2,
                attributeSeq))
          }
          },
          cw.elseValue.map { expr => {
            replaceWithExpressionTransformer(
              expr,
              attributeSeq)
          }
          },
          cw)
      case i: In =>
        new InTransformer(
          replaceWithExpressionTransformer(
            i.value,
            attributeSeq),
          i.list,
          i.value.dataType,
          i)
      case i: InSet =>
        new InSetTransformer(
          replaceWithExpressionTransformer(
            i.child,
            attributeSeq),
          i.hset,
          i.child.dataType,
          i)
      case s: org.apache.spark.sql.execution.ScalarSubquery =>
        new ScalarSubqueryTransformer(s.plan, s.exprId, s)
      case c: Cast =>
        new CastTransformer(
          replaceWithExpressionTransformer(
            c.child,
            attributeSeq),
          c.dataType,
          c.timeZoneId,
          c)
      case k: KnownFloatingPointNormalized =>
        new KnownFloatingPointNormalizedTransformer(
          replaceWithExpressionTransformer(
            k.child,
            attributeSeq),
          k)
      case n: NormalizeNaNAndZero =>
        new NormalizeNaNAndZeroTransformer(
          replaceWithExpressionTransformer(
            n.child,
            attributeSeq),
          n)
      case t: StringTrim =>
        if (t.trimStr != None) {
          // todo: to be remove and deal all these three exprs together
          //  when Velox support this argument
          throw new UnsupportedOperationException(s"not supported yet.")
        }
        new String2TrimExpressionTransformer(
          substraitExprName.get,
          replaceWithExpressionTransformer(t.srcStr, attributeSeq),
          t)
      case l: StringTrimLeft =>
        if (l.trimStr != None) {
          throw new UnsupportedOperationException(s"not supported yet.")
        }
        new String2TrimExpressionTransformer(
          substraitExprName.get,
          replaceWithExpressionTransformer(l.srcStr, attributeSeq),
          l)
      case r: StringTrimRight =>
        if (r.trimStr != None) {
          throw new UnsupportedOperationException(s"not supported yet.")
        }
        new String2TrimExpressionTransformer(
          substraitExprName.get,
          replaceWithExpressionTransformer(r.srcStr, attributeSeq),
          r)
      case m: HashExpression[_] =>
        new HashExpressionTransformer(
          substraitExprName.get,
          m.children.map { expr =>
            replaceWithExpressionTransformer(
              expr,
              attributeSeq)
          },
          m)
      case complex: ComplexTypeMergingExpression =>
        ComplexTypeMergingExpressionTransformer(
          substraitExprName.get,
          complex.children.map(replaceWithExpressionTransformer(_, attributeSeq)),
          complex)
      // Extract date
      case g: GetDateField =>
        new ExtractDateTransformer(
          substraitExprName.get,
          replaceWithExpressionTransformer(
            g.child,
            attributeSeq),
          g)
      case l: LeafExpression =>
        LeafExpressionTransformer(substraitExprName.get, l)
      case u: UnaryExpression =>
        UnaryExpressionTransformer(
          substraitExprName.get,
          replaceWithExpressionTransformer(
            u.child,
            attributeSeq),
          u)
      case b: BinaryExpression =>
        BinaryExpressionTransformer(
          substraitExprName.get,
          replaceWithExpressionTransformer(
            b.left,
            attributeSeq),
          replaceWithExpressionTransformer(
            b.right,
            attributeSeq),
          b)
      case t: TernaryExpression =>
        TernaryExpressionTransformer(
          substraitExprName.get,
          replaceWithExpressionTransformer(
            t.first,
            attributeSeq),
          replaceWithExpressionTransformer(
            t.second,
            attributeSeq),
          replaceWithExpressionTransformer(
            t.third,
            attributeSeq),
          t)
      case q: QuaternaryExpression =>
        QuaternaryExpressionTransformer(
          substraitExprName.get,
          replaceWithExpressionTransformer(
            q.first,
            attributeSeq),
          replaceWithExpressionTransformer(
            q.second,
            attributeSeq),
          replaceWithExpressionTransformer(
            q.third,
            attributeSeq),
          replaceWithExpressionTransformer(
            q.fourth,
            attributeSeq),
          q)
      case expr =>
        logWarning(s"${expr.getClass} or ${expr} is not currently supported.")
        throw new UnsupportedOperationException(
          s"${expr.getClass} or ${expr} is not currently supported.")
    }
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

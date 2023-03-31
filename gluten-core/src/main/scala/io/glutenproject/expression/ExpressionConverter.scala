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

import scala.util.control.Breaks.{break, breakable}

import io.glutenproject.GlutenConfig
import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.execution.{GlutenColumnarToRowExecBase, WholeStageTransformerExec}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{BinaryArithmetic, _}
import org.apache.spark.sql.catalyst.optimizer.NormalizeNaNAndZero
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{Decimal, DecimalType}

object ExpressionConverter extends SQLConfHelper with Logging {

  /**
   * remove the test when child is PromotePrecision and PromotePrecision is Cast(Decimal, Decimal)
   *
   * @param arithmeticExpr BinaryArithmetic left or right
   * @return expression removed child PromotePrecision->Cast
   */
  private def removeCastForDecimal(arithmeticExpr: Expression): Expression = {
    arithmeticExpr match {
      case precision: PromotePrecision =>
        precision.child match {
          case cast: Cast if cast.dataType.isInstanceOf[DecimalType]
            && cast.child.dataType.isInstanceOf[DecimalType] =>
            cast.child
          case _ => arithmeticExpr
        }
      case _ => arithmeticExpr
    }
  }

  // For decimal * 10 case, dec will be Decimal(38, 18), then the result precision is wrong,
  // so here we will get the real precision and scale of the literal
  private def getNewPrecisionScale(dec: Decimal): (Integer, Integer) = {
    val input = dec.abs.toString()
    val arr = input.toCharArray
    val dotIndex = input.indexOf(".")

    // remove all of the end 0
    var newScale = arr.length - dotIndex - 1
    breakable {
      for (i <- (dotIndex + 1 until arr.length).reverse) {
        if (arr(i) != 0) {
          newScale = arr.length - dotIndex - 1 - i
          break
        }
      }
    }
    (dotIndex + newScale, newScale)
  }

  // change the precision and scale to literal actual precision and scale, otherwise the result
  // precision loss
  private def rescaleLiteral(arithmeticExpr: BinaryArithmetic): Unit = {
    if (arithmeticExpr.left.isInstanceOf[PromotePrecision]
      && arithmeticExpr.right.isInstanceOf[Literal]) {
      val lit = arithmeticExpr.right.asInstanceOf[Literal]
      if (lit.value.isInstanceOf[Decimal]) {
        val decLit = lit.asInstanceOf[Decimal]
        val (precision, scale) = getNewPrecisionScale(decLit)
        val overflow = decLit.changePrecision(precision, scale)
        if (overflow) {
          throw new RuntimeException(s"Failed to change ${decLit} to " +
            s"new precision ${precision} and scale ${scale}")
        }
      }
    } else if (arithmeticExpr.right.isInstanceOf[PromotePrecision]
      && arithmeticExpr.left.isInstanceOf[Literal]) {
      val lit = arithmeticExpr.left.asInstanceOf[Literal]
      if (lit.value.isInstanceOf[Decimal]) {
        val decLit = lit.asInstanceOf[Decimal]
        val (precision, scale) = getNewPrecisionScale(decLit)
        val overflow = decLit.changePrecision(precision, scale)
        if (overflow) {
          throw new RuntimeException(s"Failed to change ${decLit} to " +
            s"new precision ${precision} and scale ${scale}")
        }
      }
    }
  }

  // If casting between DecimalType, unnecessary cast is skipped to avoid data loss,
  // because argument input type of "cast" is actually the res type of "+-*/".
  // Cast will use a wider input type, then calculated a less scale result type than vanilla spark
  private def isDecimalArithmetic(b: BinaryArithmetic): Boolean = {
    if (b.left.dataType.isInstanceOf[DecimalType]
      && b.right.dataType.isInstanceOf[DecimalType]) {
      b match {
        case _: Divide => true
        case _: Multiply => true
        case _: Add => true
        case _: Subtract => true
        case _: Remainder => true
        case _: Pmod => true
        case _ => false
      }
    } else false
  }

  def replaceWithExpressionTransformer(expr: Expression,
      attributeSeq: Seq[Attribute]): ExpressionTransformer = {
    // Check whether Gluten supports this expression
    val substraitExprName = ExpressionMappings.scalar_functions_map.getOrElse(expr.getClass,
      ExpressionMappings.getScalarSigOther.getOrElse(expr.prettyName, ""))
    if (substraitExprName.isEmpty) {
      throw new UnsupportedOperationException(s"Not supported: $expr. ${expr.getClass}")
    }
    // Check whether each backend supports this expression
    if (GlutenConfig.getConf.enableAnsiMode ||
        !BackendsApiManager.getValidatorApiInstance.doExprValidate(substraitExprName, expr)) {
      throw new UnsupportedOperationException(s"Not supported: $expr.")
    }
    expr match {
      case c: CreateArray =>
        val children = c.children.map(child =>
          replaceWithExpressionTransformer(child, attributeSeq))
        new CreateArrayTransformer(substraitExprName, children, true, c)
      case g: GetArrayItem =>
        new GetArrayItemTransformer(
          substraitExprName,
          replaceWithExpressionTransformer(g.left, attributeSeq),
          replaceWithExpressionTransformer(g.right, attributeSeq),
          g.failOnError,
          g)
      case c: CreateMap =>
        val children = c.children.map(child =>
          replaceWithExpressionTransformer(child, attributeSeq))
        new CreateMapTransformer(substraitExprName, children, c.useStringTypeWhenEmpty, c)
      case g: GetMapValue =>
        new GetMapValueTransformer(
          substraitExprName,
          replaceWithExpressionTransformer(g.child, attributeSeq),
          replaceWithExpressionTransformer(g.key, attributeSeq),
          g.failOnError,
          g)
      case e: Explode =>
        new ExplodeTransformer(substraitExprName,
          replaceWithExpressionTransformer(e.child, attributeSeq), e)
      case a: Alias =>
        BackendsApiManager.getSparkPlanExecApiInstance.genAliasTransformer(
          substraitExprName,
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
      case f: FromUnixTime =>
        new FromUnixTimeTransformer(substraitExprName,
          replaceWithExpressionTransformer(
            f.sec,
            attributeSeq),
          replaceWithExpressionTransformer(
            f.format,
            attributeSeq),
          f.timeZoneId, f)
      case d: DateDiff =>
        new DateDiffTransformer(substraitExprName,
          replaceWithExpressionTransformer(
            d.endDate,
            attributeSeq),
          replaceWithExpressionTransformer(
            d.startDate,
            attributeSeq),
          d)
      case t: ToUnixTimestamp =>
        new ToUnixTimestampTransformer(substraitExprName,
          replaceWithExpressionTransformer(t.timeExp, attributeSeq),
          replaceWithExpressionTransformer(t.format, attributeSeq),
          t.timeZoneId,
          t.failOnError,
          t)
      case u: UnixTimestamp =>
        new UnixTimestampTransformer(substraitExprName,
          replaceWithExpressionTransformer(u.timeExp, attributeSeq),
          replaceWithExpressionTransformer(u.format, attributeSeq),
          u.timeZoneId,
          u.failOnError,
          u)
      case r: RegExpReplace =>
        new RegExpReplaceTransformer(substraitExprName,
          replaceWithExpressionTransformer(r.subject, attributeSeq),
          replaceWithExpressionTransformer(r.regexp, attributeSeq),
          replaceWithExpressionTransformer(r.rep, attributeSeq),
          replaceWithExpressionTransformer(r.pos, attributeSeq),
          r
        )
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
        new String2TrimExpressionTransformer(
          substraitExprName,
          t.trimStr.map(replaceWithExpressionTransformer(_, attributeSeq)),
          replaceWithExpressionTransformer(t.srcStr, attributeSeq),
          t)
      case l: StringTrimLeft =>
        new String2TrimExpressionTransformer(
          substraitExprName,
          l.trimStr.map(replaceWithExpressionTransformer(_, attributeSeq)),
          replaceWithExpressionTransformer(l.srcStr, attributeSeq),
          l)
      case r: StringTrimRight =>
        new String2TrimExpressionTransformer(
          substraitExprName,
          r.trimStr.map(replaceWithExpressionTransformer(_, attributeSeq)),
          replaceWithExpressionTransformer(r.srcStr, attributeSeq),
          r)
      case m: HashExpression[_] =>
        new HashExpressionTransformer(
          substraitExprName,
          m.children.map { expr =>
            replaceWithExpressionTransformer(
              expr,
              attributeSeq)
          },
          m)
      case complex: ComplexTypeMergingExpression =>
        ComplexTypeMergingExpressionTransformer(
          substraitExprName,
          complex.children.map(replaceWithExpressionTransformer(_, attributeSeq)),
          complex)
      case getStructField: GetStructField =>
        // Different backends may have different result.
        BackendsApiManager.getSparkPlanExecApiInstance.
          genGetStructFieldTransformer(substraitExprName,
            replaceWithExpressionTransformer(getStructField.child, attributeSeq),
            getStructField.ordinal,
            getStructField)
      case md5: Md5 =>
        Md5Transformer(substraitExprName,
          replaceWithExpressionTransformer(md5.child, attributeSeq), md5)
      case t: StringTranslate =>
        StringTranslateTransformer(substraitExprName,
          replaceWithExpressionTransformer(t.srcExpr, attributeSeq),
          replaceWithExpressionTransformer(t.matchingExpr, attributeSeq),
          replaceWithExpressionTransformer(t.replaceExpr, attributeSeq), t)
      case locate: StringLocate =>
        StringLocateTransformer(substraitExprName,
          replaceWithExpressionTransformer(locate.substr, attributeSeq),
          replaceWithExpressionTransformer(locate.str, attributeSeq),
          replaceWithExpressionTransformer(locate.start, attributeSeq),
          locate)
      case l: LeafExpression =>
        LeafExpressionTransformer(substraitExprName, l)
      case u: UnaryExpression =>
        UnaryExpressionTransformer(
          substraitExprName,
          replaceWithExpressionTransformer(
            u.child,
            attributeSeq),
          u)
      case b: BinaryExpression =>
        b match {
          case arithmetic: BinaryArithmetic if isDecimalArithmetic(arithmetic)
            && !conf.decimalOperationsAllowPrecisionLoss =>
            throw new UnsupportedOperationException(
              s"Not support ${SQLConf.DECIMAL_OPERATIONS_ALLOW_PREC_LOSS.key} false mode")
          case arith: BinaryArithmetic if isDecimalArithmetic(arith) =>
            rescaleLiteral(arith)
          case _ =>
        }
        val newLeft = b match {
          case arithmetic: BinaryArithmetic if isDecimalArithmetic(arithmetic) =>
            removeCastForDecimal(b.left)
          case _ => b.left
        }
        val newRight = b match {
          case arithmetic: BinaryArithmetic if isDecimalArithmetic(arithmetic) =>
            removeCastForDecimal(b.right)
          case _ => b.right
        }

        BinaryExpressionTransformer(
          substraitExprName,
          replaceWithExpressionTransformer(
            newLeft,
            attributeSeq),
          replaceWithExpressionTransformer(
            newRight,
            attributeSeq),
          b)
      case t: TernaryExpression =>
        TernaryExpressionTransformer(
          substraitExprName,
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
          substraitExprName,
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
      case namedStruct: CreateNamedStruct =>
        var childrenTransformers = Seq[ExpressionTransformer]()
        namedStruct.children.foreach(
          child => childrenTransformers = childrenTransformers :+
            replaceWithExpressionTransformer(child, attributeSeq)
        )
        new NamedStructTransformer(substraitExprName, childrenTransformers, namedStruct)
      case element_at: ElementAt =>
        new BinaryArgumentsCollectionOperationTransformer(substraitExprName,
          left = replaceWithExpressionTransformer(element_at.left, attributeSeq),
          right = replaceWithExpressionTransformer(element_at.right, attributeSeq),
          element_at)
      case arrayContains: ArrayContains =>
        new BinaryArgumentsCollectionOperationTransformer(substraitExprName,
          left = replaceWithExpressionTransformer(arrayContains.left, attributeSeq),
          right = replaceWithExpressionTransformer(arrayContains.right, attributeSeq),
          arrayContains)
      case arrayMax: ArrayMax =>
        new UnaryArgumentCollectionOperationTransformer(substraitExprName,
          replaceWithExpressionTransformer(arrayMax.child, attributeSeq), arrayMax)
      case arrayMin: ArrayMin =>
        new UnaryArgumentCollectionOperationTransformer(substraitExprName,
          replaceWithExpressionTransformer(arrayMin.child, attributeSeq), arrayMin)
      case mapKeys: MapKeys =>
        new UnaryArgumentCollectionOperationTransformer(substraitExprName,
          replaceWithExpressionTransformer(mapKeys.child, attributeSeq), mapKeys)
      case mapValues: MapValues =>
        new UnaryArgumentCollectionOperationTransformer(substraitExprName,
          replaceWithExpressionTransformer(mapValues.child, attributeSeq), mapValues)
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

    if (GlutenConfig.getConf.enableScanOnly) {
      // Disable ColumnarSubqueryBroadcast for scan-only execution.
      partitionFilters
    } else {
      partitionFilters.map {
        case dynamicPruning: DynamicPruningExpression =>
          dynamicPruning.transform {
            // Lookup inside subqueries for duplicate exchanges.
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
}

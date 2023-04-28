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

import io.glutenproject.GlutenConfig
import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.execution.{GlutenColumnarToRowExecBase, WholeStageTransformerExec}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.analysis.DecimalPrecision
import org.apache.spark.sql.catalyst.expressions.{BinaryArithmetic, _}
import org.apache.spark.sql.catalyst.optimizer.NormalizeNaNAndZero
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ByteType, Decimal, DecimalType, IntegerType, LongType, ShortType}

object ExpressionConverter extends SQLConfHelper with Logging {

  /**
   * Remove the Cast when child is PromotePrecision and PromotePrecision is Cast(Decimal, Decimal)
   *
   * @param arithmeticExpr
   *   BinaryArithmetic left or right
   * @return
   *   expression removed child PromotePrecision->Cast
   */
  private def removeCastForDecimal(arithmeticExpr: Expression): Expression = {
    arithmeticExpr match {
      case precision: PromotePrecision =>
        precision.child match {
          case cast: Cast
              if cast.dataType.isInstanceOf[DecimalType]
                && cast.child.dataType.isInstanceOf[DecimalType] =>
            cast.child
          case _ => arithmeticExpr
        }
      case _ => arithmeticExpr
    }
  }

  private def isPromoteCastIntegral(expr: Expression): Boolean = {
    expr match {
      case precision: PromotePrecision =>
        precision.child match {
          case cast: Cast if cast.dataType.isInstanceOf[DecimalType] =>
            cast.child.dataType match {
              case IntegerType | ByteType | ShortType | LongType => true
              case _ => false
            }
          case _ => false
        }
      case _ => false
    }
  }

  private def isPromoteCast(expr: Expression): Boolean = {
    expr match {
      case precision: PromotePrecision =>
        precision.child match {
          case cast: Cast if cast.dataType.isInstanceOf[DecimalType] => true
          case _ => false
        }
      case _ => false
    }
  }

  private def rescaleCastForOneSide(expr: Expression): Expression = {
    expr match {
      case precision: PromotePrecision =>
        precision.child match {
          case castInt: Cast
              if castInt.dataType.isInstanceOf[DecimalType] &&
                BackendsApiManager.getSettings.rescaleDecimalIntegralExpression() =>
            castInt.child.dataType match {
              case IntegerType | ByteType | ShortType =>
                precision.withNewChildren(Seq(Cast(castInt.child, DecimalType(10, 0))))
              case LongType =>
                precision.withNewChildren(Seq(Cast(castInt.child, DecimalType(20, 0))))
              case _ => expr
            }
          case _ => expr
        }
      case _ => expr
    }
  }

  private def checkIsWiderType(
      left: DecimalType,
      right: DecimalType,
      wider: DecimalType): Boolean = {
    val widerType = DecimalPrecision.widerDecimalType(left, right)
    widerType.equals(wider)
  }

  private def rescaleCastForDecimal(
      left: Expression,
      right: Expression): (Expression, Expression) = {
    if (!BackendsApiManager.getSettings.rescaleDecimalIntegralExpression()) {
      return (left, right)
    }
    // decimal * cast int
    if (!isPromoteCast(left)) { // have removed PromotePrecision(Cast(DecimalType))
      if (isPromoteCastIntegral(right)) {
        val newRight = rescaleCastForOneSide(right)
        val isWiderType = checkIsWiderType(
          left.dataType.asInstanceOf[DecimalType],
          newRight.dataType.asInstanceOf[DecimalType],
          right.dataType.asInstanceOf[DecimalType])
        if (isWiderType) {
          (left, newRight)
        } else {
          (left, right)
        }
      } else {
        (left, right)
      }
      // cast int * decimal
    } else if (!isPromoteCast(right)) {
      if (isPromoteCastIntegral(left)) {
        val newLeft = rescaleCastForOneSide(left)
        val isWiderType = checkIsWiderType(
          newLeft.dataType.asInstanceOf[DecimalType],
          right.dataType.asInstanceOf[DecimalType],
          left.dataType.asInstanceOf[DecimalType])
        if (isWiderType) {
          (newLeft, right)
        } else {
          (left, right)
        }
      } else {
        (left, right)
      }
    } else {
      // cast int * cast int, usually user defined cast
      (left, right)
    }
  }

  // For decimal * 10 case, dec will be Decimal(38, 18), then the result precision is wrong,
  // so here we will get the real precision and scale of the literal
  private def getNewPrecisionScale(dec: Decimal): (Integer, Integer) = {
    val input = dec.abs.toString()
    val dotIndex = input.indexOf(".")
    if (dotIndex == -1) {
      return (input.length, 0)
    }

    if (dec.toBigDecimal.isValidLong) {
      return (dotIndex, 0)
    }

    (dec.precision, dec.scale)
  }

  // change the precision and scale to literal actual precision and scale, otherwise the result
  // precision loss
  private def rescaleLiteral(arithmeticExpr: BinaryArithmetic): BinaryArithmetic = {
    if (
      arithmeticExpr.left.isInstanceOf[PromotePrecision]
      && arithmeticExpr.right.isInstanceOf[Literal]
    ) {
      val lit = arithmeticExpr.right.asInstanceOf[Literal]
      lit.value match {
        case decLit: Decimal =>
          val (precision, scale) = getNewPrecisionScale(decLit)
          if (precision != decLit.precision || scale != decLit.scale) {
            arithmeticExpr
              .withNewChildren(Seq(arithmeticExpr.left, Cast(lit, DecimalType(precision, scale))))
              .asInstanceOf[BinaryArithmetic]
          } else arithmeticExpr
        case _ => arithmeticExpr
      }
    } else if (
      arithmeticExpr.right.isInstanceOf[PromotePrecision]
      && arithmeticExpr.left.isInstanceOf[Literal]
    ) {
      val lit = arithmeticExpr.left.asInstanceOf[Literal]
      lit.value match {
        case decLit: Decimal =>
          val (precision, scale) = getNewPrecisionScale(decLit)
          if (precision != decLit.precision || scale != decLit.scale) {
            arithmeticExpr
              .withNewChildren(Seq(Cast(lit, DecimalType(precision, scale)), arithmeticExpr.right))
              .asInstanceOf[BinaryArithmetic]
          } else arithmeticExpr
        case _ => arithmeticExpr
      }
    } else {
      arithmeticExpr
    }
  }

  // If casting between DecimalType, unnecessary cast is skipped to avoid data loss,
  // because argument input type of "cast" is actually the res type of "+-*/".
  // Cast will use a wider input type, then calculated a less scale result type than vanilla spark
  private def isDecimalArithmetic(b: BinaryArithmetic): Boolean = {
    if (
      b.left.dataType.isInstanceOf[DecimalType]
      && b.right.dataType.isInstanceOf[DecimalType]
    ) {
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

  def replaceWithExpressionTransformer(
      expr: Expression,
      attributeSeq: Seq[Attribute]): ExpressionTransformer = {
    // Check whether Gluten supports this expression
    val substraitExprName = ExpressionMappings.scalar_functions_map.getOrElse(
      expr.getClass,
      ExpressionMappings.getScalarSigOther.getOrElse(expr.prettyName, ""))
    if (substraitExprName.isEmpty) {
      throw new UnsupportedOperationException(s"Not supported: $expr. ${expr.getClass}")
    }
    // Check whether each backend supports this expression
    if (
      GlutenConfig.getConf.enableAnsiMode ||
      !BackendsApiManager.getValidatorApiInstance.doExprValidate(substraitExprName, expr)
    ) {
      throw new UnsupportedOperationException(s"Not supported: $expr.")
    }
    expr match {
      case c: CreateArray =>
        val children =
          c.children.map(child => replaceWithExpressionTransformer(child, attributeSeq))
        new CreateArrayTransformer(substraitExprName, children, true, c)
      case g: GetArrayItem =>
        new GetArrayItemTransformer(
          substraitExprName,
          replaceWithExpressionTransformer(g.left, attributeSeq),
          replaceWithExpressionTransformer(g.right, attributeSeq),
          g.failOnError,
          g)
      case c: CreateMap =>
        val children =
          c.children.map(child => replaceWithExpressionTransformer(child, attributeSeq))
        new CreateMapTransformer(substraitExprName, children, c.useStringTypeWhenEmpty, c)
      case g: GetMapValue =>
        new GetMapValueTransformer(
          substraitExprName,
          replaceWithExpressionTransformer(g.child, attributeSeq),
          replaceWithExpressionTransformer(g.key, attributeSeq),
          g.failOnError,
          g)
      case e: Explode =>
        new ExplodeTransformer(
          substraitExprName,
          replaceWithExpressionTransformer(e.child, attributeSeq),
          e)
      case p: PosExplode =>
        new PosExplodeTransformer(
          substraitExprName,
          replaceWithExpressionTransformer(p.child, attributeSeq),
          p,
          attributeSeq)
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
          throw new UnsupportedOperationException(s"$expr attribute binding failed.")
        } else {
          val b = bindReference.asInstanceOf[BoundReference]
          new AttributeReferenceTransformer(
            a.name,
            b.ordinal,
            a.dataType,
            b.nullable,
            a.exprId,
            a.qualifier,
            a.metadata)
        }
      case b: BoundReference =>
        new BoundReferenceTransformer(b.ordinal, b.dataType, b.nullable)
      case l: Literal =>
        new LiteralTransformer(l)
      case f: FromUnixTime =>
        new FromUnixTimeTransformer(
          substraitExprName,
          replaceWithExpressionTransformer(f.sec, attributeSeq),
          replaceWithExpressionTransformer(f.format, attributeSeq),
          f.timeZoneId,
          f)
      case d: DateDiff =>
        new DateDiffTransformer(
          substraitExprName,
          replaceWithExpressionTransformer(d.endDate, attributeSeq),
          replaceWithExpressionTransformer(d.startDate, attributeSeq),
          d)
      case t: ToUnixTimestamp =>
        new ToUnixTimestampTransformer(
          substraitExprName,
          replaceWithExpressionTransformer(t.timeExp, attributeSeq),
          replaceWithExpressionTransformer(t.format, attributeSeq),
          t.timeZoneId,
          t.failOnError,
          t
        )
      case u: UnixTimestamp =>
        new UnixTimestampTransformer(
          substraitExprName,
          replaceWithExpressionTransformer(u.timeExp, attributeSeq),
          replaceWithExpressionTransformer(u.format, attributeSeq),
          u.timeZoneId,
          u.failOnError,
          u
        )
      case r: RegExpReplace =>
        new RegExpReplaceTransformer(
          substraitExprName,
          replaceWithExpressionTransformer(r.subject, attributeSeq),
          replaceWithExpressionTransformer(r.regexp, attributeSeq),
          replaceWithExpressionTransformer(r.rep, attributeSeq),
          replaceWithExpressionTransformer(r.pos, attributeSeq),
          r
        )
      case i: If =>
        new IfTransformer(
          replaceWithExpressionTransformer(i.predicate, attributeSeq),
          replaceWithExpressionTransformer(i.trueValue, attributeSeq),
          replaceWithExpressionTransformer(i.falseValue, attributeSeq),
          i
        )
      case cw: CaseWhen =>
        new CaseWhenTransformer(
          cw.branches.map {
            expr =>
              {
                (
                  replaceWithExpressionTransformer(expr._1, attributeSeq),
                  replaceWithExpressionTransformer(expr._2, attributeSeq))
              }
          },
          cw.elseValue.map {
            expr =>
              {
                replaceWithExpressionTransformer(expr, attributeSeq)
              }
          },
          cw
        )
      case i: In =>
        new InTransformer(
          replaceWithExpressionTransformer(i.value, attributeSeq),
          i.list,
          i.value.dataType,
          i)
      case i: InSet =>
        new InSetTransformer(
          replaceWithExpressionTransformer(i.child, attributeSeq),
          i.hset,
          i.child.dataType,
          i)
      case s: org.apache.spark.sql.execution.ScalarSubquery =>
        new ScalarSubqueryTransformer(s.plan, s.exprId, s)
      case c: Cast =>
        new CastTransformer(
          replaceWithExpressionTransformer(c.child, attributeSeq),
          c.dataType,
          c.timeZoneId,
          c)
      case k: KnownFloatingPointNormalized =>
        new KnownFloatingPointNormalizedTransformer(
          replaceWithExpressionTransformer(k.child, attributeSeq),
          k)
      case n: NormalizeNaNAndZero =>
        new NormalizeNaNAndZeroTransformer(
          replaceWithExpressionTransformer(n.child, attributeSeq),
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
          m.children.map(expr => replaceWithExpressionTransformer(expr, attributeSeq)),
          m)
      case complex: ComplexTypeMergingExpression =>
        ComplexTypeMergingExpressionTransformer(
          substraitExprName,
          complex.children.map(replaceWithExpressionTransformer(_, attributeSeq)),
          complex)
      case getStructField: GetStructField =>
        // Different backends may have different result.
        BackendsApiManager.getSparkPlanExecApiInstance.genGetStructFieldTransformer(
          substraitExprName,
          replaceWithExpressionTransformer(getStructField.child, attributeSeq),
          getStructField.ordinal,
          getStructField)
      case md5: Md5 =>
        Md5Transformer(
          substraitExprName,
          replaceWithExpressionTransformer(md5.child, attributeSeq),
          md5)
      case t: StringTranslate =>
        StringTranslateTransformer(
          substraitExprName,
          replaceWithExpressionTransformer(t.srcExpr, attributeSeq),
          replaceWithExpressionTransformer(t.matchingExpr, attributeSeq),
          replaceWithExpressionTransformer(t.replaceExpr, attributeSeq),
          t
        )
      case locate: StringLocate =>
        StringLocateTransformer(
          substraitExprName,
          replaceWithExpressionTransformer(locate.substr, attributeSeq),
          replaceWithExpressionTransformer(locate.str, attributeSeq),
          replaceWithExpressionTransformer(locate.start, attributeSeq),
          locate
        )
      case sha1: Sha1 =>
        BackendsApiManager.getSparkPlanExecApiInstance.genSha1Transformer(
          substraitExprName,
          replaceWithExpressionTransformer(sha1.child, attributeSeq),
          sha1)
      case sha2: Sha2 =>
        BackendsApiManager.getSparkPlanExecApiInstance.genSha2Transformer(
          substraitExprName,
          replaceWithExpressionTransformer(sha2.left, attributeSeq),
          replaceWithExpressionTransformer(sha2.right, attributeSeq),
          sha2)
      case l: LeafExpression =>
        LeafExpressionTransformer(substraitExprName, l)
      case u: UnaryExpression =>
        UnaryExpressionTransformer(
          substraitExprName,
          replaceWithExpressionTransformer(u.child, attributeSeq),
          u)
      case b: BinaryExpression =>
        val idDecimalArithmetic = b.isInstanceOf[BinaryArithmetic] &&
          isDecimalArithmetic(b.asInstanceOf[BinaryArithmetic])
        val (newLeft1, newRight1) = if (idDecimalArithmetic) {
          val rescaleBinary = b match {
            case _: BinaryArithmetic if !conf.decimalOperationsAllowPrecisionLoss =>
              throw new UnsupportedOperationException(
                s"Not support ${SQLConf.DECIMAL_OPERATIONS_ALLOW_PREC_LOSS.key} false mode")
            case arith: BinaryArithmetic
                if BackendsApiManager.getSettings.rescaleDecimalLiteral() =>
              rescaleLiteral(arith)
            case _ => b
          }
          rescaleCastForDecimal(
            removeCastForDecimal(rescaleBinary.left),
            removeCastForDecimal(rescaleBinary.right))
        } else {
          (b.left, b.right)
        }

        BinaryExpressionTransformer(
          substraitExprName,
          replaceWithExpressionTransformer(newLeft1, attributeSeq),
          replaceWithExpressionTransformer(newRight1, attributeSeq),
          b)
      case t: TernaryExpression =>
        TernaryExpressionTransformer(
          substraitExprName,
          replaceWithExpressionTransformer(t.first, attributeSeq),
          replaceWithExpressionTransformer(t.second, attributeSeq),
          replaceWithExpressionTransformer(t.third, attributeSeq),
          t
        )
      case q: QuaternaryExpression =>
        QuaternaryExpressionTransformer(
          substraitExprName,
          replaceWithExpressionTransformer(q.first, attributeSeq),
          replaceWithExpressionTransformer(q.second, attributeSeq),
          replaceWithExpressionTransformer(q.third, attributeSeq),
          replaceWithExpressionTransformer(q.fourth, attributeSeq),
          q
        )
      case namedStruct: CreateNamedStruct =>
        BackendsApiManager.getSparkPlanExecApiInstance
          .genNamedStructTransformer(substraitExprName, namedStruct, attributeSeq)
      case element_at: ElementAt =>
        new BinaryArgumentsCollectionOperationTransformer(
          substraitExprName,
          left = replaceWithExpressionTransformer(element_at.left, attributeSeq),
          right = replaceWithExpressionTransformer(element_at.right, attributeSeq),
          element_at
        )
      case arrayContains: ArrayContains =>
        new BinaryArgumentsCollectionOperationTransformer(
          substraitExprName,
          left = replaceWithExpressionTransformer(arrayContains.left, attributeSeq),
          right = replaceWithExpressionTransformer(arrayContains.right, attributeSeq),
          arrayContains
        )
      case arrayMax: ArrayMax =>
        new UnaryArgumentCollectionOperationTransformer(
          substraitExprName,
          replaceWithExpressionTransformer(arrayMax.child, attributeSeq),
          arrayMax)
      case arrayMin: ArrayMin =>
        new UnaryArgumentCollectionOperationTransformer(
          substraitExprName,
          replaceWithExpressionTransformer(arrayMin.child, attributeSeq),
          arrayMin)
      case mapKeys: MapKeys =>
        new UnaryArgumentCollectionOperationTransformer(
          substraitExprName,
          replaceWithExpressionTransformer(mapKeys.child, attributeSeq),
          mapKeys)
      case mapValues: MapValues =>
        new UnaryArgumentCollectionOperationTransformer(
          substraitExprName,
          replaceWithExpressionTransformer(mapValues.child, attributeSeq),
          mapValues)
      case seq: Sequence =>
        new SequenceTransformer(
          substraitExprName,
          replaceWithExpressionTransformer(seq.start, attributeSeq),
          replaceWithExpressionTransformer(seq.stop, attributeSeq),
          seq.stepOpt.map(replaceWithExpressionTransformer(_, attributeSeq)),
          seq
        )
      case expr =>
        logWarning(s"${expr.getClass} or $expr is not currently supported.")
        throw new UnsupportedOperationException(
          s"${expr.getClass} or $expr is not currently supported.")
    }
  }

  /**
   * Transform BroadcastExchangeExec to ColumnarBroadcastExchangeExec in DynamicPruningExpression.
   *
   * @param partitionFilters
   * @return
   */
  def transformDynamicPruningExpr(partitionFilters: Seq[Expression]): Seq[Expression] = {

    def convertBroadcastExchangeToColumnar(
        exchange: BroadcastExchangeExec): ColumnarBroadcastExchangeExec = {
      val newChild = exchange.child match {
        // get WholeStageTransformerExec directly
        case c2r: GlutenColumnarToRowExecBase => c2r.child
        // in case of fallbacking
        case codeGen: WholeStageCodegenExec =>
          if (codeGen.child.isInstanceOf[ColumnarToRowExec]) {
            val wholeStageTransformerExec = exchange.find(_.isInstanceOf[WholeStageTransformerExec])
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
            case in: InSubqueryExec =>
              in.plan match {
                case _: SubqueryBroadcastExec =>
                  val newIn = in.plan
                    .transform {
                      case exchange: BroadcastExchangeExec =>
                        convertBroadcastExchangeToColumnar(exchange)
                    }
                    .asInstanceOf[SubqueryBroadcastExec]
                  val transformSubqueryBroadcast = ColumnarSubqueryBroadcastExec(
                    newIn.name,
                    newIn.index,
                    newIn.buildKeys,
                    newIn.child)
                  in.copy(plan = transformSubqueryBroadcast.asInstanceOf[BaseSubqueryExec])
                case _: ReusedSubqueryExec if in.plan.child.isInstanceOf[SubqueryBroadcastExec] =>
                  val newIn = in.plan.child
                    .transform {
                      case exchange: BroadcastExchangeExec =>
                        convertBroadcastExchangeToColumnar(exchange)
                    }
                    .asInstanceOf[SubqueryBroadcastExec]
                  val transformSubqueryBroadcast = ColumnarSubqueryBroadcastExec(
                    newIn.name,
                    newIn.index,
                    newIn.buildKeys,
                    newIn.child)
                  in.copy(plan = ReusedSubqueryExec(transformSubqueryBroadcast))
                case _ => in
              }
          }
        case e: Expression => e
      }
    }
  }
}

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
import io.glutenproject.execution.{ColumnarToRowExecBase, WholeStageTransformer}
import io.glutenproject.test.TestStats

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.analysis.DecimalPrecision
import org.apache.spark.sql.catalyst.expressions.{BinaryArithmetic, _}
import org.apache.spark.sql.catalyst.optimizer.NormalizeNaNAndZero
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.spark.sql.hive.HiveSimpleUDFTransformer
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

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

  def replacePythonUDFWithExpressionTransformer(
      udf: PythonUDF,
      attributeSeq: Seq[Attribute]): ExpressionTransformer = {
    logDebug(s"replacePythonUDFWithExpressionTransformer udf name: ${udf.name}")
    val substraitExprName = UDFMappings.pythonUDFMap.get(udf.name)
    substraitExprName match {
      case Some(name) =>
        PythonUDFTransformer(
          name,
          udf.children.map(replaceWithExpressionTransformer(_, attributeSeq)),
          udf)
      case None =>
        throw new UnsupportedOperationException(s"Not supported python udf: $udf.")
    }
  }

  def replaceScalaUDFWithExpressionTransformer(
      udf: ScalaUDF,
      attributeSeq: Seq[Attribute]): ExpressionTransformer = {
    val substraitExprName = UDFMappings.scalaUDFMap.get(udf.udfName.get)
    substraitExprName match {
      case Some(name) =>
        ScalaUDFTransformer(
          name,
          udf.children.map(replaceWithExpressionTransformer(_, attributeSeq)),
          udf)
      case None =>
        throw new UnsupportedOperationException(s"Not supported scala udf: $udf.")
    }
  }

  def replaceWithExpressionTransformer(
      expr: Expression,
      attributeSeq: Seq[Attribute]): ExpressionTransformer = {
    logDebug(
      s"replaceWithExpressionTransformer expr: $expr class: ${expr.getClass}} " +
        s"name: ${expr.prettyName}")

    if (expr.isInstanceOf[PythonUDF]) {
      return replacePythonUDFWithExpressionTransformer(expr.asInstanceOf[PythonUDF], attributeSeq)
    }

    if (expr.isInstanceOf[ScalaUDF]) {
      return replaceScalaUDFWithExpressionTransformer(expr.asInstanceOf[ScalaUDF], attributeSeq)
    }

    if (HiveSimpleUDFTransformer.isHiveSimpleUDF(expr)) {
      return HiveSimpleUDFTransformer.replaceWithExpressionTransformer(expr, attributeSeq)
    }

    TestStats.addExpressionClassName(expr.getClass.getName)
    // Check whether Gluten supports this expression
    val substraitExprName = ExpressionMappings.expressionsMap.get(expr.getClass)
    if (substraitExprName.isEmpty) {
      throw new UnsupportedOperationException(s"Not supported: $expr. ${expr.getClass}")
    }

    // Check whether each backend supports this expression
    if (
      GlutenConfig.getConf.enableAnsiMode ||
      !BackendsApiManager.getValidatorApiInstance.doExprValidate(substraitExprName.get, expr)
    ) {
      throw new UnsupportedOperationException(s"Not supported: $expr.")
    }
    expr match {
      case extendedExpr
        if ExpressionMappings.expressionExtensionTransformer
          .extensionExpressionsMapping.contains(extendedExpr.getClass) =>
        // Use extended expression transformer to replace custom expression first
        ExpressionMappings.expressionExtensionTransformer
          .replaceWithExtensionExpressionTransformer(
            substraitExprName.get,
            extendedExpr,
            attributeSeq)
      case c: CreateArray =>
        val children =
          c.children.map(child => replaceWithExpressionTransformer(child, attributeSeq))
        new CreateArrayTransformer(substraitExprName.get, children, true, c)
      case g: GetArrayItem =>
        new GetArrayItemTransformer(
          substraitExprName.get,
          replaceWithExpressionTransformer(g.left, attributeSeq),
          replaceWithExpressionTransformer(g.right, attributeSeq),
          g.failOnError,
          g)
      case c: CreateMap =>
        val children =
          c.children.map(child => replaceWithExpressionTransformer(child, attributeSeq))
        new CreateMapTransformer(substraitExprName.get, children, c.useStringTypeWhenEmpty, c)
      case g: GetMapValue =>
        new GetMapValueTransformer(
          substraitExprName.get,
          replaceWithExpressionTransformer(g.child, attributeSeq),
          replaceWithExpressionTransformer(g.key, attributeSeq),
          g.failOnError,
          g)
      case e: Explode =>
        new ExplodeTransformer(
          substraitExprName.get,
          replaceWithExpressionTransformer(e.child, attributeSeq),
          e)
      case p: PosExplode =>
        new PosExplodeTransformer(
          substraitExprName.get,
          replaceWithExpressionTransformer(p.child, attributeSeq),
          p,
          attributeSeq)
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
          substraitExprName.get,
          replaceWithExpressionTransformer(f.sec, attributeSeq),
          replaceWithExpressionTransformer(f.format, attributeSeq),
          f.timeZoneId,
          f)
      case d: DateDiff =>
        new DateDiffTransformer(
          substraitExprName.get,
          replaceWithExpressionTransformer(d.endDate, attributeSeq),
          replaceWithExpressionTransformer(d.startDate, attributeSeq),
          d)
      case t: ToUnixTimestamp =>
        BackendsApiManager.getSparkPlanExecApiInstance.genUnixTimestampTransformer(
          substraitExprName.get,
          replaceWithExpressionTransformer(t.timeExp, attributeSeq),
          replaceWithExpressionTransformer(t.format, attributeSeq),
          t
        )
      case u: UnixTimestamp =>
        BackendsApiManager.getSparkPlanExecApiInstance.genUnixTimestampTransformer(
          substraitExprName.get,
          replaceWithExpressionTransformer(u.timeExp, attributeSeq),
          replaceWithExpressionTransformer(u.format, attributeSeq),
          ToUnixTimestamp(u.timeExp, u.format, u.timeZoneId, u.failOnError)
        )
      case truncTimestamp: TruncTimestamp =>
        BackendsApiManager.getSparkPlanExecApiInstance.genTruncTimestampTransformer(
          substraitExprName.get,
          replaceWithExpressionTransformer(truncTimestamp.format, attributeSeq),
          replaceWithExpressionTransformer(truncTimestamp.timestamp, attributeSeq),
          truncTimestamp.timeZoneId,
          truncTimestamp
        )
      case r: RegExpReplace =>
        new RegExpReplaceTransformer(
          substraitExprName.get,
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
        // Add trim node, as necessary.
        val newCast =
          BackendsApiManager.getSparkPlanExecApiInstance.genCastWithNewChild(c)
        new CastTransformer(
          replaceWithExpressionTransformer(newCast.child, attributeSeq),
          newCast.dataType,
          newCast.timeZoneId,
          newCast)
      case k: KnownFloatingPointNormalized =>
        new KnownFloatingPointNormalizedTransformer(
          replaceWithExpressionTransformer(k.child, attributeSeq),
          k)
      case n: NormalizeNaNAndZero =>
        new NormalizeNaNAndZeroTransformer(
          replaceWithExpressionTransformer(n.child, attributeSeq),
          n)
      case l: StringTrimLeft =>
        new String2TrimExpressionTransformer(
          substraitExprName.get,
          l.trimStr.map(replaceWithExpressionTransformer(_, attributeSeq)),
          replaceWithExpressionTransformer(l.srcStr, attributeSeq),
          l)
      case r: StringTrimRight =>
        new String2TrimExpressionTransformer(
          substraitExprName.get,
          r.trimStr.map(replaceWithExpressionTransformer(_, attributeSeq)),
          replaceWithExpressionTransformer(r.srcStr, attributeSeq),
          r)
      case t: StringTrim =>
        new String2TrimExpressionTransformer(
          substraitExprName.get,
          t.trimStr.map(replaceWithExpressionTransformer(_, attributeSeq)),
          replaceWithExpressionTransformer(t.srcStr, attributeSeq),
          t)
      case m: HashExpression[_] =>
        BackendsApiManager.getSparkPlanExecApiInstance.genHashExpressionTransformer(
          substraitExprName.get,
          m.children.map(expr => replaceWithExpressionTransformer(expr, attributeSeq)),
          m)
      case complex: ComplexTypeMergingExpression =>
        ComplexTypeMergingExpressionTransformer(
          substraitExprName.get,
          complex.children.map(replaceWithExpressionTransformer(_, attributeSeq)),
          complex)
      case getStructField: GetStructField =>
        // Different backends may have different result.
        BackendsApiManager.getSparkPlanExecApiInstance.genGetStructFieldTransformer(
          substraitExprName.get,
          replaceWithExpressionTransformer(getStructField.child, attributeSeq),
          getStructField.ordinal,
          getStructField)
      case md5: Md5 =>
        Md5Transformer(
          substraitExprName.get,
          replaceWithExpressionTransformer(md5.child, attributeSeq),
          md5)
      case t: StringTranslate =>
        StringTranslateTransformer(
          substraitExprName.get,
          replaceWithExpressionTransformer(t.srcExpr, attributeSeq),
          replaceWithExpressionTransformer(t.matchingExpr, attributeSeq),
          replaceWithExpressionTransformer(t.replaceExpr, attributeSeq),
          t
        )
      case locate: StringLocate =>
        StringLocateTransformer(
          substraitExprName.get,
          replaceWithExpressionTransformer(locate.substr, attributeSeq),
          replaceWithExpressionTransformer(locate.str, attributeSeq),
          replaceWithExpressionTransformer(locate.start, attributeSeq),
          locate
        )
      case equal: EqualNullSafe =>
        BackendsApiManager.getSparkPlanExecApiInstance.genEqualNullSafeTransformer(
          substraitExprName.get,
          replaceWithExpressionTransformer(equal.left, attributeSeq),
          replaceWithExpressionTransformer(equal.right, attributeSeq),
          equal
        )
      case sha1: Sha1 =>
        BackendsApiManager.getSparkPlanExecApiInstance.genSha1Transformer(
          substraitExprName.get,
          replaceWithExpressionTransformer(sha1.child, attributeSeq),
          sha1)
      case sha2: Sha2 =>
        BackendsApiManager.getSparkPlanExecApiInstance.genSha2Transformer(
          substraitExprName.get,
          replaceWithExpressionTransformer(sha2.left, attributeSeq),
          replaceWithExpressionTransformer(sha2.right, attributeSeq),
          sha2)
      case size: Size =>
        BackendsApiManager.getSparkPlanExecApiInstance.genSizeExpressionTransformer(
          substraitExprName.get,
          replaceWithExpressionTransformer(size.child, attributeSeq),
          size)
      case namedStruct: CreateNamedStruct =>
        BackendsApiManager.getSparkPlanExecApiInstance
          .genNamedStructTransformer(substraitExprName.get, namedStruct, attributeSeq)
      case elementAt: ElementAt =>
        new BinaryArgumentsCollectionOperationTransformer(
          substraitExprName.get,
          left = replaceWithExpressionTransformer(elementAt.left, attributeSeq),
          right = replaceWithExpressionTransformer(elementAt.right, attributeSeq),
          elementAt
        )
      case arrayContains: ArrayContains =>
        new BinaryArgumentsCollectionOperationTransformer(
          substraitExprName.get,
          left = replaceWithExpressionTransformer(arrayContains.left, attributeSeq),
          right = replaceWithExpressionTransformer(arrayContains.right, attributeSeq),
          arrayContains
        )
      case arrayMax: ArrayMax =>
        new UnaryArgumentCollectionOperationTransformer(
          substraitExprName.get,
          replaceWithExpressionTransformer(arrayMax.child, attributeSeq),
          arrayMax)
      case arrayMin: ArrayMin =>
        new UnaryArgumentCollectionOperationTransformer(
          substraitExprName.get,
          replaceWithExpressionTransformer(arrayMin.child, attributeSeq),
          arrayMin)
      case mapKeys: MapKeys =>
        new UnaryArgumentCollectionOperationTransformer(
          substraitExprName.get,
          replaceWithExpressionTransformer(mapKeys.child, attributeSeq),
          mapKeys)
      case mapValues: MapValues =>
        new UnaryArgumentCollectionOperationTransformer(
          substraitExprName.get,
          replaceWithExpressionTransformer(mapValues.child, attributeSeq),
          mapValues)
      case seq: Sequence =>
        new SequenceTransformer(
          substraitExprName.get,
          replaceWithExpressionTransformer(seq.start, attributeSeq),
          replaceWithExpressionTransformer(seq.stop, attributeSeq),
          seq.stepOpt.map(replaceWithExpressionTransformer(_, attributeSeq)),
          seq
        )
      case j: JsonTuple =>
        val children = j.children.map(child =>
          replaceWithExpressionTransformer(child, attributeSeq))
        JsonTupleExpressionTransformer(substraitExprName.get, children.toArray, j)
      // The other expression case must be put before LeafExpression, UnaryExpression,
      // BinaryExpression, TernaryExpression, QuaternaryExpression
      case l: LeafExpression =>
        LeafExpressionTransformer(substraitExprName.get, l)
      case u: UnaryExpression =>
        UnaryExpressionTransformer(
          substraitExprName.get,
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
          substraitExprName.get,
          replaceWithExpressionTransformer(newLeft1, attributeSeq),
          replaceWithExpressionTransformer(newRight1, attributeSeq),
          b)
      case t: TernaryExpression =>
        TernaryExpressionTransformer(
          substraitExprName.get,
          replaceWithExpressionTransformer(t.first, attributeSeq),
          replaceWithExpressionTransformer(t.second, attributeSeq),
          replaceWithExpressionTransformer(t.third, attributeSeq),
          t
        )
      case q: QuaternaryExpression =>
        QuaternaryExpressionTransformer(
          substraitExprName.get,
          replaceWithExpressionTransformer(q.first, attributeSeq),
          replaceWithExpressionTransformer(q.second, attributeSeq),
          replaceWithExpressionTransformer(q.third, attributeSeq),
          replaceWithExpressionTransformer(q.fourth, attributeSeq),
          q
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
  def transformDynamicPruningExpr(
    partitionFilters: Seq[Expression],
    reuseSubquery: Boolean): Seq[Expression] = {

    def convertBroadcastExchangeToColumnar(
        exchange: BroadcastExchangeExec): ColumnarBroadcastExchangeExec = {
      val newChild = exchange.child match {
        // get WholeStageTransformer directly
        case c2r: ColumnarToRowExecBase => c2r.child
        // in case of fallbacking
        case codeGen: WholeStageCodegenExec =>
          if (codeGen.child.isInstanceOf[ColumnarToRowExec]) {
            val wholeStageTransformer = exchange.find(_.isInstanceOf[WholeStageTransformer])
            if (wholeStageTransformer.nonEmpty) {
              wholeStageTransformer.get
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
                case s: SubqueryBroadcastExec =>
                  val newIn = s.transform {
                      case exchange: BroadcastExchangeExec =>
                        convertBroadcastExchangeToColumnar(exchange)
                    }.asInstanceOf[SubqueryBroadcastExec]
                  val transformSubqueryBroadcast = ColumnarSubqueryBroadcastExec(
                    newIn.name,
                    newIn.index,
                    newIn.buildKeys,
                    newIn.child)

                  // When AQE is on, spark will apply ReuseAdaptiveSubquery rule first,
                  // it will reuse vanilla SubqueryBroadcastExec,
                  // and then use gluten ColumnarOverrides rule to transform Subquery,
                  // so all the SubqueryBroadcastExec in the ReusedSubqueryExec will be transformed
                  // to a new ColumnarSubqueryBroadcastExec for each SubqueryBroadcastExec,
                  // which will lead to execute ColumnarSubqueryBroadcastExec.relationFuture
                  // repeatedly even in the ReusedSubqueryExec.
                  //
                  // On the other hand, it needs to use
                  // the AdaptiveSparkPlanExec.AdaptiveExecutionContext to hold the reused map
                  // for each query.
                  if (newIn.child.isInstanceOf[AdaptiveSparkPlanExec] && reuseSubquery) {
                    // When AQE is on and reuseSubquery is on.
                    newIn.child.asInstanceOf[AdaptiveSparkPlanExec].context
                      .subqueryCache.update(newIn.canonicalized, transformSubqueryBroadcast)
                  }
                  in.copy(plan = transformSubqueryBroadcast.asInstanceOf[BaseSubqueryExec])
                case r: ReusedSubqueryExec if r.child.isInstanceOf[SubqueryBroadcastExec] =>
                  val newIn = r.child.transform {
                      case exchange: BroadcastExchangeExec =>
                        convertBroadcastExchangeToColumnar(exchange)
                    }.asInstanceOf[SubqueryBroadcastExec]
                  newIn.child match {
                    case a: AdaptiveSparkPlanExec =>
                      // Only when AQE is on, it needs to replace SubqueryBroadcastExec
                      // with reused ColumnarSubqueryBroadcastExec
                      val cachedSubquery = a.context.subqueryCache.get(newIn.canonicalized)
                      if (cachedSubquery.isDefined) {
                        in.copy(plan = ReusedSubqueryExec(cachedSubquery.get))
                      } else {
                        val errMsg = "Can not get the reused ColumnarSubqueryBroadcastExec" +
                          "by the ${newIn.canonicalized}"
                        logWarning(errMsg)
                        throw new UnsupportedOperationException(errMsg)
                      }
                    case other =>
                      val errMsg = "Can not get the reused ColumnarSubqueryBroadcastExec" +
                        "by the ${newIn.canonicalized}"
                      logWarning(errMsg)
                      throw new UnsupportedOperationException(errMsg)
                  }
                case _ => in
              }
          }
        case e: Expression => e
      }
    }
  }
}

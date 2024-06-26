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

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.exception.GlutenNotSupportException
import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.test.TestStats
import org.apache.gluten.utils.DecimalArithmeticUtil

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.catalyst.optimizer.NormalizeNaNAndZero
import org.apache.spark.sql.execution.ScalarSubquery
import org.apache.spark.sql.hive.HiveUDFTransformer
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

trait Transformable {
  def getTransformer(childrenTransformers: Seq[ExpressionTransformer]): ExpressionTransformer
}

object ExpressionConverter extends SQLConfHelper with Logging {

  def replaceWithExpressionTransformer(
      exprs: Seq[Expression],
      attributeSeq: Seq[Attribute]): Seq[ExpressionTransformer] = {
    val expressionsMap = ExpressionMappings.expressionsMap
    exprs.map {
      expr => replaceWithExpressionTransformerInternal(expr, attributeSeq, expressionsMap)
    }
  }

  def replaceWithExpressionTransformer(
      expr: Expression,
      attributeSeq: Seq[Attribute]): ExpressionTransformer = {
    val expressionsMap = ExpressionMappings.expressionsMap
    replaceWithExpressionTransformerInternal(expr, attributeSeq, expressionsMap)
  }

  private def replacePythonUDFWithExpressionTransformer(
      udf: PythonUDF,
      attributeSeq: Seq[Attribute],
      expressionsMap: Map[Class[_], String]): ExpressionTransformer = {
    val substraitExprName = UDFMappings.pythonUDFMap.get(udf.name)
    substraitExprName match {
      case Some(name) =>
        GenericExpressionTransformer(
          name,
          udf.children.map(
            replaceWithExpressionTransformerInternal(_, attributeSeq, expressionsMap)),
          udf)
      case _ =>
        throw new GlutenNotSupportException(s"Not supported python udf: $udf.")
    }
  }

  private def replaceScalaUDFWithExpressionTransformer(
      udf: ScalaUDF,
      attributeSeq: Seq[Attribute],
      expressionsMap: Map[Class[_], String]): ExpressionTransformer = {
    if (udf.udfName.isEmpty) {
      throw new GlutenNotSupportException("UDF name is not found!")
    }
    val substraitExprName = UDFMappings.scalaUDFMap.get(udf.udfName.get)
    substraitExprName match {
      case Some(name) =>
        GenericExpressionTransformer(
          name,
          udf.children.map(
            replaceWithExpressionTransformerInternal(_, attributeSeq, expressionsMap)),
          udf)
      case _ =>
        throw new GlutenNotSupportException(s"Not supported scala udf: $udf.")
    }
  }

  private def genRescaleDecimalTransformer(
      substraitName: String,
      b: BinaryArithmetic,
      attributeSeq: Seq[Attribute],
      expressionsMap: Map[Class[_], String]): DecimalArithmeticExpressionTransformer = {
    val rescaleBinary = DecimalArithmeticUtil.rescaleLiteral(b)
    val (left, right) = DecimalArithmeticUtil.rescaleCastForDecimal(
      DecimalArithmeticUtil.removeCastForDecimal(rescaleBinary.left),
      DecimalArithmeticUtil.removeCastForDecimal(rescaleBinary.right))
    val resultType = DecimalArithmeticUtil.getResultType(
      b,
      left.dataType.asInstanceOf[DecimalType],
      right.dataType.asInstanceOf[DecimalType]
    )

    val leftChild =
      replaceWithExpressionTransformerInternal(left, attributeSeq, expressionsMap)
    val rightChild =
      replaceWithExpressionTransformerInternal(right, attributeSeq, expressionsMap)
    DecimalArithmeticExpressionTransformer(substraitName, leftChild, rightChild, resultType, b)
  }

  private def replaceWithExpressionTransformerInternal(
      expr: Expression,
      attributeSeq: Seq[Attribute],
      expressionsMap: Map[Class[_], String]): ExpressionTransformer = {
    logDebug(
      s"replaceWithExpressionTransformer expr: $expr class: ${expr.getClass} " +
        s"name: ${expr.prettyName}")

    expr match {
      case p: PythonUDF =>
        return replacePythonUDFWithExpressionTransformer(p, attributeSeq, expressionsMap)
      case s: ScalaUDF =>
        return replaceScalaUDFWithExpressionTransformer(s, attributeSeq, expressionsMap)
      case _ if HiveUDFTransformer.isHiveUDF(expr) =>
        return HiveUDFTransformer.replaceWithExpressionTransformer(expr, attributeSeq)
      case i: StaticInvoke =>
        val objectName = i.staticObject.getName.stripSuffix("$")
        if (objectName.endsWith("UrlCodec")) {
          val child = i.arguments.head
          i.functionName match {
            case "decode" =>
              return GenericExpressionTransformer(
                ExpressionNames.URL_DECODE,
                child.map(
                  replaceWithExpressionTransformerInternal(_, attributeSeq, expressionsMap)),
                i)
            case "encode" =>
              return GenericExpressionTransformer(
                ExpressionNames.URL_ENCODE,
                child.map(
                  replaceWithExpressionTransformerInternal(_, attributeSeq, expressionsMap)),
                i)
          }
        }
      case _ =>
    }

    val substraitExprName: String = getAndCheckSubstraitName(expr, expressionsMap)

    expr match {
      case extendedExpr
          if ExpressionMappings.expressionExtensionTransformer.extensionExpressionsMapping.contains(
            extendedExpr.getClass) =>
        // Use extended expression transformer to replace custom expression first
        ExpressionMappings.expressionExtensionTransformer
          .replaceWithExtensionExpressionTransformer(substraitExprName, extendedExpr, attributeSeq)
      case c: CreateArray =>
        val children =
          c.children.map(replaceWithExpressionTransformerInternal(_, attributeSeq, expressionsMap))
        CreateArrayTransformer(substraitExprName, children, c)
      case g: GetArrayItem =>
        BackendsApiManager.getSparkPlanExecApiInstance.genGetArrayItemTransformer(
          substraitExprName,
          replaceWithExpressionTransformerInternal(g.left, attributeSeq, expressionsMap),
          replaceWithExpressionTransformerInternal(g.right, attributeSeq, expressionsMap),
          g
        )
      case c: CreateMap =>
        val children =
          c.children.map(replaceWithExpressionTransformerInternal(_, attributeSeq, expressionsMap))
        CreateMapTransformer(substraitExprName, children, c)
      case g: GetMapValue =>
        BackendsApiManager.getSparkPlanExecApiInstance.genGetMapValueTransformer(
          substraitExprName,
          replaceWithExpressionTransformerInternal(g.child, attributeSeq, expressionsMap),
          replaceWithExpressionTransformerInternal(g.key, attributeSeq, expressionsMap),
          g
        )
      case m: MapEntries =>
        BackendsApiManager.getSparkPlanExecApiInstance.genMapEntriesTransformer(
          substraitExprName,
          replaceWithExpressionTransformerInternal(m.child, attributeSeq, expressionsMap),
          m)
      case e: Explode =>
        ExplodeTransformer(
          substraitExprName,
          replaceWithExpressionTransformerInternal(e.child, attributeSeq, expressionsMap),
          e)
      case p: PosExplode =>
        BackendsApiManager.getSparkPlanExecApiInstance.genPosExplodeTransformer(
          substraitExprName,
          replaceWithExpressionTransformerInternal(p.child, attributeSeq, expressionsMap),
          p,
          attributeSeq)
      case i: Inline =>
        BackendsApiManager.getSparkPlanExecApiInstance.genInlineTransformer(
          substraitExprName,
          replaceWithExpressionTransformerInternal(i.child, attributeSeq, expressionsMap),
          i)
      case a: Alias =>
        BackendsApiManager.getSparkPlanExecApiInstance.genAliasTransformer(
          substraitExprName,
          replaceWithExpressionTransformerInternal(a.child, attributeSeq, expressionsMap),
          a)
      case a: AttributeReference =>
        if (attributeSeq == null) {
          throw new UnsupportedOperationException(s"attributeSeq should not be null.")
        }
        try {
          val bindReference =
            BindReferences.bindReference(expr, attributeSeq, allowFailures = false)
          val b = bindReference.asInstanceOf[BoundReference]
          AttributeReferenceTransformer(substraitExprName, a, b)
        } catch {
          case e: IllegalStateException =>
            // This situation may need developers to fix, although we just throw the below
            // exception to let the corresponding operator fall back.
            throw new UnsupportedOperationException(
              s"Failed to bind reference for $expr: ${e.getMessage}")
        }
      case b: BoundReference =>
        BoundReferenceTransformer(substraitExprName, b)
      case l: Literal =>
        LiteralTransformer(l)
      case d: DateDiff =>
        BackendsApiManager.getSparkPlanExecApiInstance.genDateDiffTransformer(
          substraitExprName,
          replaceWithExpressionTransformerInternal(d.endDate, attributeSeq, expressionsMap),
          replaceWithExpressionTransformerInternal(d.startDate, attributeSeq, expressionsMap),
          d
        )
      case r: Round if r.child.dataType.isInstanceOf[DecimalType] =>
        DecimalRoundTransformer(
          substraitExprName,
          replaceWithExpressionTransformerInternal(r.child, attributeSeq, expressionsMap),
          r)
      case t: ToUnixTimestamp =>
        // The failOnError depends on the config for ANSI. ANSI is not supported currently.
        // And timeZoneId is passed to backend config.
        GenericExpressionTransformer(
          substraitExprName,
          Seq(
            replaceWithExpressionTransformerInternal(t.timeExp, attributeSeq, expressionsMap),
            replaceWithExpressionTransformerInternal(t.format, attributeSeq, expressionsMap)
          ),
          t
        )
      case u: UnixTimestamp =>
        GenericExpressionTransformer(
          substraitExprName,
          Seq(
            replaceWithExpressionTransformerInternal(u.timeExp, attributeSeq, expressionsMap),
            replaceWithExpressionTransformerInternal(u.format, attributeSeq, expressionsMap)
          ),
          ToUnixTimestamp(u.timeExp, u.format, u.timeZoneId, u.failOnError)
        )
      case t: TruncTimestamp =>
        BackendsApiManager.getSparkPlanExecApiInstance.genTruncTimestampTransformer(
          substraitExprName,
          replaceWithExpressionTransformerInternal(t.format, attributeSeq, expressionsMap),
          replaceWithExpressionTransformerInternal(t.timestamp, attributeSeq, expressionsMap),
          t.timeZoneId,
          t
        )
      case m: MonthsBetween =>
        MonthsBetweenTransformer(
          substraitExprName,
          replaceWithExpressionTransformerInternal(m.date1, attributeSeq, expressionsMap),
          replaceWithExpressionTransformerInternal(m.date2, attributeSeq, expressionsMap),
          replaceWithExpressionTransformerInternal(m.roundOff, attributeSeq, expressionsMap),
          m
        )
      case i: If =>
        IfTransformer(
          substraitExprName,
          replaceWithExpressionTransformerInternal(i.predicate, attributeSeq, expressionsMap),
          replaceWithExpressionTransformerInternal(i.trueValue, attributeSeq, expressionsMap),
          replaceWithExpressionTransformerInternal(i.falseValue, attributeSeq, expressionsMap),
          i
        )
      case cw: CaseWhen =>
        CaseWhenTransformer(
          substraitExprName,
          cw.branches.map {
            expr =>
              {
                (
                  replaceWithExpressionTransformerInternal(expr._1, attributeSeq, expressionsMap),
                  replaceWithExpressionTransformerInternal(expr._2, attributeSeq, expressionsMap))
              }
          },
          cw.elseValue.map {
            expr =>
              {
                replaceWithExpressionTransformerInternal(expr, attributeSeq, expressionsMap)
              }
          },
          cw
        )
      case i: In =>
        if (i.list.exists(!_.foldable)) {
          throw new GlutenNotSupportException(
            s"In list option does not support non-foldable expression, ${i.list.map(_.sql)}")
        }
        InTransformer(
          substraitExprName,
          replaceWithExpressionTransformerInternal(i.value, attributeSeq, expressionsMap),
          i)
      case i: InSet =>
        InSetTransformer(
          substraitExprName,
          replaceWithExpressionTransformerInternal(i.child, attributeSeq, expressionsMap),
          i)
      case s: ScalarSubquery =>
        ScalarSubqueryTransformer(substraitExprName, s)
      case c: Cast =>
        // Add trim node, as necessary.
        val newCast =
          BackendsApiManager.getSparkPlanExecApiInstance.genCastWithNewChild(c)
        CastTransformer(
          substraitExprName,
          replaceWithExpressionTransformerInternal(newCast.child, attributeSeq, expressionsMap),
          newCast)
      case s: String2TrimExpression =>
        val (srcStr, trimStr) = s match {
          case StringTrim(srcStr, trimStr) => (srcStr, trimStr)
          case StringTrimLeft(srcStr, trimStr) => (srcStr, trimStr)
          case StringTrimRight(srcStr, trimStr) => (srcStr, trimStr)
        }
        val children = trimStr
          .map(replaceWithExpressionTransformerInternal(_, attributeSeq, expressionsMap))
          .toSeq ++
          Seq(replaceWithExpressionTransformerInternal(srcStr, attributeSeq, expressionsMap))
        GenericExpressionTransformer(
          substraitExprName,
          children,
          s
        )
      case m: HashExpression[_] =>
        BackendsApiManager.getSparkPlanExecApiInstance.genHashExpressionTransformer(
          substraitExprName,
          m.children.map(
            expr => replaceWithExpressionTransformerInternal(expr, attributeSeq, expressionsMap)),
          m)
      case getStructField: GetStructField =>
        // Different backends may have different result.
        BackendsApiManager.getSparkPlanExecApiInstance.genGetStructFieldTransformer(
          substraitExprName,
          replaceWithExpressionTransformerInternal(
            getStructField.child,
            attributeSeq,
            expressionsMap),
          getStructField.ordinal,
          getStructField)
      case getArrayStructFields: GetArrayStructFields =>
        GenericExpressionTransformer(
          substraitExprName,
          Seq(
            replaceWithExpressionTransformerInternal(
              getArrayStructFields.child,
              attributeSeq,
              expressionsMap),
            LiteralTransformer(getArrayStructFields.ordinal)),
          getArrayStructFields
        )
      case t: StringTranslate =>
        BackendsApiManager.getSparkPlanExecApiInstance.genStringTranslateTransformer(
          substraitExprName,
          replaceWithExpressionTransformerInternal(t.srcExpr, attributeSeq, expressionsMap),
          replaceWithExpressionTransformerInternal(t.matchingExpr, attributeSeq, expressionsMap),
          replaceWithExpressionTransformerInternal(t.replaceExpr, attributeSeq, expressionsMap),
          t
        )
      case s: StringSplit =>
        BackendsApiManager.getSparkPlanExecApiInstance.genStringSplitTransformer(
          substraitExprName,
          replaceWithExpressionTransformerInternal(s.str, attributeSeq, expressionsMap),
          replaceWithExpressionTransformerInternal(s.regex, attributeSeq, expressionsMap),
          replaceWithExpressionTransformerInternal(s.limit, attributeSeq, expressionsMap),
          s
        )
      case r: RegExpReplace =>
        BackendsApiManager.getSparkPlanExecApiInstance.genRegexpReplaceTransformer(
          substraitExprName,
          Seq(
            replaceWithExpressionTransformerInternal(r.subject, attributeSeq, expressionsMap),
            replaceWithExpressionTransformerInternal(r.regexp, attributeSeq, expressionsMap),
            replaceWithExpressionTransformerInternal(r.rep, attributeSeq, expressionsMap),
            replaceWithExpressionTransformerInternal(r.pos, attributeSeq, expressionsMap)
          ),
          r
        )
      case size: Size =>
        if (size.legacySizeOfNull != SQLConf.get.legacySizeOfNull) {
          throw new GlutenNotSupportException(
            "The value of legacySizeOfNull field of size is " +
              "not equals to legacySizeOfNull of SQLConf, this case is not supported yet")
        }
        BackendsApiManager.getSparkPlanExecApiInstance.genSizeExpressionTransformer(
          substraitExprName,
          replaceWithExpressionTransformerInternal(size.child, attributeSeq, expressionsMap),
          size)
      case namedStruct: CreateNamedStruct =>
        BackendsApiManager.getSparkPlanExecApiInstance.genNamedStructTransformer(
          substraitExprName,
          namedStruct.children.map(
            replaceWithExpressionTransformerInternal(_, attributeSeq, expressionsMap)),
          namedStruct,
          attributeSeq)
      case namedLambdaVariable: NamedLambdaVariable =>
        // namedlambdavariable('acc')-> <Integer, notnull>
        GenericExpressionTransformer(
          substraitExprName,
          LiteralTransformer(namedLambdaVariable.name),
          namedLambdaVariable
        )
      case lambdaFunction: LambdaFunction =>
        LambdaFunctionTransformer(
          substraitExprName,
          function = replaceWithExpressionTransformerInternal(
            lambdaFunction.function,
            attributeSeq,
            expressionsMap),
          arguments = lambdaFunction.arguments.map(
            replaceWithExpressionTransformerInternal(_, attributeSeq, expressionsMap)),
          original = lambdaFunction
        )
      case j: JsonTuple =>
        val children =
          j.children.map(replaceWithExpressionTransformerInternal(_, attributeSeq, expressionsMap))
        JsonTupleExpressionTransformer(substraitExprName, children, j)
      case l: Like =>
        BackendsApiManager.getSparkPlanExecApiInstance.genLikeTransformer(
          substraitExprName,
          replaceWithExpressionTransformerInternal(l.left, attributeSeq, expressionsMap),
          replaceWithExpressionTransformerInternal(l.right, attributeSeq, expressionsMap),
          l
        )
      case m: MakeDecimal =>
        GenericExpressionTransformer(
          substraitExprName,
          Seq(
            replaceWithExpressionTransformerInternal(m.child, attributeSeq, expressionsMap),
            LiteralTransformer(m.nullOnOverflow)),
          m
        )
      case _: NormalizeNaNAndZero | _: PromotePrecision | _: TaggingExpression =>
        ChildTransformer(
          substraitExprName,
          replaceWithExpressionTransformerInternal(
            expr.children.head,
            attributeSeq,
            expressionsMap),
          expr
        )
      case _: GetDateField | _: GetTimeField =>
        ExtractDateTransformer(
          substraitExprName,
          replaceWithExpressionTransformerInternal(
            expr.children.head,
            attributeSeq,
            expressionsMap),
          expr)
      case _: StringToMap =>
        BackendsApiManager.getSparkPlanExecApiInstance.genStringToMapTransformer(
          substraitExprName,
          expr.children.map(
            replaceWithExpressionTransformerInternal(_, attributeSeq, expressionsMap)),
          expr)
      case CheckOverflow(b: BinaryArithmetic, decimalType, _)
          if !BackendsApiManager.getSettings.transformCheckOverflow &&
            DecimalArithmeticUtil.isDecimalArithmetic(b) =>
        DecimalArithmeticUtil.checkAllowDecimalArithmetic()
        val leftChild =
          replaceWithExpressionTransformerInternal(b.left, attributeSeq, expressionsMap)
        val rightChild =
          replaceWithExpressionTransformerInternal(b.right, attributeSeq, expressionsMap)
        DecimalArithmeticExpressionTransformer(
          getAndCheckSubstraitName(b, expressionsMap),
          leftChild,
          rightChild,
          decimalType,
          b)
      case c: CheckOverflow =>
        CheckOverflowTransformer(
          substraitExprName,
          replaceWithExpressionTransformerInternal(c.child, attributeSeq, expressionsMap),
          c)
      case b: BinaryArithmetic if DecimalArithmeticUtil.isDecimalArithmetic(b) =>
        DecimalArithmeticUtil.checkAllowDecimalArithmetic()
        if (!BackendsApiManager.getSettings.transformCheckOverflow) {
          GenericExpressionTransformer(
            substraitExprName,
            expr.children.map(
              replaceWithExpressionTransformerInternal(_, attributeSeq, expressionsMap)),
            expr
          )
        } else {
          // Without the rescale and remove cast, result is right for high version Spark,
          // but performance regression in velox
          genRescaleDecimalTransformer(substraitExprName, b, attributeSeq, expressionsMap)
        }
      case n: NaNvl =>
        BackendsApiManager.getSparkPlanExecApiInstance.genNaNvlTransformer(
          substraitExprName,
          replaceWithExpressionTransformerInternal(n.left, attributeSeq, expressionsMap),
          replaceWithExpressionTransformerInternal(n.right, attributeSeq, expressionsMap),
          n
        )
      case m: MakeTimestamp =>
        BackendsApiManager.getSparkPlanExecApiInstance.genMakeTimestampTransformer(
          substraitExprName,
          m.children.map(replaceWithExpressionTransformerInternal(_, attributeSeq, expressionsMap)),
          m)
      case timestampAdd if timestampAdd.getClass.getSimpleName.equals("TimestampAdd") =>
        // for spark3.3
        val extract = SparkShimLoader.getSparkShims.extractExpressionTimestampAddUnit(timestampAdd)
        if (extract.isEmpty) {
          throw new UnsupportedOperationException(s"Not support expression TimestampAdd.")
        }
        val add = timestampAdd.asInstanceOf[BinaryExpression]
        TimestampAddTransformer(
          substraitExprName,
          extract.get.head,
          replaceWithExpressionTransformerInternal(add.left, attributeSeq, expressionsMap),
          replaceWithExpressionTransformerInternal(add.right, attributeSeq, expressionsMap),
          extract.get.last,
          add
        )
      case e: Transformable =>
        val childrenTransformers =
          e.children.map(replaceWithExpressionTransformerInternal(_, attributeSeq, expressionsMap))
        e.getTransformer(childrenTransformers)
      case u: Uuid =>
        BackendsApiManager.getSparkPlanExecApiInstance.genUuidTransformer(substraitExprName, u)
      case f: ArrayFilter =>
        BackendsApiManager.getSparkPlanExecApiInstance.genArrayFilterTransformer(
          substraitExprName,
          replaceWithExpressionTransformerInternal(f.argument, attributeSeq, expressionsMap),
          replaceWithExpressionTransformerInternal(f.function, attributeSeq, expressionsMap),
          f
        )
      case arrayTransform: ArrayTransform =>
        BackendsApiManager.getSparkPlanExecApiInstance.genArrayTransformTransformer(
          substraitExprName,
          replaceWithExpressionTransformerInternal(
            arrayTransform.argument,
            attributeSeq,
            expressionsMap),
          replaceWithExpressionTransformerInternal(
            arrayTransform.function,
            attributeSeq,
            expressionsMap),
          arrayTransform
        )
      case tryEval @ TryEval(a: Add) =>
        BackendsApiManager.getSparkPlanExecApiInstance.genTryArithmeticTransformer(
          substraitExprName,
          replaceWithExpressionTransformerInternal(a.left, attributeSeq, expressionsMap),
          replaceWithExpressionTransformerInternal(a.right, attributeSeq, expressionsMap),
          tryEval,
          ExpressionNames.CHECK_ADD
        )
      case tryEval @ TryEval(a: Subtract) =>
        BackendsApiManager.getSparkPlanExecApiInstance.genTryArithmeticTransformer(
          substraitExprName,
          replaceWithExpressionTransformerInternal(a.left, attributeSeq, expressionsMap),
          replaceWithExpressionTransformerInternal(a.right, attributeSeq, expressionsMap),
          tryEval,
          ExpressionNames.CHECK_SUBTRACT
        )
      case tryEval @ TryEval(a: Divide) =>
        BackendsApiManager.getSparkPlanExecApiInstance.genTryArithmeticTransformer(
          substraitExprName,
          replaceWithExpressionTransformerInternal(a.left, attributeSeq, expressionsMap),
          replaceWithExpressionTransformerInternal(a.right, attributeSeq, expressionsMap),
          tryEval,
          ExpressionNames.CHECK_DIVIDE
        )
      case tryEval @ TryEval(a: Multiply) =>
        BackendsApiManager.getSparkPlanExecApiInstance.genTryArithmeticTransformer(
          substraitExprName,
          replaceWithExpressionTransformerInternal(a.left, attributeSeq, expressionsMap),
          replaceWithExpressionTransformerInternal(a.right, attributeSeq, expressionsMap),
          tryEval,
          ExpressionNames.CHECK_MULTIPLY
        )
      case a: Add =>
        BackendsApiManager.getSparkPlanExecApiInstance.genArithmeticTransformer(
          substraitExprName,
          replaceWithExpressionTransformerInternal(a.left, attributeSeq, expressionsMap),
          replaceWithExpressionTransformerInternal(a.right, attributeSeq, expressionsMap),
          a,
          ExpressionNames.CHECK_ADD
        )
      case a: Subtract =>
        BackendsApiManager.getSparkPlanExecApiInstance.genArithmeticTransformer(
          substraitExprName,
          replaceWithExpressionTransformerInternal(a.left, attributeSeq, expressionsMap),
          replaceWithExpressionTransformerInternal(a.right, attributeSeq, expressionsMap),
          a,
          ExpressionNames.CHECK_SUBTRACT
        )
      case a: Multiply =>
        BackendsApiManager.getSparkPlanExecApiInstance.genArithmeticTransformer(
          substraitExprName,
          replaceWithExpressionTransformerInternal(a.left, attributeSeq, expressionsMap),
          replaceWithExpressionTransformerInternal(a.right, attributeSeq, expressionsMap),
          a,
          ExpressionNames.CHECK_MULTIPLY
        )
      case a: Divide =>
        BackendsApiManager.getSparkPlanExecApiInstance.genArithmeticTransformer(
          substraitExprName,
          replaceWithExpressionTransformerInternal(a.left, attributeSeq, expressionsMap),
          replaceWithExpressionTransformerInternal(a.right, attributeSeq, expressionsMap),
          a,
          ExpressionNames.CHECK_DIVIDE
        )
      case tryEval: TryEval =>
        // This is a placeholder to handle try_eval(other expressions).
        BackendsApiManager.getSparkPlanExecApiInstance.genTryEvalTransformer(
          substraitExprName,
          replaceWithExpressionTransformerInternal(tryEval.child, attributeSeq, expressionsMap),
          tryEval
        )
      case a: ArrayForAll =>
        BackendsApiManager.getSparkPlanExecApiInstance.genArrayForAllTransformer(
          substraitExprName,
          replaceWithExpressionTransformerInternal(a.argument, attributeSeq, expressionsMap),
          replaceWithExpressionTransformerInternal(a.function, attributeSeq, expressionsMap),
          a
        )
      case a: ArrayExists =>
        BackendsApiManager.getSparkPlanExecApiInstance.genArrayExistsTransformer(
          substraitExprName,
          replaceWithExpressionTransformerInternal(a.argument, attributeSeq, expressionsMap),
          replaceWithExpressionTransformerInternal(a.function, attributeSeq, expressionsMap),
          a
        )
      case s: Shuffle =>
        GenericExpressionTransformer(
          substraitExprName,
          Seq(
            replaceWithExpressionTransformerInternal(s.child, attributeSeq, expressionsMap),
            LiteralTransformer(Literal(s.randomSeed.get))),
          s)
      case c: PreciseTimestampConversion =>
        BackendsApiManager.getSparkPlanExecApiInstance.genPreciseTimestampConversionTransformer(
          substraitExprName,
          Seq(replaceWithExpressionTransformerInternal(c.child, attributeSeq, expressionsMap)),
          c
        )
      case t: TransformKeys =>
        // default is `EXCEPTION`
        val mapKeyDedupPolicy = SQLConf.get.getConf(SQLConf.MAP_KEY_DEDUP_POLICY)
        if (mapKeyDedupPolicy == SQLConf.MapKeyDedupPolicy.LAST_WIN.toString) {
          // TODO: Remove after fix ready for
          //  https://github.com/facebookincubator/velox/issues/10219
          throw new GlutenNotSupportException(
            "LAST_WIN policy is not supported yet in native to deduplicate map keys"
          )
        }
        GenericExpressionTransformer(
          substraitExprName,
          t.children.map(replaceWithExpressionTransformerInternal(_, attributeSeq, expressionsMap)),
          t
        )
      case expr =>
        GenericExpressionTransformer(
          substraitExprName,
          expr.children.map(
            replaceWithExpressionTransformerInternal(_, attributeSeq, expressionsMap)),
          expr
        )
    }
  }

  private def getAndCheckSubstraitName(expr: Expression, expressionsMap: Map[Class[_], String]) = {
    TestStats.addExpressionClassName(expr.getClass.getName)
    // Check whether Gluten supports this expression
    val substraitExprNameOpt = expressionsMap.get(expr.getClass)
    if (substraitExprNameOpt.isEmpty) {
      throw new GlutenNotSupportException(
        s"Not supported to map spark function name" +
          s" to substrait function name: $expr, class name: ${expr.getClass.getSimpleName}.")
    }
    val substraitExprName = substraitExprNameOpt.get
    // Check whether each backend supports this expression
    if (!BackendsApiManager.getValidatorApiInstance.doExprValidate(substraitExprName, expr)) {
      throw new GlutenNotSupportException(s"Not supported: $expr.")
    }
    substraitExprName
  }
}

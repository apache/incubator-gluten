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

package io.glutenproject.backendsapi.velox

import io.glutenproject.backendsapi.glutendata.GlutenSparkPlanExecApi
import io.glutenproject.execution.{HashAggregateExecBaseTransformer, VeloxHashAggregateExecTransformer}
import io.glutenproject.expression.{ExpressionNames, ExpressionTransformer, GlutenNamedStructTransformer, Sig}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.catalyst.VeloxAggregateFunctionRewriteRule
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, HLLVeloxAdapter}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Cast, CreateNamedStruct, Expression, Literal, NamedExpression, StringTrim}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.{ArrayType, BinaryType, DecimalType, DoubleType, FloatType, MapType, StringType, StructType, UserDefinedType}
import org.apache.spark.sql.execution.datasources.GlutenColumnarRules.NativeWritePostRule

class VeloxSparkPlanExecApi extends GlutenSparkPlanExecApi {
  /**
   * Generate extended columnar post-rules.
   *
   * @return
   */
  override def genExtendedColumnarPostRules(): List[SparkSession => Rule[SparkPlan]] =
    List(spark => NativeWritePostRule(spark)) ::: super.genExtendedColumnarPostRules()

  /**
   * Generate an expression transformer to transform NamedStruct to Substrait.
   */
  override def genNamedStructTransformer(substraitExprName: String,
                                         original: CreateNamedStruct,
                                         attributeSeq: Seq[Attribute]): ExpressionTransformer = {
    new GlutenNamedStructTransformer(substraitExprName, original, attributeSeq)
  }

  /**
   * To align with spark in casting string type input to other types,
   * add trim node for trimming space or whitespace. See spark's Cast.scala.
   */
  override def genCastWithNewChild(c: Cast): Cast = {
    // Common whitespace to be trimmed, including: ' ', '\n', '\r', '\f', etc.
    val trimWhitespaceStr = " \t\n\u000B\u000C\u000D\u001C\u001D\u001E\u001F"
    // Space separator.
    val trimSpaceSepStr = "\u1680\u2008\u2009\u200A\u205F\u3000" +
        ('\u2000' to '\u2006').toList.mkString
    // Line separator.
    val trimLineSepStr = "\u2028"
    // Paragraph separator.
    val trimParaSepStr = "\u2029"
    // Needs to be trimmed for casting to float/double/decimal
    val trimSpaceStr = ('\u0000' to '\u0020').toList.mkString
    c.dataType match {
      case BinaryType | _: ArrayType | _: MapType | _: StructType | _: UserDefinedType[_] =>
        c
      case FloatType | DoubleType | _: DecimalType =>
        c.child.dataType match {
          case StringType =>
            val trimNode = StringTrim(c.child, Some(Literal(trimSpaceStr)))
            c.withNewChildren(Seq(trimNode)).asInstanceOf[Cast]
          case _ =>
            c
        }
      case _ =>
        c.child.dataType match {
          case StringType =>
            val trimNode = StringTrim(c.child, Some(Literal(trimWhitespaceStr +
                trimSpaceSepStr + trimLineSepStr + trimParaSepStr)))
            c.withNewChildren(Seq(trimNode)).asInstanceOf[Cast]
          case _ =>
            c
        }
    }
  }

   /**
   * Generate HashAggregateExecTransformer.
   */
  override def genHashAggregateExecTransformer(
    requiredChildDistributionExpressions: Option[Seq[Expression]],
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    child: SparkPlan): HashAggregateExecBaseTransformer =
    VeloxHashAggregateExecTransformer(
      requiredChildDistributionExpressions,
      groupingExpressions,
      aggregateExpressions,
      aggregateAttributes,
      initialInputBufferOffset,
      resultExpressions,
      child)

  /**
   * Generate extended Optimizer.
   * Currently only for Velox backend.
   *
   * @return
   */
  override def genExtendedOptimizers(): List[SparkSession =>
    Rule[LogicalPlan]] = List(VeloxAggregateFunctionRewriteRule)

  /**
   * Define backend specfic expression mappings.
   */
  override def extraExpressionMappings: Seq[Sig] =
    Seq(Sig[HLLVeloxAdapter](ExpressionNames.APPROX_DISTINCT))
}

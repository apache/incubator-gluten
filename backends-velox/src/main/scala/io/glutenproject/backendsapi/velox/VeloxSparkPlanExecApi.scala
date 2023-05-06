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
import io.glutenproject.expression.{ExpressionTransformer, GlutenNamedStructTransformer}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.VeloxColumnarRules.OtherWritePostRule
import org.apache.spark.sql.catalyst.expressions.{Attribute, Cast, CreateNamedStruct, Expression, Literal, StringTrim}
import org.apache.spark.sql.types.{ByteType, IntegerType, LongType, ShortType, StringType}

class VeloxSparkPlanExecApi extends GlutenSparkPlanExecApi {
  /**
   * Generate extended columnar post-rules.
   * Currently only for Velox backend.
   *
   * @return
   */
  override def genExtendedColumnarPostRules(): List[SparkSession => Rule[SparkPlan]] =
    List(spark => OtherWritePostRule(spark)) ::: super.genExtendedColumnarPostRules()

  /**
   * Generate an expression transformer to transform NamedStruct to Substrait.
   */
  override def genNamedStructTransformer(substraitExprName: String,
                                         original: CreateNamedStruct,
                                         attributeSeq: Seq[Attribute]): ExpressionTransformer = {
    new GlutenNamedStructTransformer(substraitExprName, original, attributeSeq)
  }

  // To align with spark in casting string type input to integral type,
  // add trim node for trimming whitespace. See spark's toInt in UTF8String.java.
  override def genCastWithNewChild(c: Cast): Cast = {
    // Whitespace to be trimmed, including: ' ', '\n', '\r', '\f', etc.
    val trimStr = " \t\n\u000B\u000C\r\u001C\u001D\u001E\u001F"
    c.dataType match {
      case ByteType | ShortType | IntegerType | LongType =>
        c.child.dataType match {
          case StringType =>
            val trim = StringTrim(c.child, Some(Literal(trimStr)))
            c.withNewChildren(Seq(trim)).asInstanceOf[Cast]
          case _ =>
            c
        }
      case _ =>
        c
    }
  }

}

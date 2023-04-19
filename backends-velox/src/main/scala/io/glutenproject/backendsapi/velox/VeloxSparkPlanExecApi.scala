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
import org.apache.spark.sql.catalyst.expressions.{Attribute, CreateNamedStruct}

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
}

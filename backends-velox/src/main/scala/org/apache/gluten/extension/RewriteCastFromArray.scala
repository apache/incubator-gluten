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
package org.apache.gluten.extension

import org.apache.gluten.config.GlutenConfig

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{ArrayJoin, Cast, Concat, Literal}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.CAST
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, BooleanType, DateType, DoubleType, IntegerType, LongType, StringType, TimestampType}

/**
 * Velox does not support cast Array to String. Before velox support, temporarily add this rule to
 * replace `cast(array as String)` with `concat('[', array_join(array, ', ', 'null'), ']')` to
 * support offload.
 */
case class RewriteCastFromArray(spark: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (
      !GlutenConfig.get.enableRewriteCastArrayToString ||
      SQLConf.get.getConf(SQLConf.LEGACY_COMPLEX_TYPES_TO_STRING)
    ) {
      return plan
    }
    plan.transformExpressionsUpWithPruning(_.containsPattern(CAST)) {
      case cast @ Cast(child, StringType, timeZoneId, evalMode)
          if child.dataType.isInstanceOf[ArrayType] =>
        child.dataType match {
          case ArrayType(StringType, _) =>
            val arrayJoin = ArrayJoin(child, Literal(", "), Some(Literal("null")))
            Concat(Seq(Literal("["), arrayJoin, Literal("]")))
          case ArrayType(IntegerType, _) | ArrayType(LongType, _) | ArrayType(DoubleType, _) |
              ArrayType(BooleanType, _) | ArrayType(DateType, _) | ArrayType(TimestampType, _) =>
            val arrayJoin = ArrayJoin(
              Cast(child, ArrayType(StringType), timeZoneId, evalMode),
              Literal(", "),
              Some(Literal("null")))
            Concat(Seq(Literal("["), arrayJoin, Literal("]")))
          case _ => cast
        }
    }
  }
}

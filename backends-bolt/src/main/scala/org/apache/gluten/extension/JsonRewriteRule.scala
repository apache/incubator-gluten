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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

case class JsonRewriteRule(spark: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (
      plan.resolved
      && GlutenConfig.get.enableJsonRewrite
    ) {
      visitPlan(plan)
    } else {
      plan
    }
  }

  private lazy val JSON_PATH_PREFIX = UTF8String.fromString("$.")

  private def getSplitJsonPath(jsonPath: String): Array[String] = {
    jsonPath.split("\\.")
  }

  private def cleanPath(path: Expression): Option[String] = path match {
    case Literal(v: UTF8String, StringType) if v.startsWith(JSON_PATH_PREFIX) =>
      // Ref: org.apache.spark.sql.catalyst.expressions.JsonPathParser.parse
      val checkStr = v.substring(JSON_PATH_PREFIX.numChars(), v.numChars()).toString
      if (
        getSplitJsonPath(checkStr).length > 2
        || checkStr.contains("[")
        || checkStr.contains("]")
        || checkStr.contains("?")
        || checkStr.contains("*")
      ) {
        None
      } else {
        Some(checkStr)
      }
    case _ => None
  }

  private def visitPlan(plan: LogicalPlan): LogicalPlan = plan.transformUp {
    case p @ (_: Project | _: Filter) =>
      p.transformExpressionsUp {
        case g @ GetJsonObject(StructsToJson(_, nn @ CreateNamedStruct(_), _), path)
            if nn.nameExprs.forall(e => e.isInstanceOf[Literal] && e.dataType == StringType) &&
              cleanPath(path).isDefined =>
          val jsonPath = cleanPath(path).get
          val jsonPaths = getSplitJsonPath(jsonPath)
          if (jsonPaths.length <= 1) {
            val idx = nn.nameExprs.map(_.asInstanceOf[Literal].value.toString).indexOf(jsonPath)
            if (idx >= 0) {
              Cast(nn.valExprs(idx), g.dataType)
            } else {
              g
            }
          } else if (jsonPaths.length == 2) {
            val (firstLevel, secondLevel) = (jsonPaths(0), jsonPaths(1))
            val idx = nn.nameExprs.map(_.asInstanceOf[Literal].value.toString).indexOf(firstLevel)
            if (idx >= 0) {
              Cast(GetJsonObject(nn.valExprs(idx), Literal.create("$." + secondLevel)), g.dataType)
            } else {
              g
            }
          } else {
            throw new IllegalStateException("Only json paths with depth  <= 2 are expected here.")
          }

        case l @ Like(s, Concat(Seq(first, ss, last)), escapeChar)
            if escapeChar == '\\' && first.foldable && last.foldable =>
          if (
            first.eval(null).asInstanceOf[UTF8String].toString.equals("%") &&
            last.eval(null).asInstanceOf[UTF8String].toString.equals("%")
          ) {
            Contains(s, ss)
          } else {
            l
          }

        case a @ ArrayContains(arrays, value) if arrays.foldable && !arrays.nullable =>
          (arrays.dataType, value.dataType) match {
            case (ArrayType(childType, _), valueType) if childType == valueType =>
              val valueSet = arrays.eval(null).asInstanceOf[GenericArrayData].array
              InSet(value, valueSet.toSet)
            case _ =>
              a
          }
      }
  }
}

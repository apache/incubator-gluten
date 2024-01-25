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
package io.glutenproject.utils

import io.glutenproject.extension.columnar.TransformHints

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.SparkPlan

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable

trait PullOutProjectHelper {

  private val generatedNameIndex = new AtomicInteger(0)

  /**
   * The majority of Expressions only support Attribute and BoundReference when converting them into
   * native plans.
   */
  protected def isNotAttribute(expression: Expression): Boolean = expression match {
    case _: Attribute | _: BoundReference => false
    case _ => true
  }

  protected def replaceExpressionWithAttribute(
      expr: Expression,
      projectExprsMap: mutable.HashMap[Expression, NamedExpression]): Attribute =
    expr match {
      case alias: Alias =>
        projectExprsMap.getOrElseUpdate(alias.child.canonicalized, alias).toAttribute
      case attr: Attribute =>
        attr
      case other =>
        projectExprsMap
          .getOrElseUpdate(
            other.canonicalized,
            Alias(other, s"_pre_${generatedNameIndex.getAndIncrement()}")())
          .toAttribute
    }

  protected def eliminateProjectList(
      childOutput: Seq[NamedExpression],
      appendAttributes: Seq[NamedExpression]): Seq[NamedExpression] = {
    childOutput ++ appendAttributes.filter(attr => !childOutput.contains(attr))
  }

  protected def notSupportTransform(plan: SparkPlan): Boolean =
    TransformHints.isAlreadyTagged(plan) && TransformHints.isNotTransformable(plan)
}

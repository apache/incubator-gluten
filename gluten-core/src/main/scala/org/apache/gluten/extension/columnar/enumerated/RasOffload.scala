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
package org.apache.gluten.extension.columnar.enumerated

import org.apache.gluten.extension.GlutenPlan
import org.apache.gluten.extension.columnar.OffloadSingleNode
import org.apache.gluten.extension.columnar.rewrite.RewriteSingleNode
import org.apache.gluten.extension.columnar.validator.{Validator, Validators}
import org.apache.gluten.ras.path.Pattern
import org.apache.gluten.ras.path.Pattern.node
import org.apache.gluten.ras.rule.{RasRule, Shape}
import org.apache.gluten.ras.rule.Shapes.pattern

import org.apache.spark.sql.execution.SparkPlan

import scala.reflect.{classTag, ClassTag}

trait RasOffload {
  def offload(plan: SparkPlan): SparkPlan
  def typeIdentifier(): RasOffload.TypeIdentifier
}

object RasOffload {
  trait TypeIdentifier {
    def isInstance(node: SparkPlan): Boolean
  }

  object TypeIdentifier {
    def of[T <: SparkPlan: ClassTag]: TypeIdentifier = {
      val nodeClass: Class[SparkPlan] =
        classTag[T].runtimeClass.asInstanceOf[Class[SparkPlan]]
      new TypeIdentifier {
        override def isInstance(node: SparkPlan): Boolean = nodeClass.isInstance(node)
      }
    }
  }

  val validator: Validator = Validators
    .builder()
    .fallbackByHint()
    .fallbackIfScanOnly()
    .fallbackComplexExpressions()
    .fallbackByBackendSettings()
    .fallbackByUserOptions()
    .fallbackByTestInjects()
    .build()

  private val rewrites = RewriteSingleNode.allRules()

  def from[T <: SparkPlan: ClassTag](base: OffloadSingleNode): RasOffload = {
    new RasOffload {
      override def offload(plan: SparkPlan): SparkPlan = base.offload(plan)
      override def typeIdentifier(): TypeIdentifier = TypeIdentifier.of[T]
    }
  }

  def from(identifier: TypeIdentifier, base: OffloadSingleNode): RasOffload = {
    new RasOffload {
      override def offload(plan: SparkPlan): SparkPlan = base.offload(plan)
      override def typeIdentifier(): TypeIdentifier = identifier
    }
  }

  implicit class RasOffloadOps(base: RasOffload) {
    def toRule: RasRule[SparkPlan] = {
      new RuleImpl(base)
    }
  }

  private class RuleImpl(base: RasOffload) extends RasRule[SparkPlan] {
    private val typeIdentifier: TypeIdentifier = base.typeIdentifier()

    final override def shift(node: SparkPlan): Iterable[SparkPlan] = {
      // 0. If the node is already offloaded, fail fast.
      assert(typeIdentifier.isInstance(node))

      // 1. Rewrite the node to form that native library supports.
      val rewritten = rewrites.foldLeft(node) {
        case (node, rewrite) =>
          node.transformUp {
            case p =>
              val out = rewrite.rewrite(p)
              out
          }
      }

      // 2. Walk the rewritten tree.
      val offloaded = rewritten.transformUp {
        case from if typeIdentifier.isInstance(from) =>
          // 3. Validate current node. If passed, offload it.
          validator.validate(from) match {
            case Validator.Passed =>
              val offloaded = base.offload(from)
              offloaded match {
                case t: GlutenPlan if !t.doValidate().isValid =>
                  // 4. If native validation fails on the offloaded node, return the
                  // original one.
                  from
                case other =>
                  other
              }
            case Validator.Failed(reason) =>
              from
          }
      }

      // 5. If rewritten plan is not offload-able, discard it.
      if (offloaded.fastEquals(rewritten)) {
        return List.empty
      }

      // 6. Otherwise, return the final tree.
      List(offloaded)
    }

    override def shape(): Shape[SparkPlan] = {
      pattern(node[SparkPlan](new Pattern.Matcher[SparkPlan] {
        override def apply(plan: SparkPlan): Boolean = {
          if (plan.isInstanceOf[GlutenPlan]) {
            return false
          }
          if (typeIdentifier.isInstance(plan)) {
            return true
          }
          false
        }
      }).build())
    }
  }
}

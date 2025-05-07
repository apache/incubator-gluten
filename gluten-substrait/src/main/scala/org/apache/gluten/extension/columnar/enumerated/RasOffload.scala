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

import org.apache.gluten.execution.{GlutenPlan, ValidatablePlan}
import org.apache.gluten.extension.columnar.offload.OffloadSingleNode
import org.apache.gluten.extension.columnar.rewrite.RewriteSingleNode
import org.apache.gluten.extension.columnar.validator.Validator
import org.apache.gluten.ras.path.Pattern
import org.apache.gluten.ras.path.Pattern.node
import org.apache.gluten.ras.rule.{RasRule, Shape}
import org.apache.gluten.ras.rule.Shapes.pattern

import org.apache.spark.internal.Logging
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

  def from[T <: SparkPlan: ClassTag](base: OffloadSingleNode): RasOffload = {
    new RasOffload {
      override def offload(plan: SparkPlan): SparkPlan = base.offload(plan)
      override def typeIdentifier(): TypeIdentifier = TypeIdentifier.of[T]
    }
  }

  def from(identifier: SparkPlan => Boolean)(base: OffloadSingleNode): RasOffload = {
    new RasOffload {
      override def offload(plan: SparkPlan): SparkPlan = base.offload(plan)
      override def typeIdentifier(): TypeIdentifier = new TypeIdentifier {
        override def isInstance(node: SparkPlan): Boolean = identifier(node)
      }
    }
  }

  object Rule {
    def apply(
        base: RasOffload,
        validator: Validator,
        rewrites: Seq[RewriteSingleNode]): RasRule[SparkPlan] = {
      new RuleImpl(base, validator, rewrites)
    }

    private class RuleImpl(base: RasOffload, validator: Validator, rewrites: Seq[RewriteSingleNode])
      extends RasRule[SparkPlan]
      with Logging {
      private val typeIdentifier: TypeIdentifier = base.typeIdentifier()

      final override def shift(node: SparkPlan): Iterable[SparkPlan] = {
        // 0. If the node is already offloaded, fail fast.
        assert(typeIdentifier.isInstance(node))

        // 1. Pre-validate the input node. Fast fail if no good.
        validator.validate(node) match {
          case Validator.Passed =>
          case Validator.Failed(reason) =>
            // TODO: Tag the original plan with fallback reason.
            return List.empty
        }

        // 2. Rewrite the node to form that native library supports.
        val rewritten =
          try {
            rewrites.foldLeft(node) {
              case (node, rewrite) =>
                node.transformUp {
                  case p =>
                    val out = rewrite.rewrite(p)
                    out
                }
            }
          } catch {
            case e: Exception =>
              // TODO: Remove this catch block
              //  See https://github.com/apache/incubator-gluten/issues/7766
              logWarning(
                s"Exception thrown during rewriting the plan ${node.nodeName}. Skip offloading it",
                e)
              return List.empty
          }

        // 3. Walk the rewritten tree.
        var offloadSucceeded: Boolean = false
        val offloaded = rewritten.transformUp {
          case from if typeIdentifier.isInstance(from) =>
            // 4. Validate current node. If passed, offload it.
            validator.validate(from) match {
              case Validator.Passed =>
                val offloadedPlan = base.offload(from)
                val offloadedNodes = offloadedPlan.collect[ValidatablePlan] {
                  case t: ValidatablePlan => t
                }
                val outComes = offloadedNodes.map(_.doValidate()).filter(!_.ok())
                if (outComes.nonEmpty) {
                  // 5. If native validation fails on at least one of the offloaded nodes, return
                  // the original one.
                  //
                  // TODO: Tag the original plan with fallback reason. This is a non-trivial work
                  //  in RAS as the query plan we got here may be a copy so may not propagate tags
                  //  to original plan.
                  from
                } else {
                  offloadSucceeded = true
                  offloadedPlan
                }
              case Validator.Failed(reason) =>
                // TODO: Tag the original plan with fallback reason. This is a non-trivial work
                //  in RAS as the query plan we got here may be a copy so may not propagate tags
                //  to original plan.
                from
            }
        }

        // 6. If rewritten plan is not offload-able, discard it.
        if (!offloadSucceeded) {
          return List.empty
        }

        // 7. Otherwise, return the final tree.
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
}

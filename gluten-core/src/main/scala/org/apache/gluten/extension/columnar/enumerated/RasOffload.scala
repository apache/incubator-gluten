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
import org.apache.gluten.extension.columnar.rewrite.RewriteSingleNode
import org.apache.gluten.extension.columnar.validator.{Validator, Validators}
import org.apache.gluten.ras.rule.{RasRule, Shape, Shapes}

import org.apache.spark.sql.execution.SparkPlan

trait RasOffload extends RasRule[SparkPlan] {
  import RasOffload._

  final override def shift(node: SparkPlan): Iterable[SparkPlan] = {
    // 0. If the node is already offloaded, return fast.
    if (node.isInstanceOf[GlutenPlan]) {
      return List.empty
    }

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
      case from =>
        // 3. Validate current node. If passed, offload it.
        validator.validate(from) match {
          case Validator.Passed =>
            offload(from) match {
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

    // 5. Return the final tree.
    List(offloaded)
  }

  protected def offload(node: SparkPlan): SparkPlan

  final override def shape(): Shape[SparkPlan] = Shapes.fixedHeight(1)
}

object RasOffload {
  val validator = Validators
    .builder()
    .fallbackByHint()
    .fallbackIfScanOnly()
    .fallbackComplexExpressions()
    .fallbackByBackendSettings()
    .fallbackByUserOptions()
    .fallbackByTestInjects()
    .build()

  private val rewrites = RewriteSingleNode.allRules()
}

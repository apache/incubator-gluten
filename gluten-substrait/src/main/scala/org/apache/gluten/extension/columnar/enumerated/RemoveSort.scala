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

import org.apache.gluten.execution.{GlutenPlan, HashAggregateExecBaseTransformer, ShuffledHashJoinExecTransformerBase, SortExecTransformer}
import org.apache.gluten.ras.path.Pattern._
import org.apache.gluten.ras.path.Pattern.Matchers._
import org.apache.gluten.ras.rule.{RasRule, Shape}
import org.apache.gluten.ras.rule.Shapes._

import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.execution.SparkPlan

/**
 * Removes unnecessary sort if its parent doesn't require for sorted input.
 *
 * TODO: Sort's removal could be made much simpler once output ordering is added as a physical
 * property in RAS planer.
 */
object RemoveSort extends RasRule[SparkPlan] {
  private val appliedTypes: Seq[Class[_ <: GlutenPlan]] =
    List(classOf[HashAggregateExecBaseTransformer], classOf[ShuffledHashJoinExecTransformerBase])

  override def shift(node: SparkPlan): Iterable[SparkPlan] = {
    assert(node.isInstanceOf[GlutenPlan])
    val newChildren = node.requiredChildOrdering.zip(node.children).map {
      case (Nil, sort: SortExecTransformer) =>
        // Parent doesn't ask for sorted input from this child but a sort op was somehow added.
        // Remove it.
        sort.child
      case (req, child) =>
        // Parent asks for sorted input from this child. Do nothing but an assertion.
        assert(SortOrder.orderingSatisfies(child.outputOrdering, req))
        child
    }
    val out = List(node.withNewChildren(newChildren))
    out
  }
  override def shape(): Shape[SparkPlan] = pattern(
    branch2[SparkPlan](
      or(appliedTypes.map(clazz[SparkPlan](_)): _*),
      _ >= 1,
      _ => node(clazz(classOf[GlutenPlan]))
    ).build()
  )
}

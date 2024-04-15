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

import org.apache.gluten.execution.{BasicScanExecTransformer, FilterExecTransformerBase}
import org.apache.gluten.ras.path.Pattern._
import org.apache.gluten.ras.path.Pattern.Matchers._
import org.apache.gluten.ras.rule.{RasRule, Shape}
import org.apache.gluten.ras.rule.Shapes._

import org.apache.spark.sql.execution.SparkPlan

// Removes Gluten filter operator if its no-op. Typically a Gluten filter is no-op when it
// pushes all of its conditions into the child scan.
//
// The rule is needed in RAS since our cost model treats all filter + scan plans with constant cost
// because the pushed filter is not considered in the model. Removing the filter will make
// optimizer choose a single scan as the winner sub-plan since a single scan's cost is lower than
// filter + scan.
object FilterRemoveRule extends RasRule[SparkPlan] {
  override def shift(node: SparkPlan): Iterable[SparkPlan] = {
    val filter = node.asInstanceOf[FilterExecTransformerBase]
    if (filter.isNoop()) {
      return List(filter.child)
    }
    List.empty
  }

  override def shape(): Shape[SparkPlan] =
    pattern(
      node[SparkPlan](
        clazz(classOf[FilterExecTransformerBase]),
        leaf(clazz(classOf[BasicScanExecTransformer]))
      ).build())
}

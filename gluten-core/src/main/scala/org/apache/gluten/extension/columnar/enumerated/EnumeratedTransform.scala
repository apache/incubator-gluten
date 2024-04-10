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

import org.apache.gluten.extension.columnar.{TransformExchange, TransformJoin, TransformOthers, TransformSingleNode}
import org.apache.gluten.planner.GlutenOptimization
import org.apache.gluten.planner.property.Conventions
import org.apache.gluten.ras.property.PropertySet
import org.apache.gluten.ras.rule.{RasRule, Shape, Shapes}
import org.apache.gluten.utils.LogLevelUtil

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan

case class EnumeratedTransform(session: SparkSession, outputsColumnar: Boolean)
  extends Rule[SparkPlan]
  with LogLevelUtil {
  import EnumeratedTransform._

  private val rasRules = List(
    AsRasImplement(TransformOthers()),
    AsRasImplement(TransformExchange()),
    AsRasImplement(TransformJoin()),
    ImplementAggregate,
    ImplementFilter, // TODO pushed filter + scan should have lower cost
    PushFilterToScan
  )

  private val optimization = GlutenOptimization(rasRules)

  private val reqConvention = Conventions.ANY
  private val altConventions =
    Seq(Conventions.GLUTEN_COLUMNAR, Conventions.ROW_BASED)

  override def apply(plan: SparkPlan): SparkPlan = {
    val constraintSet = PropertySet(List(reqConvention))
    val altConstraintSets =
      altConventions.map(altConv => PropertySet(List(altConv)))
    val planner = optimization.newPlanner(plan, constraintSet, altConstraintSets)
    val out = planner.plan()
    out
  }
}

object EnumeratedTransform {
  private case class AsRasImplement(delegate: TransformSingleNode) extends RasRule[SparkPlan] {
    override def shift(node: SparkPlan): Iterable[SparkPlan] = {
      val out = List(delegate.impl(node))
      out
    }

    override def shape(): Shape[SparkPlan] = Shapes.fixedHeight(1)
  }
}

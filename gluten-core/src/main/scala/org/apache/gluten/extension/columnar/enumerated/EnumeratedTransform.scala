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

import org.apache.gluten.extension.columnar.{OffloadExchange, OffloadJoin, OffloadOthers, OffloadSingleNode}
import org.apache.gluten.planner.GlutenOptimization
import org.apache.gluten.planner.property.Conventions
import org.apache.gluten.ras.property.PropertySet
import org.apache.gluten.utils.LogLevelUtil

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan

case class EnumeratedTransform(session: SparkSession, outputsColumnar: Boolean)
  extends Rule[SparkPlan]
  with LogLevelUtil {
  import EnumeratedTransform._

  private val rules = List(
    new PushFilterToScan(RasOffload.validator),
    RemoveFilter
  )

  // TODO: Should obey ReplaceSingleNode#applyScanNotTransformable to select
  //  (vanilla) scan with cheaper sub-query plan through cost model.
  private val offloadRules = List(
    new AsRasOffload(OffloadOthers()),
    new AsRasOffload(OffloadExchange()),
    new AsRasOffload(OffloadJoin()),
    RasOffloadAggregate,
    RasOffloadFilter
  )

  private val optimization = GlutenOptimization(rules ++ offloadRules)

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

  /** Accepts a [[OffloadSingleNode]] rule to convert it into a RAS offload rule. */
  private class AsRasOffload(delegate: OffloadSingleNode) extends RasOffload {
    override protected def offload(node: SparkPlan): SparkPlan = {
      val out = delegate.offload(node)
      out
    }
  }
}

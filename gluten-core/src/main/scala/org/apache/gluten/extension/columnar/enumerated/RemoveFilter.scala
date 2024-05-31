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

import org.apache.gluten.execution._
import org.apache.gluten.metrics.MetricsUpdater
import org.apache.gluten.ras.path.Pattern._
import org.apache.gluten.ras.path.Pattern.Matchers._
import org.apache.gluten.ras.rule.{RasRule, Shape}
import org.apache.gluten.ras.rule.Shapes._
import org.apache.gluten.substrait.SubstraitContext

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.vectorized.ColumnarBatch

// Removes Gluten filter operator if its no-op. Typically a Gluten filter is no-op when it
// pushes all of its conditions into the child scan.
//
// The rule is needed in RAS since our cost model treats all filter + scan plans with constant cost
// because the pushed filter is not considered in the model. Removing the filter will make
// optimizer choose a single scan as the winner sub-plan since a single scan's cost is lower than
// filter + scan.
object RemoveFilter extends RasRule[SparkPlan] {
  override def shift(node: SparkPlan): Iterable[SparkPlan] = {
    val filter = node.asInstanceOf[FilterExecTransformerBase]
    if (filter.isNoop()) {
      val out = NoopFilter(filter.child, filter.output)
      return List(out)
    }
    List.empty
  }

  override def shape(): Shape[SparkPlan] =
    pattern(
      branch[SparkPlan](
        clazz(classOf[FilterExecTransformerBase]),
        leaf(clazz(classOf[BasicScanExecTransformer]))
      ).build())

  // A noop filter placeholder that indicates that all conditions are pushed down to scan.
  //
  // This operator has zero cost in cost model to avoid planner from choosing the
  // original filter-scan that doesn't have all conditions pushed down to scan.
  //
  // We cannot simply remove the filter to let planner choose the pushed scan since by vanilla
  // Spark's definition the filter may have different output nullability than scan. So
  // we have to keep this empty filter to let the optimized tree have the identical output schema
  // with the original tree. If we simply remove the filter, possible UBs might be caused. For
  // example, redundant broadcast exchanges may be added by EnsureRequirements because the
  // broadcast join detects that its join keys' nullabilities have been changed. Then AQE
  // re-optimization could be broken by ValidateSparkPlan so that AQE could completely
  // have no effect as if it's off. This case can be observed by explicitly setting a higher
  // AQE logger level to make sure the validation log doesn't get suppressed, e.g.,
  // spark.sql.adaptive.logLevel=ERROR.
  case class NoopFilter(override val child: SparkPlan, override val output: Seq[Attribute])
    extends UnaryTransformSupport {
    override def metricsUpdater(): MetricsUpdater = MetricsUpdater.None
    override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan = copy(newChild)
    override def outputPartitioning: Partitioning = child.outputPartitioning
    override def outputOrdering: Seq[SortOrder] = child.outputOrdering
    override protected def doTransform(context: SubstraitContext): TransformContext =
      child.asInstanceOf[TransformSupport].transform(context)
    override protected def doExecuteColumnar(): RDD[ColumnarBatch] = child.executeColumnar()
  }
}

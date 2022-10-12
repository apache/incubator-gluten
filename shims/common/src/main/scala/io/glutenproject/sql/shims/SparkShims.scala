/*
 * Copyright 2020 Intel Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.glutenproject.sql.shims


import io.glutenproject.BackendLib

import org.apache.spark.sql.catalyst.plans.physical.{Distribution, Partitioning}
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan

sealed abstract class ShimDescriptor

case class SparkShimDescriptor(major: Int, minor: Int, patch: Int) extends ShimDescriptor {
  override def toString(): String = s"$major.$minor.$patch"
}

trait SparkShims {
  def getShimDescriptor: ShimDescriptor

  // for this purpose, change HashClusteredDistribution to ClusteredDistribution
  // https://github.com/apache/spark/pull/32875
  def getDistribution(leftKeys: Seq[Expression], rightKeys: Seq[Expression]): Seq[Distribution]

  // https://issues.apache.org/jira/browse/SPARK-36745
  def applyPlan(plan: LogicalPlan, forceShuffledHashJoin: Boolean, backendLib: BackendLib)
  : Seq[SparkPlan]

}

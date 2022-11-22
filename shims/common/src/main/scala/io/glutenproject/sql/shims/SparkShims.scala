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
package io.glutenproject.sql.shims

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.physical.Distribution
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

  protected def sanityCheck(plan: SparkPlan): Boolean = plan.logicalLink.isDefined

  def supportAdaptiveWithExchangeConsidered(plan: SparkPlan): Boolean
}

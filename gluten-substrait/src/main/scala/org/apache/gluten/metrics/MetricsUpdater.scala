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
package org.apache.gluten.metrics

import org.apache.spark.sql.utils.SparkInputMetricsUtil.InputMetricsWrapper

/**
 * A minimized controller for updating operator's metrics, which means it never persists the
 * SparkPlan instance of the operator then the serialized RDD's size can be therefore minimized.
 *
 * TODO: place it to somewhere else since it's used not only by whole stage facilities.
 */
trait MetricsUpdater extends Serializable {
  def updateInputMetrics(inputMetrics: InputMetricsWrapper): Unit = {}
  def updateNativeMetrics(operatorMetrics: IOperatorMetrics): Unit = {}
}

object MetricsUpdater {
  // An empty metrics updater. Used when the operator generates native metrics but
  // it's yet unwanted to update the metrics in JVM side.
  object Todo extends MetricsUpdater {}

  // Used when the operator doesn't generate native metrics. It could be because
  // the operator doesn't generate any native query plan.
  object None extends MetricsUpdater {
    override def updateInputMetrics(inputMetrics: InputMetricsWrapper): Unit =
      throw new UnsupportedOperationException()
    override def updateNativeMetrics(operatorMetrics: IOperatorMetrics): Unit =
      throw new UnsupportedOperationException()
  }

  // Indicates a branch of a MetricsUpdaterTree is terminated. It's not bound to
  // any operators.
  object Terminate extends MetricsUpdater {
    override def updateInputMetrics(inputMetrics: InputMetricsWrapper): Unit =
      throw new UnsupportedOperationException()
    override def updateNativeMetrics(operatorMetrics: IOperatorMetrics): Unit =
      throw new UnsupportedOperationException()
  }
}

final case class MetricsUpdaterTree(updater: MetricsUpdater, children: Seq[MetricsUpdaterTree])

object MetricsUpdaterTree {}

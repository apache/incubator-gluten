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
package org.apache.gluten.extension

import org.apache.gluten.config.RssSortShuffleWriterType
import org.apache.gluten.config.SortShuffleWriterType
import org.apache.gluten.config.VeloxConfig
import org.apache.gluten.execution.VeloxResizeBatchesExec

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarShuffleExchangeExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{AQEShuffleReadExec, ShuffleQueryStageExec}

/**
 * Try to append [[VeloxResizeBatchesExec]] for shuffle input and output to make the batch sizes in
 * good shape.
 */
case class AppendBatchResizeForShuffleInputAndOutput() extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    val resizeBatchesShuffleInputEnabled = VeloxConfig.get.veloxResizeBatchesShuffleInput
    val resizeBatchesShuffleOutputEnabled = VeloxConfig.get.veloxResizeBatchesShuffleOutput
    if (!resizeBatchesShuffleInputEnabled && !resizeBatchesShuffleOutputEnabled) {
      return plan
    }

    val range = VeloxConfig.get.veloxResizeBatchesShuffleInputOutputRange
    plan.transformUp {
      case ColumnarResizeableShuffleExchangeExec(shuffle) if resizeBatchesShuffleInputEnabled =>
        val appendBatches =
          VeloxResizeBatchesExec(shuffle.child, range.min, range.max)
        shuffle.withNewChildren(Seq(appendBatches))
      case a @ AQEShuffleReadExec(
            ShuffleQueryStageExec(_, ColumnarResizeableShuffleExchangeExec(_), _),
            _) if resizeBatchesShuffleOutputEnabled =>
        VeloxResizeBatchesExec(a, range.min, range.max)
      // Since it's transformed in a bottom to up order, so we may first encountered
      // ShuffeQueryStageExec, which is transformed to VeloxResizeBatchesExec(ShuffeQueryStageExec),
      // then we see AQEShuffleReadExec
      case a @ AQEShuffleReadExec(
            VeloxResizeBatchesExec(
              s @ ShuffleQueryStageExec(_, ColumnarResizeableShuffleExchangeExec(_), _),
              _,
              _),
            _) if resizeBatchesShuffleOutputEnabled =>
        VeloxResizeBatchesExec(a.copy(child = s), range.min, range.max)
      case s @ ShuffleQueryStageExec(_, ColumnarResizeableShuffleExchangeExec(_), _)
          if resizeBatchesShuffleOutputEnabled =>
        VeloxResizeBatchesExec(s, range.min, range.max)
    }
  }

  private object ColumnarResizeableShuffleExchangeExec {
    def unapply(plan: SparkPlan): Option[ColumnarShuffleExchangeExec] = {
      plan match {
        // sort-based/rss-sort shuffle has already resized the batch size in shuffle read,
        // so no need to resize again
        case shuffle: ColumnarShuffleExchangeExec
            if shuffle.shuffleWriterType == SortShuffleWriterType
              || shuffle.shuffleWriterType == RssSortShuffleWriterType =>
          None
        case shuffle: ColumnarShuffleExchangeExec => Some(shuffle)
        case _ => None
      }
    }
  }
}

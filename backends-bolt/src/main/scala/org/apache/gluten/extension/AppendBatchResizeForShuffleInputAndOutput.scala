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

import org.apache.gluten.config.BoltConfig
import org.apache.gluten.config.HashShuffleWriterType
import org.apache.gluten.execution.BoltResizeBatchesExec

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarShuffleExchangeExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{AQEShuffleReadExec, ShuffleQueryStageExec}

/**
 * Try to append [[BoltResizeBatchesExec]] for shuffle input and output to make the batch sizes in
 * good shape.
 */
case class AppendBatchResizeForShuffleInputAndOutput() extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    val range = BoltConfig.get.boltResizeBatchesShuffleInputOutputRange
    plan.transformUp {
      case shuffle: ColumnarShuffleExchangeExec
          if shuffle.shuffleWriterType == HashShuffleWriterType &&
            BoltConfig.get.boltResizeBatchesShuffleInput =>
        val appendBatches =
          BoltResizeBatchesExec(shuffle.child, range.min, range.max)
        shuffle.withNewChildren(Seq(appendBatches))
      case a @ AQEShuffleReadExec(ShuffleQueryStageExec(_, _: ColumnarShuffleExchangeExec, _), _)
          if BoltConfig.get.boltResizeBatchesShuffleOutput =>
        BoltResizeBatchesExec(a, range.min, range.max)
      // Since it's transformed in a bottom to up order, so we may first encountered
      // ShuffeQueryStageExec, which is transformed to BoltResizeBatchesExec(ShuffeQueryStageExec),
      // then we see AQEShuffleReadExec
      case a @ AQEShuffleReadExec(
            BoltResizeBatchesExec(
              s @ ShuffleQueryStageExec(_, _: ColumnarShuffleExchangeExec, _),
              _,
              _),
            _) if BoltConfig.get.boltResizeBatchesShuffleOutput =>
        BoltResizeBatchesExec(a.copy(child = s), range.min, range.max)
      case s @ ShuffleQueryStageExec(_, _: ColumnarShuffleExchangeExec, _)
          if BoltConfig.get.boltResizeBatchesShuffleOutput =>
        BoltResizeBatchesExec(s, range.min, range.max)
    }
  }
}

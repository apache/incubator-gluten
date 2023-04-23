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

package io.glutenproject.utils

import io.glutenproject.execution.CoalesceBatchesExec
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{ColumnarShuffleExchangeExec, SparkPlan}
import org.apache.spark.sql.execution.exchange.{ShuffleExchangeExec}

object ColumnarShuffleUtil {

  /**
   * Generate a columnar plan for shuffle exchange.
   *
   * @param plan             the spark plan of shuffle exchange.
   * @param child            the child of shuffle exchange.
   * @param removeHashColumn whether the hash column should be removed.
   * @return a columnar shuffle exchange.
   */
  def genColumnarShuffleExchange(plan: ShuffleExchangeExec,
                                 child: SparkPlan,
                                 isAdaptiveContextOrTopParentExchange: Boolean,
                                 shuffleOutputAttributes: Seq[Attribute]): SparkPlan = {
    if (isAdaptiveContextOrTopParentExchange) {
      ColumnarShuffleExchangeExec(plan.outputPartitioning, child,
        plan.shuffleOrigin, shuffleOutputAttributes)
    } else {
      CoalesceBatchesExec(ColumnarShuffleExchangeExec(
        plan.outputPartitioning, child, plan.shuffleOrigin, shuffleOutputAttributes))
    }
  }
}

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

package org.apache.gluten.columnarbatch

import org.apache.gluten.extension.columnar.transition.Convention

import org.apache.spark.sql.execution.{ColumnarToRowExec, SparkPlan}

/**
 * ArrowBatch stands for Gluten's Arrow-based columnar batch implementation. Vanilla Spark's
 * ColumnarBatch consisting of [[org.apache.spark.sql.vectorized.ArrowColumnVector]]s is still
 * treated as [[Convention.BatchType.VanillaBatch]].
 *
 * As of now, ArrowBatch should have [[org.apache.gluten.vectorized.ArrowWritableColumnVector]]s
 * populated in it. ArrowBatch can be loaded from / offloaded to native to C++ ArrowColumnarBatch
 * through API in [[ColumnarBatches]]. After being offloaded, ArrowBatch is no longer considered a
 * legal ArrowBatch and cannot be accepted by trivial ColumnarToRowExec. To follow that rule, Any
 * plan with this batch type should promise it emits loaded batch only.
 */
object ArrowBatch extends Convention.BatchType {
  toRow(
    () =>
      (plan: SparkPlan) => {
        ColumnarToRowExec(plan)
      })
}

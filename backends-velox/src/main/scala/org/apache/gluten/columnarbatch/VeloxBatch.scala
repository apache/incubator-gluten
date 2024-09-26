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

import org.apache.gluten.execution.{LoadArrowDataExec, OffloadArrowDataExec, RowToVeloxColumnarExec, VeloxColumnarToRowExec}
import org.apache.gluten.extension.columnar.transition.{Convention, TransitionDef}

import org.apache.spark.sql.execution.SparkPlan

object VeloxBatch extends Convention.BatchType {
  fromRow(
    () =>
      (plan: SparkPlan) => {
        RowToVeloxColumnarExec(plan)
      })

  toRow(
    () =>
      (plan: SparkPlan) => {
        VeloxColumnarToRowExec(plan)
      })

  // TODO: Add explicit transitions between Arrow native batch and Velox batch.
  //  See https://github.com/apache/incubator-gluten/issues/7313.

  fromBatch(
    ArrowBatches.ArrowJavaBatch,
    () =>
      (plan: SparkPlan) => {
        OffloadArrowDataExec(plan)
      })

  toBatch(
    ArrowBatches.ArrowJavaBatch,
    () =>
      (plan: SparkPlan) => {
        LoadArrowDataExec(plan)
      })

  fromBatch(
    ArrowBatches.ArrowNativeBatch,
    () =>
      (plan: SparkPlan) => {
        LoadArrowDataExec(plan)
      })

  toBatch(ArrowBatches.ArrowNativeBatch, TransitionDef.empty)
}

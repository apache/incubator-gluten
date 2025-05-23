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
package org.apache.gluten.backendsapi.clickhouse

import org.apache.gluten.extension.columnar.transition.Convention

import org.apache.spark.sql.execution.{CHColumnarToRowExec, RowToCHNativeColumnarExec}

/**
 * ClickHouse batch convention.
 *
 * [[fromRow]] and [[toRow]] need a
 * [[org.apache.gluten.extension.columnar.transition.TransitionDef]] instance. The scala allows an
 * compact way to implement trait using a lambda function.
 *
 * Here the detail definition is given in [[CHBatchType.fromRow]].
 * {{{
 *       fromRow(new TransitionDef {
 *       override def create(): Transition = new Transition {
 *         override protected def apply0(plan: SparkPlan): SparkPlan =
 *           RowToCHNativeColumnarExec(plan)
 *       }
 *     })
 * }}}
 */
object CHBatchType extends Convention.BatchType {
  override protected def registerTransitions(): Unit = {
    fromRow(Convention.RowType.VanillaRowType, RowToCHNativeColumnarExec.apply)
    toRow(Convention.RowType.VanillaRowType, CHColumnarToRowExec.apply)
  }
}

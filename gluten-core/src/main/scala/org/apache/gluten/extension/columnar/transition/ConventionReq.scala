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
package org.apache.gluten.extension.columnar.transition

import org.apache.spark.sql.execution.SparkPlan

/**
 * ConventionReq describes the requirement for [[Convention]]. This is mostly used in determining
 * the acceptable conventions for its children of a parent plan node.
 */
sealed trait ConventionReq {
  def requiredRowType: ConventionReq.RowType
  def requiredBatchType: ConventionReq.BatchType
}

object ConventionReq {
  sealed trait RowType

  object RowType {
    final case object Any extends RowType
    final case class Is(t: Convention.RowType) extends RowType {
      assert(t != Convention.RowType.None)
    }
  }

  sealed trait BatchType

  object BatchType {
    final case object Any extends BatchType
    final case class Is(t: Convention.BatchType) extends BatchType {
      assert(t != Convention.BatchType.None)
    }
  }

  private case class Impl(
      override val requiredRowType: RowType,
      override val requiredBatchType: BatchType
  ) extends ConventionReq

  val any: ConventionReq = of(RowType.Any, BatchType.Any)
  val vanillaRow: ConventionReq = ofRow(RowType.Is(Convention.RowType.VanillaRowType))
  val vanillaBatch: ConventionReq = ofBatch(BatchType.Is(Convention.BatchType.VanillaBatchType))

  def get(plan: SparkPlan): Seq[ConventionReq] = ConventionFunc.create().conventionReqOf(plan)
  def of(rowType: RowType, batchType: BatchType): ConventionReq = Impl(rowType, batchType)
  def ofRow(rowType: RowType): ConventionReq = Impl(rowType, BatchType.Any)
  def ofBatch(batchType: BatchType): ConventionReq = Impl(RowType.Any, batchType)

  trait KnownChildConvention {
    def requiredChildConvention(): Seq[ConventionReq]
  }
}

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

import org.apache.spark.sql.execution.{ColumnarToRowExec, RowToColumnarExec, SparkPlan}

/**
 * Convention of a query plan consists of the row data type and columnar data type it supports to
 * output.
 */
sealed trait Convention {
  def rowType: Convention.RowType
  def batchType: Convention.BatchType
}

object Convention {
  implicit class ConventionOps(val convention: Convention) extends AnyVal {
    def isNone: Boolean = {
      convention.rowType == RowTypes.None && convention.batchType == BatchTypes.None
    }
  }
  private case class Impl(override val rowType: RowType, override val batchType: BatchType)
    extends Convention

  def get(plan: SparkPlan): Convention = {
    ConventionFunc.create().conventionOf(plan)
  }

  def of(rowType: RowType, batchType: BatchType): Convention = {
    Impl(rowType, batchType)
  }

  sealed trait RowType

  object RowTypes {
    // None indicates that the plan doesn't support row-based processing.
    final case object None extends RowType
    final case object VanillaRow extends RowType
  }

  trait BatchType {
    final def fromRow(transitionDef: TransitionDef): Unit = {
      Transition.factory.update().defineFromRowTransition(this, transitionDef)
    }

    final def toRow(transitionDef: TransitionDef): Unit = {
      Transition.factory.update().defineToRowTransition(this, transitionDef)
    }

    final def fromBatch(from: BatchType, transitionDef: TransitionDef): Unit = {
      assert(from != this)
      Transition.factory.update().defineBatchTransition(from, this, transitionDef)
    }

    final def toBatch(to: BatchType, transitionDef: TransitionDef): Unit = {
      assert(to != this)
      Transition.factory.update().defineBatchTransition(this, to, transitionDef)
    }
  }

  object BatchTypes {
    // None indicates that the plan doesn't support batch-based processing.
    final case object None extends BatchType
    final case object VanillaBatch extends BatchType {
      fromRow(
        () =>
          (plan: SparkPlan) => {
            RowToColumnarExec(plan)
          })

      toRow(
        () =>
          (plan: SparkPlan) => {
            ColumnarToRowExec(plan)
          })
    }
  }
}

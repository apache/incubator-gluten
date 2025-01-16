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
import org.apache.spark.util.SparkVersionUtil

import scala.collection.mutable

/**
 * Convention of a query plan consists of the row data type and columnar data type it supports to
 * output.
 */
sealed trait Convention {
  def rowType: Convention.RowType
  def batchType: Convention.BatchType
}

object Convention {
  def ensureSparkRowAndBatchTypesRegistered(): Unit = {
    RowType.None.ensureRegistered()
    RowType.VanillaRow.ensureRegistered()
    BatchType.None.ensureRegistered()
    BatchType.VanillaBatch.ensureRegistered()
  }

  implicit class ConventionOps(val conv: Convention) extends AnyVal {
    def isNone: Boolean = {
      conv.rowType == RowType.None && conv.batchType == BatchType.None
    }

    def &&(other: Convention): Convention = {
      def rowType(): RowType = {
        if (conv.rowType == other.rowType) {
          return conv.rowType
        }
        RowType.None
      }
      def batchType(): BatchType = {
        if (conv.batchType == other.batchType) {
          return conv.batchType
        }
        BatchType.None
      }
      Convention.of(rowType(), batchType())
    }

    def asReq(): ConventionReq = {
      val rowTypeReq = conv.rowType match {
        case Convention.RowType.None => ConventionReq.RowType.Any
        case r => ConventionReq.RowType.Is(r)
      }

      val batchTypeReq = conv.batchType match {
        case Convention.BatchType.None => ConventionReq.BatchType.Any
        case b => ConventionReq.BatchType.Is(b)
      }
      ConventionReq.of(rowTypeReq, batchTypeReq)
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

  sealed trait RowType extends TransitionGraph.Vertex with Serializable {
    import RowType._

    final protected[this] def register0(): Unit = BatchType.synchronized {
      assert(all.add(this))
    }
  }

  object RowType {
    private val all: mutable.Set[RowType] = mutable.Set()
    def values(): Set[RowType] = all.toSet

    // None indicates that the plan doesn't support row-based processing.
    final case object None extends RowType
    final case object VanillaRow extends RowType
  }

  trait BatchType extends TransitionGraph.Vertex with Serializable {
    import BatchType._

    final protected[this] def register0(): Unit = BatchType.synchronized {
      assert(all.add(this))
      registerTransitions()
    }

    /**
     * User batch type could override this method to define transitions from/to this batch type by
     * calling the subsequent protected APIs.
     */
    protected[this] def registerTransitions(): Unit

    final protected[this] def fromRow(transition: Transition): Unit = {
      Transition.factory.update(graph => graph.addEdge(RowType.VanillaRow, this, transition))
    }

    final protected[this] def toRow(transition: Transition): Unit = {
      Transition.factory.update(graph => graph.addEdge(this, RowType.VanillaRow, transition))
    }

    final protected[this] def fromBatch(from: BatchType, transition: Transition): Unit = {
      assert(from != this)
      Transition.factory.update(graph => graph.addEdge(from, this, transition))
    }

    final protected[this] def toBatch(to: BatchType, transition: Transition): Unit = {
      assert(to != this)
      Transition.factory.update(graph => graph.addEdge(this, to, transition))
    }
  }

  object BatchType {
    private val all: mutable.Set[BatchType] = mutable.Set()
    def values(): Set[BatchType] = all.toSet
    // None indicates that the plan doesn't support batch-based processing.
    final case object None extends BatchType {
      override protected[this] def registerTransitions(): Unit = {}
    }
    final case object VanillaBatch extends BatchType {
      override protected[this] def registerTransitions(): Unit = {
        fromRow(RowToColumnarExec.apply)
        toRow(ColumnarToRowExec.apply)
      }
    }
  }

  trait KnownBatchType {
    def batchType(): BatchType
  }

  sealed trait KnownRowType {
    def rowType(): RowType
  }

  trait KnownRowTypeForSpark33OrLater extends KnownRowType {
    this: SparkPlan =>
    import KnownRowTypeForSpark33OrLater._

    final override def rowType(): RowType = {
      if (lteSpark32) {
        // It's known that in Spark 3.2, one Spark plan node is considered either only having
        // row-based support or only having columnar support at a time.
        // Hence, if the plan supports columnar output, we'd disable its row-based support.
        // The same for the opposite.
        if (supportsColumnar) {
          Convention.RowType.None
        } else {
          Convention.RowType.VanillaRow
        }
      } else {
        rowType0()
      }
    }

    def rowType0(): RowType
  }

  object KnownRowTypeForSpark33OrLater {
    private val lteSpark32: Boolean = {
      val v = SparkVersionUtil.majorMinorVersion()
      SparkVersionUtil.compareMajorMinorVersion(v, (3, 2)) <= 0
    }
  }
}

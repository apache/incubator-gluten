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

package io.glutenproject.execution

import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.columnarbatch.ArrowColumnarBatches
import io.glutenproject.memory.arrowalloc.ArrowBufferAllocators
import io.glutenproject.utils.{LogicalPlanSelector, QueryPlanSelector}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, OrderPreservingUnaryNode}
import org.apache.spark.sql.execution.{ColumnarBroadcastExchangeExec, ColumnarShuffleExchangeExec, ColumnarToRowExec, ColumnarToRowTransition, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.types.{DataType, Decimal}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

class FakeRow(val batch: ColumnarBatch) extends InternalRow {
  override def numFields: Int = throw new UnsupportedOperationException()

  override def setNullAt(i: Int): Unit = throw new UnsupportedOperationException()

  override def update(i: Int, value: Any): Unit = throw new UnsupportedOperationException()

  override def copy(): InternalRow = throw new UnsupportedOperationException()

  override def isNullAt(ordinal: Int): Boolean = throw new UnsupportedOperationException()

  override def getBoolean(ordinal: Int): Boolean = throw new UnsupportedOperationException()

  override def getByte(ordinal: Int): Byte = throw new UnsupportedOperationException()

  override def getShort(ordinal: Int): Short = throw new UnsupportedOperationException()

  override def getInt(ordinal: Int): Int = throw new UnsupportedOperationException()

  override def getLong(ordinal: Int): Long = throw new UnsupportedOperationException()

  override def getFloat(ordinal: Int): Float = throw new UnsupportedOperationException()

  override def getDouble(ordinal: Int): Double = throw new UnsupportedOperationException()

  override def getDecimal(ordinal: Int, precision: Int, scale: Int): Decimal =
    throw new UnsupportedOperationException()

  override def getUTF8String(ordinal: Int): UTF8String =
    throw new UnsupportedOperationException()

  override def getBinary(ordinal: Int): Array[Byte] = throw new UnsupportedOperationException()

  override def getInterval(ordinal: Int): CalendarInterval =
    throw new UnsupportedOperationException()

  override def getStruct(ordinal: Int, numFields: Int): InternalRow =
    throw new UnsupportedOperationException()

  override def getArray(ordinal: Int): ArrayData = throw new UnsupportedOperationException()

  override def getMap(ordinal: Int): MapData = throw new UnsupportedOperationException()

  override def get(ordinal: Int, dataType: DataType): AnyRef =
    throw new UnsupportedOperationException()
}

case class ColumnarToFakeRowStrategy(session: SparkSession) extends Strategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = LogicalPlanSelector.maybeNil(session,
    plan) {plan match {
    case ColumnarToFakeRowLogicAdaptor(child: LogicalPlan) =>
      Seq(ColumnarToFakeRowAdaptor(planLater(child)))
    case other =>
      Nil
  }}
}

private case class ColumnarToFakeRowLogicAdaptor(child: LogicalPlan)
  extends OrderPreservingUnaryNode {
  override def output: Seq[Attribute] = child.output

  // For spark 3.2.
  protected def withNewChildInternal(newChild: LogicalPlan): ColumnarToFakeRowLogicAdaptor =
    copy(child = newChild)
}

/**
 * To wrap children's columnar output within `FakeRow`s. This is usually used in data writing
 * since Spark doesn't expose APIs to write columnar data as of now.
 */
case class ColumnarToFakeRowAdaptor(child: SparkPlan) extends ColumnarToRowTransition {
  if (child.logicalLink.isDefined) {
    setLogicalLink(ColumnarToFakeRowLogicAdaptor(child.logicalLink.get))
  }

  override def output: Seq[Attribute] = child.output

  override protected def doExecute(): RDD[InternalRow] = {
    child.executeColumnar().map { cb => new FakeRow(cb) }
  }

  // For spark 3.2.
  protected def withNewChildInternal(newChild: SparkPlan): ColumnarToFakeRowAdaptor =
    copy(child = newChild)
}

case class LoadArrowData(child: SparkPlan) extends UnaryExecNode {
  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(
      "LoadArrowData does not support the execute() code path.")
  }

  override def nodeName: String = "LoadArrowData"

  override def supportsColumnar: Boolean = true


  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    child.executeColumnar().mapPartitions { itr =>
      BackendsApiManager.getIteratorApiInstance.genCloseableColumnBatchIterator(
        itr.map { cb =>
          ArrowColumnarBatches.ensureLoaded(ArrowBufferAllocators.contextInstance(), cb)
        })
    }
  }

  override def output: Seq[Attribute] = {
    child.output
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
}

object GlutenColumnarRules {
  // Load when fallback to vanilla columnar to row
  // Remove it when supports all the spark type velox columnar to row
  case class LoadBeforeColumnarToRow() extends Rule[SparkPlan] {
    override def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
      case c2r @ ColumnarToRowExec(_: ColumnarShuffleExchangeExec) =>
        c2r // AdaptiveSparkPlanExec.scala:536
      case c2r @ ColumnarToRowExec(_: ColumnarBroadcastExchangeExec) =>
        c2r // AdaptiveSparkPlanExec.scala:546
      case ColumnarToRowExec(child) => ColumnarToRowExec(LoadArrowData(child))
    }
  }

  object DummyRule extends Rule[SparkPlan] {
    def apply(p: SparkPlan): SparkPlan = p
  }
}

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

package org.apache.spark.sql

import io.glutenproject.columnarbatch.ArrowColumnarBatches
import io.glutenproject.execution.VeloxRowToArrowColumnarExec
import io.glutenproject.memory.arrowalloc.ArrowBufferAllocators

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, OrderPreservingUnaryNode}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand
import org.apache.spark.sql.execution.datasources.velox.DwrfFileFormat
import org.apache.spark.sql.types.{DataType, Decimal}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

object VeloxColumnarRules {

  case class OtherWritePostRule(session: SparkSession) extends Rule[SparkPlan] {
    override def apply(plan: SparkPlan): SparkPlan = plan match {
      case rc@DataWritingCommandExec(cmd, ColumnarToRowExec(child)) =>
        cmd match {
          case command: InsertIntoHadoopFsRelationCommand =>
            if (command.fileFormat.isInstanceOf[DwrfFileFormat]) {
              rc.withNewChildren(Array(ColumnarToFakeRowAdaptor(child)))
            } else {
              plan.withNewChildren(plan.children.map(apply))
            }
          case _ => plan.withNewChildren(plan.children.map(apply))
        }
      case rc@DataWritingCommandExec(cmd, child) =>
        cmd match {
          case command: InsertIntoHadoopFsRelationCommand =>
            if (command.fileFormat.isInstanceOf[DwrfFileFormat]) {
              child match {
                case c: AdaptiveSparkPlanExec =>
                  rc.withNewChildren(
                    Array(
                      AdaptiveSparkPlanExec(
                        ColumnarToFakeRowAdaptor(c.inputPlan),
                        c.context,
                        c.preprocessingRules,
                        c.isSubquery)))
                case other =>
                  rc.withNewChildren(
                    Array(ColumnarToFakeRowAdaptor(new VeloxRowToArrowColumnarExec(child))))
              }
            } else {
              plan.withNewChildren(plan.children.map(apply))
            }
          case _ => plan.withNewChildren(plan.children.map(apply))
        }
      case plan: SparkPlan => plan.withNewChildren(plan.children.map(apply))
    }
  }

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

  case class SimpleStrategy() extends Strategy {
    override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case ColumnarToFakeRowLogicAdaptor(child: LogicalPlan) =>
        Seq(ColumnarToFakeRowAdaptor(planLater(child)))
      case other =>
        Nil
    }
  }

  private case class ColumnarToFakeRowLogicAdaptor(child: LogicalPlan)
    extends OrderPreservingUnaryNode {
    override def output: Seq[Attribute] = child.output

    // For spark 3.2.
    protected def withNewChildInternal(newChild: LogicalPlan): ColumnarToFakeRowLogicAdaptor =
      copy(child = newChild)
  }

  case class ColumnarToFakeRowAdaptor(child: SparkPlan) extends ColumnarToRowTransition {
    if (!child.logicalLink.isEmpty) {
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

  case class VeloxLoadArrowData(child: SparkPlan) extends UnaryExecNode {

    override protected def doExecute(): RDD[InternalRow] = {
      throw new UnsupportedOperationException(
        "VeloxLoadArrowData does not support the execute() code path.")
    }

    override def nodeName: String = "VeloxLoadArrowData"

    override def supportsColumnar: Boolean = true


    override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
      child.executeColumnar().mapPartitions { itr =>
        itr.map { cb =>
          ArrowColumnarBatches.ensureLoaded(ArrowBufferAllocators.contextInstance(), cb)
        }
      }
    }

    override def output: Seq[Attribute] = {
      child.output
    }

    override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
      copy(child = newChild)
  }

  case class LoadBeforeColumnarToRow() extends Rule[SparkPlan] {
    override def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
      case c2r @ ColumnarToRowExec(child: ColumnarShuffleExchangeAdaptor) =>
        c2r // AdaptiveSparkPlanExec.scala:536
      case c2r @ ColumnarToRowExec(child: ColumnarBroadcastExchangeAdaptor) =>
        c2r // AdaptiveSparkPlanExec.scala:546
      case ColumnarToRowExec(child) => ColumnarToRowExec(VeloxLoadArrowData(child))
    }
  }

  object DummyRule extends Rule[SparkPlan] {
    def apply(p: SparkPlan): SparkPlan = p
  }
}

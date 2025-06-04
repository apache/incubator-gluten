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
package org.apache.gluten.extension.columnar.enumerated.planner.plan

import org.apache.gluten.execution.GlutenPlan
import org.apache.gluten.extension.columnar.enumerated.planner.metadata.{GlutenMetadata, LogicalLink}
import org.apache.gluten.extension.columnar.enumerated.planner.property.Conv
import org.apache.gluten.extension.columnar.transition.{Convention, ConventionReq}
import org.apache.gluten.ras.GroupLeafBuilder

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.{LeafExecNode, SparkPlan}

import java.util.concurrent.atomic.AtomicBoolean

// TODO: Make this inherit from GlutenPlan.
case class GroupLeafExec(groupId: Int, metadata: GlutenMetadata, convReq: Conv.Req)
  extends LeafExecNode
  with Convention.KnownBatchType
  with Convention.KnownRowTypeForSpark33OrLater
  with GlutenPlan.SupportsRowBasedCompatible {

  private val frozen = new AtomicBoolean(false)

  // Set the logical link then make the plan node immutable. All future
  // mutable operations related to tagging will be aborted.
  if (metadata.logicalLink() != LogicalLink.notFound) {
    setLogicalLink(metadata.logicalLink().plan)
  }
  frozen.set(true)

  override protected def doExecute(): RDD[InternalRow] = throw new IllegalStateException()
  override def output: Seq[Attribute] = metadata.schema().output

  override val batchType: Convention.BatchType = {
    val out = convReq.req.requiredBatchType match {
      case ConventionReq.BatchType.Any => Convention.BatchType.None
      case ConventionReq.BatchType.Is(b) => b
    }
    out
  }

  final override val supportsColumnar: Boolean = {
    batchType != Convention.BatchType.None
  }

  override val rowType0: Convention.RowType = {
    val out = convReq.req.requiredRowType match {
      case ConventionReq.RowType.Any => Convention.RowType.VanillaRowType
      case ConventionReq.RowType.Is(r) => r
    }
    out
  }

  final override val supportsRowBased: Boolean = {
    rowType() != Convention.RowType.None
  }

  private def ensureNotFrozen(): Unit = {
    if (frozen.get()) {
      throw new UnsupportedOperationException()
    }
  }

  // Enclose mutable APIs.
  override def setLogicalLink(logicalPlan: LogicalPlan): Unit = {
    ensureNotFrozen()
    super.setLogicalLink(logicalPlan)
  }
  override def setTagValue[T](tag: TreeNodeTag[T], value: T): Unit = {
    ensureNotFrozen()
    super.setTagValue(tag, value)
  }
  override def unsetTagValue[T](tag: TreeNodeTag[T]): Unit = {
    ensureNotFrozen()
    super.unsetTagValue(tag)
  }
  override def copyTagsFrom(other: SparkPlan): Unit = {
    ensureNotFrozen()
    super.copyTagsFrom(other)
  }
}

object GroupLeafExec {
  class Builder private[GroupLeafExec] (override val id: Int) extends GroupLeafBuilder[SparkPlan] {
    private var convReq: Conv.Req = _
    private var metadata: GlutenMetadata = _

    def withMetadata(metadata: GlutenMetadata): Builder = {
      this.metadata = metadata
      this
    }

    def withConvReq(convReq: Conv.Req): Builder = {
      this.convReq = convReq
      this
    }

    override def build(): SparkPlan = {
      require(metadata != null)
      require(convReq != null)
      GroupLeafExec(id, metadata, convReq)
    }
  }

  def newBuilder(groupId: Int): Builder = {
    new Builder(groupId)
  }
}

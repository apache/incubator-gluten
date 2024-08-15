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
package org.apache.gluten.planner.plan

import org.apache.gluten.extension.columnar.transition.{Convention, ConventionReq}
import org.apache.gluten.extension.columnar.transition.Convention.{KnownBatchType, KnownRowType}
import org.apache.gluten.planner.metadata.GlutenMetadata
import org.apache.gluten.planner.property.{Conv, ConvDef}
import org.apache.gluten.ras.{Metadata, PlanModel}
import org.apache.gluten.ras.property.PropertySet
import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{ColumnarToRowExec, LeafExecNode, SparkPlan}
import org.apache.spark.util.{SparkTaskUtil, TaskResources}

import java.util.{Objects, Properties}

object GlutenPlanModel {
  def apply(): PlanModel[SparkPlan] = {
    PlanModelImpl
  }

  case class GroupLeafExec(
      groupId: Int,
      metadata: GlutenMetadata,
      constraintSet: PropertySet[SparkPlan])
    extends LeafExecNode
    with KnownBatchType
    with KnownRowType {
    private val req: Conv.Req = constraintSet.get(ConvDef).asInstanceOf[Conv.Req]

    override protected def doExecute(): RDD[InternalRow] = throw new IllegalStateException()
    override def output: Seq[Attribute] = metadata.schema().output

    override def supportsColumnar: Boolean = {
      batchType != Convention.BatchType.None
    }

    override val batchType: Convention.BatchType = {
      val out = req.req.requiredBatchType match {
        case ConventionReq.BatchType.Any => Convention.BatchType.None
        case ConventionReq.BatchType.Is(b) => b
      }
      out
    }

    override val rowType: Convention.RowType = {
      val out = req.req.requiredRowType match {
        case ConventionReq.RowType.Any => Convention.RowType.None
        case ConventionReq.RowType.Is(r) => r
      }
      out
    }
  }

  private object PlanModelImpl extends PlanModel[SparkPlan] {
    private val fakeTc = SparkShimLoader.getSparkShims.createTestTaskContext(new Properties())
    private def fakeTc[T](body: => T): T = {
      assert(!TaskResources.inSparkTask())
      SparkTaskUtil.setTaskContext(fakeTc)
      try {
        body
      } finally {
        SparkTaskUtil.unsetTaskContext()
      }
    }

    override def childrenOf(node: SparkPlan): Seq[SparkPlan] = node.children

    override def withNewChildren(node: SparkPlan, children: Seq[SparkPlan]): SparkPlan =
      node match {
        case c2r: ColumnarToRowExec =>
          // Workaround: To bypass the assertion in ColumnarToRowExec's code if child is
          // a group leaf.
          fakeTc {
            c2r.withNewChildren(children)
          }
        case other =>
          other.withNewChildren(children)
      }

    override def hashCode(node: SparkPlan): Int = Objects.hashCode(node)

    override def equals(one: SparkPlan, other: SparkPlan): Boolean = Objects.equals(one, other)

    override def newGroupLeaf(
        groupId: Int,
        metadata: Metadata,
        constraintSet: PropertySet[SparkPlan]): SparkPlan =
      GroupLeafExec(groupId, metadata.asInstanceOf[GlutenMetadata], constraintSet)

    override def isGroupLeaf(node: SparkPlan): Boolean = node match {
      case _: GroupLeafExec => true
      case _ => false
    }

    override def getGroupId(node: SparkPlan): Int = node match {
      case gl: GroupLeafExec => gl.groupId
      case _ => throw new IllegalStateException()
    }
  }
}

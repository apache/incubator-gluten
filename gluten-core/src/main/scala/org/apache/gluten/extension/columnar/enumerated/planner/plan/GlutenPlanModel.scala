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

import org.apache.gluten.ras.PlanModel
import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{ColumnarToRowExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanExecBase
import org.apache.spark.task.{SparkTaskUtil, TaskResources}

import java.util.{Objects, Properties}

object GlutenPlanModel {
  def apply(): PlanModel[SparkPlan] = {
    PlanModelImpl
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

    override def hashCode(node: SparkPlan): Int = Objects.hashCode(withEqualityWrapper(node))

    override def equals(one: SparkPlan, other: SparkPlan): Boolean =
      Objects.equals(withEqualityWrapper(one), withEqualityWrapper(other))

    override def newGroupLeaf(groupId: Int): GroupLeafExec.Builder =
      GroupLeafExec.newBuilder(groupId)

    override def isGroupLeaf(node: SparkPlan): Boolean = node match {
      case _: GroupLeafExec => true
      case _ => false
    }

    override def getGroupId(node: SparkPlan): Int = node match {
      case gl: GroupLeafExec => gl.groupId
      case _ => throw new IllegalStateException()
    }

    private def withEqualityWrapper(node: SparkPlan): AnyRef = node match {
      case scan: DataSourceV2ScanExecBase =>
        // Override V2 scan operators' equality implementation to include output attributes.
        //
        // Spark's V2 scans don't incorporate out attributes in equality so E.g.,
        // BatchScan[date#1] can be considered equal to BatchScan[date#2], which is unexpected
        // in RAS planner because it strictly relies on plan equalities for sanity.
        //
        // Related UT: `VeloxOrcDataTypeValidationSuite#Date type`
        // Related Spark PRs:
        // https://github.com/apache/spark/pull/23086
        // https://github.com/apache/spark/pull/23619
        // https://github.com/apache/spark/pull/23430
        ScanV2ExecEqualityWrapper(scan, scan.output)
      case other => other
    }

    private case class ScanV2ExecEqualityWrapper(
        scan: DataSourceV2ScanExecBase,
        output: Seq[Attribute])
  }
}

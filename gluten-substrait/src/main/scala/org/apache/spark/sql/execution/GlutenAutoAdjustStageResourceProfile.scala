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
package org.apache.spark.sql.execution

import org.apache.gluten.GlutenConfig
import org.apache.gluten.execution.{ColumnarToRowExecBase, GlutenPlan, WithResourceProfileSupport}
import org.apache.gluten.logging.LogLevelUtil

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.resource.{ExecutorResourceRequest, ResourceProfile, ResourceProfileManager, TaskResourceRequest}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, OrderPreservingUnaryNode}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.GlutenAutoAdjustStageResourceProfile.{applyNewResourceProfile, collectStagePlan}
import org.apache.spark.sql.execution.adaptive.QueryStageExec
import org.apache.spark.sql.execution.command.ExecutedCommandExec
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ShuffleExchangeExec}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * This rule is used to dynamic adjust stage resource profile for following purposes:
 *   1. swap offheap and onheap memory size when whole stage fallback happened 2. increase executor
 *      heap memory if stage contains gluten operator and spark operator at the same time.
 */
case class GlutenAutoAdjustStageResourceProfile(glutenConf: GlutenConfig, spark: SparkSession)
  extends Rule[SparkPlan]
  with LogLevelUtil {

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!glutenConf.enableAutoAdjustStageResourceProfile) {
      return plan
    }
    val planNodes = collectStagePlan(plan)
    if (planNodes.isEmpty) {
      return plan
    }
    log.info(s"detailPlanNodes ${planNodes.map(_.nodeName).mkString("Array(", ", ", ")")}")

    // one stage is fallback if all node is not GlutenPlan
    // or all GlutenPlan node is C2R node.
    val wholeStageFallback = planNodes
      .filter(_.isInstanceOf[GlutenPlan])
      .count(!_.isInstanceOf[ColumnarToRowExecBase]) == 0

    val rpManager = spark.sparkContext.resourceProfileManager
    val defaultRP = rpManager.defaultResourceProfile

    // initial resource profile config as default resource profile
    val taskResource = mutable.Map.empty[String, TaskResourceRequest] ++= defaultRP.taskResources
    val executorResource =
      mutable.Map.empty[String, ExecutorResourceRequest] ++= defaultRP.executorResources
    val memoryRequest = executorResource.get(ResourceProfile.MEMORY)
    val offheapRequest = executorResource.get(ResourceProfile.OFFHEAP_MEM)
    logInfo(s"default memory request $memoryRequest")
    logInfo(s"default offheap request $offheapRequest")

    // case 1: whole stage fallback to vanilla spark in such case we swap the heap
    // and offheap amount.
    if (wholeStageFallback) {
      if (plan.isInstanceOf[ShuffleExchangeExec] || plan.isInstanceOf[BroadcastExchangeExec]) {
        val newMemoryAmount = offheapRequest.get.amount
        val newOffheapAmount = memoryRequest.get.amount
        val newExecutorMemory =
          new ExecutorResourceRequest(ResourceProfile.MEMORY, newMemoryAmount)
        val newExecutorOffheap =
          new ExecutorResourceRequest(ResourceProfile.OFFHEAP_MEM, newOffheapAmount)
        executorResource.put(ResourceProfile.MEMORY, newExecutorMemory)
        executorResource.put(ResourceProfile.OFFHEAP_MEM, newExecutorOffheap)
        val newRP = new ResourceProfile(executorResource.toMap, taskResource.toMap)
        val maybeEqProfile = rpManager.getEquivalentProfile(newRP)
        val finalRP = if (maybeEqProfile.isDefined) {
          maybeEqProfile.get
        } else {
          // register new resource profile here
          rpManager.addResourceProfile(newRP)
          newRP
        }
        // Wrap with RowBasedNodeResourceProfileWrapperExec so that we can have set ResourceProfile
        val wrapperPlan = RowBasedNodeResourceProfileWrapperExec(plan.children.head, Some(finalRP))
        logInfo(s"Set resource profile $finalRP for plan ${wrapperPlan.nodeName}")
        return plan.withNewChildren(IndexedSeq(wrapperPlan))
      } else {
        logInfo(s"Ignore resource profile for plan ${plan.nodeName}")
        // todo: support set final stage's resource profile
        return plan
      }
    }

    // case 1: check whether fallback exists and decide whether increase heap memory
    val existsC2RorR2C = planNodes
      .exists(
        p => p.isInstanceOf[ColumnarToRowTransition] || p.isInstanceOf[RowToColumnarTransition])

    val c2RorR2CCnt = planNodes.count(
      p => p.isInstanceOf[ColumnarToRowTransition] || p.isInstanceOf[RowToColumnarTransition])

    if (
      existsC2RorR2C && c2RorR2CCnt >= glutenConf.autoAdjustStageC2RorR2CThreshold
      && planNodes.exists(_.isInstanceOf[WithResourceProfileSupport])
    ) {

      val newMemoryAmount = memoryRequest.get.amount * glutenConf.autoAdjustStageRPHeapRatio;
      val newExecutorMemory =
        new ExecutorResourceRequest(ResourceProfile.MEMORY, newMemoryAmount.toLong)
      executorResource.put(ResourceProfile.MEMORY, newExecutorMemory)

      val newResourceProfile = new ResourceProfile(executorResource.toMap, taskResource.toMap)
      val nodeWithResourceProfileSupport = planNodes
        .filter(_.isInstanceOf[WithResourceProfileSupport])
        .head
        .asInstanceOf[WithResourceProfileSupport]
      applyNewResourceProfile(nodeWithResourceProfileSupport, rpManager, newResourceProfile)
    }

    plan
  }
}

object GlutenAutoAdjustStageResourceProfile extends Logging {
  // collect all plan nodes belong to this stage including child query stage
  // but exclude query stage child
  def collectStagePlan(plan: SparkPlan): ArrayBuffer[SparkPlan] = {
    val planNodes = new ArrayBuffer[SparkPlan]()
    collectStagePlan(plan, planNodes)
    planNodes
  }

  private def collectStagePlan(plan: SparkPlan, planNodes: ArrayBuffer[SparkPlan]): Unit = {
    if (plan.isInstanceOf[ExecutedCommandExec]) {
      return
    }
    planNodes += plan
    if (plan.isInstanceOf[QueryStageExec]) {
      return
    }
    plan.children.foreach(collectStagePlan(_, planNodes))
  }

  def applyNewResourceProfile(
      node: WithResourceProfileSupport,
      rpManager: ResourceProfileManager,
      newRP: ResourceProfile): Unit = {
    val maybeEqProfile = rpManager.getEquivalentProfile(newRP)
    val finalRP = if (maybeEqProfile.isDefined) {
      maybeEqProfile.get
    } else {
      // register new resource profile here
      rpManager.addResourceProfile(newRP)
      newRP
    }
    node.withResourceProfile(finalRP)
    logInfo(s"set resource profile $finalRP for plan $node")
  }
}

case class RowBasedNodeResourProfileWrapperExecAdaptor(child: LogicalPlan)
  extends OrderPreservingUnaryNode {
  override def output: Seq[Attribute] = child.output

  // For spark 3.2.
  protected def withNewChildInternal(
      newChild: LogicalPlan): RowBasedNodeResourProfileWrapperExecAdaptor =
    copy(child = newChild)
}

/** Used to wrap a row-based child plan to have a change to setting ResourceProfile. */
case class RowBasedNodeResourceProfileWrapperExec(
    child: SparkPlan,
    resourceProfile: Option[ResourceProfile])
  extends UnaryExecNode {

  if (child.logicalLink.isDefined) {
    setLogicalLink(RowBasedNodeResourProfileWrapperExecAdaptor(child.logicalLink.get))
  }

  override def output: Seq[Attribute] = child.output

  override protected def doExecute(): RDD[InternalRow] = {
    if (resourceProfile.isDefined) {
      log.info(s"Use resource profile ${resourceProfile.get} for RowOperatorWrapperExec.")
      child.execute.withResources(resourceProfile.get)
    } else {
      log.info(s"resourceProfile is empty in RowOperatorWrapperExec!")
      child.execute
    }
  }

  // For spark 3.2.
  protected def withNewChildInternal(newChild: SparkPlan): RowBasedNodeResourceProfileWrapperExec =
    copy(child = newChild)
}

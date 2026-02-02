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

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.{ColumnarToRowExecBase, GlutenPlan}
import org.apache.gluten.logging.LogLevelUtil

import org.apache.spark.annotation.Experimental
import org.apache.spark.resource.{ExecutorResourceRequest, ResourceProfile, TaskResourceRequest}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.exchange.Exchange
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.utils.GlutenResourceProfileUtil

import scala.collection.mutable

/**
 * This rule is used to dynamic adjust stage resource profile for following purposes:
 *   1. Decrease offheap and increase onheap memory size when whole stage fallback happened; 2.
 *      Increase executor heap memory if stage contains gluten operator and spark operator at the
 *      same time. Note: we don't support set resource profile for final stage now. Todo: will
 *      support it.
 */
@Experimental
case class GlutenAutoAdjustStageResourceProfile(glutenConf: GlutenConfig, spark: SparkSession)
  extends Rule[SparkPlan]
  with LogLevelUtil {

  lazy val sparkConf = spark.sparkContext.getConf

  override def apply(plan: SparkPlan): SparkPlan = {
    if (
      !glutenConf.enableAutoAdjustStageResourceProfile ||
      // Auto adjust stage resource for GPU is not supported.
      glutenConf.enableColumnarCudf ||
      !SQLConf.get.adaptiveExecutionEnabled
    ) {
      return plan
    }

    GlutenResourceProfileUtil.restoreDefaultResourceSetting(sparkConf)

    if (!plan.isInstanceOf[Exchange]) {
      // todo: support set resource profile for final stage
      return plan
    }
    val planNodes = GlutenResourceProfileUtil.collectStagePlan(plan)
    if (planNodes.isEmpty) {
      return plan
    }
    log.info(s"detailPlanNodes ${planNodes.map(_.nodeName).mkString("Array(", ", ", ")")}")

    // one stage is considered as fallback if all node is not GlutenPlan
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

    // case 1: whole stage fallback to vanilla spark in such case we increase the heap
    if (wholeStageFallback) {
      val newMemoryAmount = memoryRequest.get.amount * glutenConf.autoAdjustStageRPHeapRatio
      val newExecutorMemory =
        new ExecutorResourceRequest(ResourceProfile.MEMORY, newMemoryAmount.toLong)
      executorResource.put(ResourceProfile.MEMORY, newExecutorMemory)

      val newExecutorOffheap =
        new ExecutorResourceRequest(ResourceProfile.OFFHEAP_MEM, offheapRequest.get.amount / 10)
      executorResource.put(ResourceProfile.OFFHEAP_MEM, newExecutorOffheap)

      val newRP = new ResourceProfile(executorResource.toMap, taskResource.toMap)
      return GlutenResourceProfileUtil
        .applyNewResourceProfileIfPossible(plan, newRP, rpManager, sparkConf)
    }

    // case 2: check whether fallback exists and decide whether increase heap memory
    // and decrease offheap memory.
    val fallenNodeCnt = planNodes.count(p => !p.isInstanceOf[GlutenPlan])
    val totalCount = planNodes.size

    if (1.0 * fallenNodeCnt / totalCount >= glutenConf.autoAdjustStageFallenNodeThreshold) {
      val newMemoryAmount = memoryRequest.get.amount * glutenConf.autoAdjustStageRPHeapRatio;
      val newExecutorMemory =
        new ExecutorResourceRequest(ResourceProfile.MEMORY, newMemoryAmount.toLong)
      executorResource.put(ResourceProfile.MEMORY, newExecutorMemory)

      val newOffHeapMemoryAmount =
        offheapRequest.get.amount * glutenConf.autoAdjustStageRPOffHeapRatio;
      val newExecutorOffheap =
        new ExecutorResourceRequest(ResourceProfile.OFFHEAP_MEM, newOffHeapMemoryAmount.toLong)
      executorResource.put(ResourceProfile.OFFHEAP_MEM, newExecutorOffheap)

      val newRP = new ResourceProfile(executorResource.toMap, taskResource.toMap)
      return GlutenResourceProfileUtil.applyNewResourceProfileIfPossible(
        plan,
        newRP,
        rpManager,
        sparkConf)
    }
    plan
  }
}

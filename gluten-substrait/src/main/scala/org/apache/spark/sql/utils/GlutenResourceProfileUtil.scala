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
package org.apache.spark.sql.utils

import org.apache.gluten.config.GlutenCoreConfig

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.{CPUS_PER_TASK, EXECUTOR_CORES, MEMORY_OFFHEAP_SIZE}
import org.apache.spark.resource.{ResourceProfile, ResourceProfileManager}
import org.apache.spark.sql.execution.{ApplyResourceProfileExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.QueryStageExec
import org.apache.spark.sql.execution.command.{DataWritingCommandExec, ExecutedCommandExec}
import org.apache.spark.sql.execution.exchange.Exchange
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.SparkTestUtil

import scala.collection.mutable.ArrayBuffer

object GlutenResourceProfileUtil extends Logging {
  // collect all plan nodes belong to this stage including child query stage
  // but exclude query stage child
  def collectStagePlan(plan: SparkPlan): ArrayBuffer[SparkPlan] = {

    def collectStagePlan(plan: SparkPlan, planNodes: ArrayBuffer[SparkPlan]): Unit = {
      if (plan.isInstanceOf[DataWritingCommandExec] || plan.isInstanceOf[ExecutedCommandExec]) {
        // todo: support set final stage's resource profile
        return
      }
      planNodes += plan
      if (plan.isInstanceOf[QueryStageExec]) {
        return
      }
      plan.children.foreach(collectStagePlan(_, planNodes))
    }

    val planNodes = new ArrayBuffer[SparkPlan]()
    collectStagePlan(plan, planNodes)
    planNodes
  }

  private def getFinalResourceProfile(
      rpManager: ResourceProfileManager,
      newRP: ResourceProfile): (ResourceProfile, Boolean) = {
    val maybeEqProfile = rpManager.getEquivalentProfile(newRP)
    if (maybeEqProfile.isDefined) {
      (maybeEqProfile.get, true)
    } else {
      try {
        rpManager.isSupported(newRP)
      } catch {
        case e: SparkException =>
          // ResourceProfiles are only supported on YARN and Kubernetes with
          // dynamic allocation enabled
          logWarning(
            s"Resource profile $newRP is not supported, fallback to default profile. Reason: " +
              s"${e.getMessage}")
          return (rpManager.defaultResourceProfile, false)
      }
      // register new resource profile here
      rpManager.addResourceProfile(newRP)
      (newRP, true)
    }
  }

  /**
   * Starting here, the resource profile may differ between stages. Configure resource settings
   * using the default profile to prevent any impact from the previous stage. If a new resource
   * profile is applied, the settings will be updated accordingly.
   */
  def restoreDefaultResourceSetting(sparkConf: SparkConf): Unit = {
    GlutenResourceProfileUtil.updateResourceSetting(
      ResourceProfile.getOrCreateDefaultProfile(sparkConf),
      sparkConf)
  }

  /**
   * Reflects resource changes in some configurations that will be passed to the native side. It
   * only affects the current thread.
   */
  def updateResourceSetting(rp: ResourceProfile, sparkConf: SparkConf): Unit = {
    val coresPerExecutor = rp.getExecutorCores.getOrElse(sparkConf.get(EXECUTOR_CORES))
    val coresPerTask = rp.getTaskCpus.getOrElse(sparkConf.get(CPUS_PER_TASK))
    val taskSlots = coresPerExecutor / coresPerTask
    val conf = SQLConf.get
    conf.setConfString(GlutenCoreConfig.NUM_TASK_SLOTS_PER_EXECUTOR.key, taskSlots.toString)
    val offHeapSize = rp.executorResources
      .get(ResourceProfile.OFFHEAP_MEM)
      .map(_.amount)
      .getOrElse(sparkConf.get(MEMORY_OFFHEAP_SIZE))
    conf.setConfString(GlutenCoreConfig.COLUMNAR_OFFHEAP_SIZE_IN_BYTES.key, offHeapSize.toString)
    conf.setConfString(
      GlutenCoreConfig.COLUMNAR_TASK_OFFHEAP_SIZE_IN_BYTES.key,
      (offHeapSize / taskSlots).toString)
  }

  // Returns the plan and whether the new resource profile is applied.
  def applyNewResourceProfileIfPossible(
      plan: SparkPlan,
      rp: ResourceProfile,
      rpManager: ResourceProfileManager,
      sparkConf: SparkConf): SparkPlan = {
    updateResourceSetting(rp, sparkConf)

    val (finalRP, profileApplied) = getFinalResourceProfile(rpManager, rp)
    if (profileApplied || SparkTestUtil.isTesting) {
      // Wrap the plan with ApplyResourceProfileExec so that we can apply new ResourceProfile.
      logInfo(s"Apply resource profile $finalRP for plan ${plan.nodeName}")
      plan match {
        case exchange: Exchange =>
          exchange.withNewChildren(Seq(ApplyResourceProfileExec(exchange.child, finalRP)))
        case other =>
          ApplyResourceProfileExec(other, finalRP)
      }
    } else {
      plan
    }
  }
}

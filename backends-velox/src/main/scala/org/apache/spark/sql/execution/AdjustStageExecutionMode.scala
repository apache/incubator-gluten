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
import org.apache.gluten.exception.GlutenException
import org.apache.gluten.execution.{CudfTag, TransformSupport, VeloxResizeBatchesExec, WholeStageTransformer}
import org.apache.gluten.logging.LogLevelUtil

import org.apache.spark.SparkConf
import org.apache.spark.annotation.Experimental
import org.apache.spark.internal.Logging
import org.apache.spark.resource.{ExecutorResourceRequest, ResourceProfile, TaskResourceRequest}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.AdjustStageExecutionMode.{adjustExecutionMode, unsetTag}
import org.apache.spark.sql.execution.adaptive.{AQEShuffleReadExec, ColumnarAQEShuffleReadExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.joins.BaseJoinExec
import org.apache.spark.sql.utils.GlutenResourceProfileUtil
import org.apache.spark.util.SparkTestUtil

import scala.collection.mutable

// For ShuffleStage, the resource profile is set to ColumnarShuffleExchangeExec.inputColumnarRDD.
@Experimental
case class AdjustStageExecutionMode(
    glutenConf: GlutenConfig,
    spark: SparkSession,
    isAdaptiveContext: Boolean)
  extends Rule[SparkPlan]
  with LogLevelUtil {

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!isAdaptiveContext) {
      return plan
    }

    val sparkConf = spark.sparkContext.getConf

    GlutenResourceProfileUtil.restoreDefaultResourceSetting(sparkConf)

    if (glutenConf.enableColumnarCudf) {
      return adjustExecutionModeForGPU(plan, sparkConf)
    }

    plan
  }

  private def adjustExecutionModeForGPU(plan: SparkPlan, sparkConf: SparkConf): SparkPlan = {
    val planNodes = GlutenResourceProfileUtil.collectStagePlan(plan)
    if (planNodes.isEmpty) {
      return plan
    }
    log.info(s"detailPlanNodes ${planNodes.map(_.nodeName).mkString("Array(", ", ", ")")}")

    val transformers = plan.collect { case t: WholeStageTransformer => t }
    if (transformers.isEmpty) {
      return plan
    }
    if (transformers.size > 1) {
      logWarning(s"Not offloading GPU because multiple WholeStageTransformer exist. Remove tags.")
      unsetTag(plan, CudfTag.CudfTag)
      return plan
    }

    val transformer = transformers.head
    if (transformer.isCudf) {
      if (glutenConf.autoAdjustStageExecutionMode) {
        // TODO: change to flexible config.
        val offloadGpu = planNodes.exists(_.isInstanceOf[BaseJoinExec])
        if (!offloadGpu) {
          logWarning(s"Not offloading GPU because missing offload condition. Remove tag.")
          unsetTag(plan, CudfTag.CudfTag)
          return plan
        }
      }

      val gpuStageMode = if (SparkTestUtil.isTesting) {
        // Only unset for transformer.
        transformer.unsetTagValue(CudfTag.CudfTag)
        MockGPUStageMode
      } else {
        GPUStageMode
      }

      logWarning(s"Adjust resource profile to use GPU.")
      val rpManager = spark.sparkContext.resourceProfileManager
      val defaultRP = spark.sparkContext.resourceProfileManager.defaultResourceProfile

      // initial resource profile config as default resource profile
      val taskResource = mutable.Map.empty[String, TaskResourceRequest] ++= defaultRP.taskResources
      val executorResource =
        mutable.Map.empty[String, ExecutorResourceRequest] ++= defaultRP.executorResources

      // The gpu task resource limits how many tasks can be launched in one executor.
      // TODO: Make gpu task resource configurable.
      taskResource.put("gpu", new TaskResourceRequest("gpu", 0.1))
      executorResource.put("gpu", new ExecutorResourceRequest("gpu", 1))
      executorResource.remove("cpu")
      val newRP = new ResourceProfile(executorResource.toMap, taskResource.toMap)
      val newPlan = GlutenResourceProfileUtil.applyNewResourceProfileIfPossible(
        adjustExecutionMode(plan, gpuStageMode),
        newRP,
        rpManager,
        sparkConf)
      if (
        !newPlan.isInstanceOf[ApplyResourceProfileExec] && !newPlan.children.exists(
          _.isInstanceOf[ApplyResourceProfileExec])
      ) {
        throw new GlutenException(s"Failed to apply new resource profile $newRP")
      }
      newPlan
    } else {
      plan
    }
  }
}

object AdjustStageExecutionMode extends Logging {
  def adjustExecutionMode(plan: SparkPlan, stageExecutionMode: StageExecutionMode): SparkPlan = {
    plan match {
      case aqeReader: AQEShuffleReadExec =>
        logWarning(s"Adjust AQE shuffle read to ${stageExecutionMode.name}.")
        ColumnarAQEShuffleReadExec(
          aqeReader.child,
          aqeReader.partitionSpecs,
          stageExecutionMode,
          isWrapper = false)
      case queryStageExec: ShuffleQueryStageExec =>
        // If no AQEShuffleReadExec is created for the shuffle query stage,
        // create ColumnarAQEShuffleReadExec here with default partition specs and set the stage
        // execution mode.
        val partitionSpecs =
          Array.tabulate(queryStageExec.shuffle.numPartitions)(
            i => CoalescedPartitionSpec(i, i + 1))
        ColumnarAQEShuffleReadExec(
          queryStageExec,
          partitionSpecs,
          stageExecutionMode,
          isWrapper = true)
      case shuffle: ColumnarShuffleExchangeExec =>
        logInfo(s"Adjust shuffle exchange to ${stageExecutionMode.name}.")
        shuffle
          .copy(mapperStageMode = Some(stageExecutionMode))
          .withNewChildren(Seq(adjustExecutionMode(shuffle.child, stageExecutionMode)))
      case resizeBatches: VeloxResizeBatchesExec =>
        logInfo(s"Adjust VeloxResizeBatchesExec to ${stageExecutionMode.name}.")
        VeloxResizeBatchesExec(
          adjustExecutionMode(resizeBatches.child, stageExecutionMode),
          Some(stageExecutionMode))
      case _ =>
        plan.withNewChildren(plan.children.map(adjustExecutionMode(_, stageExecutionMode)))
    }
  }

  def unsetTag[T](plan: SparkPlan, tag: TreeNodeTag[T]): Unit = {
    plan.foreach {
      case t: TransformSupport =>
        t.unsetTagValue(tag)
      case _ =>
    }
  }
}

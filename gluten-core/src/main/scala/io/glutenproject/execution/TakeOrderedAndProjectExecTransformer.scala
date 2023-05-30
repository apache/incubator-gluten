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

import io.glutenproject.extension.GlutenPlan
import io.glutenproject.utils.ColumnarShuffleUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, SinglePartition}
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.{ColumnarCollapseTransformStages, ColumnarInputAdapter, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.vectorized.ColumnarBatch

import java.util.concurrent.atomic.AtomicInteger

case class TakeOrderedAndProjectExecTransformer(limit: Int,
                                                sortOrder: Seq[SortOrder],
                                                projectList: Seq[NamedExpression],
                                                child: SparkPlan,
                                                isAdaptiveContextOrTopParentExchange: Boolean)
    extends UnaryExecNode with GlutenPlan {
  override def outputPartitioning: Partitioning = SinglePartition
  override def outputOrdering: Seq[SortOrder] = sortOrder
  override def supportsColumnar: Boolean = true

  override def output: Seq[Attribute] = {
    projectList.map(_.toAttribute)
  }

  override def simpleString(maxFields: Int): String = {
    val orderByString = truncatedString(sortOrder, "[", ",", "]", maxFields)
    val outputString = truncatedString(output, "[", ",", "]", maxFields)

    s"TakeOrderedAndProjectExecTransform(limit=$limit, " +
      s"orderBy=$orderByString, output=$outputString)"
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan = {
    copy(child = newChild)
  }

  protected override def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(s"This operator doesn't support doExecute().")
  }


  protected override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    // No real computation here
    val childRDD = child.executeColumnar()
    val childRDDPartsNum = childRDD.getNumPartitions

    if (childRDDPartsNum == 0) {
      sparkContext.parallelize(Seq.empty, 1)
    } else {
      // The child should have been replaced by ColumnarCollapseTransformStages.
      val limitExecPlan = child match {
        case wholeStage: WholeStageTransformerExec =>
          // remove this WholeStageTransformerExec, put the new sort, limit and project
          // into a new whole stage.
          val wholeStageChild = wholeStage.child
          val sortExecPlan = SortExecTransformer(sortOrder, false, wholeStageChild)
          LimitTransformer(sortExecPlan, 0, limit)
        case other =>
          val sortExecPlan = SortExecTransformer(sortOrder, false, other)
          LimitTransformer(sortExecPlan, 0, limit)
      }
      val transformStageCounter: AtomicInteger =
        ColumnarCollapseTransformStages.transformStageCounter
      val finalLimitPlan = if (childRDDPartsNum == 1) {
          limitExecPlan
      } else {
        val sortStagePlan = WholeStageTransformerExec(limitExecPlan)(
          transformStageCounter.incrementAndGet())
        val shuffleExec = ShuffleExchangeExec(SinglePartition, sortStagePlan)
        val transformedShuffleExec = ColumnarShuffleUtil.genColumnarShuffleExchange(
          shuffleExec, sortStagePlan, isAdaptiveContextOrTopParentExchange,
          shuffleExec.child.output, enableCoalesceBatches = false)
        val globalSortExecPlan = SortExecTransformer(sortOrder, false,
          new ColumnarInputAdapter(transformedShuffleExec))
        LimitTransformer(globalSortExecPlan, 0, limit)
      }

      val projectPlan = if (projectList != child.output) {
        ProjectExecTransformer(projectList, finalLimitPlan)
      } else {
        finalLimitPlan
      }

      val finalPlan =
        WholeStageTransformerExec(projectPlan)(transformStageCounter.incrementAndGet())

      finalPlan.doExecuteColumnar()
    }
  }

}

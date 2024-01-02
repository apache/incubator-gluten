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

import io.glutenproject.extension.{ColumnarOverrides, GlutenPlan}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, SinglePartition}
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.execution.{ColumnarCollapseTransformStages, ColumnarShuffleExchangeExec, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.vectorized.ColumnarBatch

import java.util.concurrent.atomic.AtomicInteger

case class TakeOrderedAndProjectExecTransformer(
    limit: Int,
    sortOrder: Seq[SortOrder],
    projectList: Seq[NamedExpression],
    child: SparkPlan)
  extends UnaryExecNode
  with GlutenPlan {
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

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(s"This operator doesn't support doExecute().")
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    // No real computation here
    val childRDD = child.executeColumnar()
    val childRDDPartsNum = childRDD.getNumPartitions

    if (childRDDPartsNum == 0) {
      sparkContext.parallelize(Seq.empty, 1)
    } else {
      val orderingSatisfies = SortOrder.orderingSatisfies(child.outputOrdering, sortOrder)
      def withLocalSort(sparkPlan: SparkPlan): SparkPlan = {
        if (orderingSatisfies) {
          sparkPlan
        } else {
          SortExecTransformer(sortOrder, false, sparkPlan)
        }
      }

      // The child should have been replaced by ColumnarCollapseTransformStages.
      val limitBeforeShuffle = child match {
        case wholeStage: WholeStageTransformer =>
          // remove this WholeStageTransformer, put the new sort, limit and project
          // into a new whole stage.
          val localSortPlan = withLocalSort(wholeStage.child)
          LimitTransformer(localSortPlan, 0, limit)
        case other =>
          // if the child it is not WholeStageTransformer, add the adapter first
          // so that, later we can wrap WholeStageTransformer.
          val localSortPlan = withLocalSort(
            ColumnarCollapseTransformStages.wrapInputIteratorTransformer(other))
          LimitTransformer(localSortPlan, 0, limit)
      }
      val transformStageCounter: AtomicInteger =
        ColumnarCollapseTransformStages.transformStageCounter
      val finalLimitPlan = if (childRDDPartsNum == 1) {
        limitBeforeShuffle
      } else {
        val limitStagePlan =
          WholeStageTransformer(limitBeforeShuffle)(transformStageCounter.incrementAndGet())
        val shuffleExec = ShuffleExchangeExec(SinglePartition, limitStagePlan)
        val transformedShuffleExec =
          ColumnarShuffleExchangeExec(shuffleExec, limitStagePlan, shuffleExec.child.output)
        val localSortPlan =
          SortExecTransformer(
            sortOrder,
            false,
            ColumnarCollapseTransformStages.wrapInputIteratorTransformer(transformedShuffleExec))
        LimitTransformer(localSortPlan, 0, limit)
      }

      val projectPlan = if (projectList != child.output) {
        ProjectExecTransformer(projectList, finalLimitPlan)
      } else {
        finalLimitPlan
      }

      val finalPlan = ColumnarOverrides.applyExtendedColumnarPreRules(
        WholeStageTransformer(projectPlan)(transformStageCounter.incrementAndGet()))

      finalPlan.executeColumnar()
    }
  }
}

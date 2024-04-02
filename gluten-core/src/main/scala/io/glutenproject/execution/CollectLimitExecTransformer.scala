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

import io.glutenproject.extension.{GlutenPlan, ValidationResult}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, SinglePartition}
import org.apache.spark.sql.execution.{ColumnarCollapseTransformStages, ColumnarShuffleExchangeExec, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.vectorized.ColumnarBatch

import java.util.concurrent.atomic.AtomicInteger

case class CollectLimitExecTransformer(limit: Int, child: SparkPlan, offset: Long)
  extends UnaryExecNode
  with GlutenPlan {

  override def supportsColumnar: Boolean = true

  override def output: Seq[Attribute] = child.output

  override def simpleString(maxFields: Int): String = {
    s"CollectLimitExecTransformer (limit=$limit)"
  }

  override def outputPartitioning: Partitioning = SinglePartition

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(s"This operator doesn't support doExecute().")
  }

  override protected def doValidateInternal(): ValidationResult = {
    LimitTransformer(child, offset, limit).doValidate()
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val childRDD = child.executeColumnar()
    val childRDDPartsNum = childRDD.getNumPartitions

    if (childRDDPartsNum == 0) {
      sparkContext.parallelize(Seq.empty, 1)
    } else {
      val transformStageCounter: AtomicInteger =
        ColumnarCollapseTransformStages.transformStageCounter

      val childTransformer = child match {
        case wholeStage: WholeStageTransformer =>
          LimitTransformer(wholeStage.child, 0, limit)
        case other =>
          LimitTransformer(
            ColumnarCollapseTransformStages.wrapInputIteratorTransformer(other),
            0,
            limit)
      }

      val finalLimitPlan = if (childRDD.getNumPartitions == 1) {
        childTransformer
      } else {
        val limitStagePlan =
          WholeStageTransformer(childTransformer)(transformStageCounter.incrementAndGet())
        val shuffleExec = ShuffleExchangeExec(SinglePartition, limitStagePlan)
        val transformedShuffleExec =
          ColumnarShuffleExchangeExec(shuffleExec, limitStagePlan, shuffleExec.child.output)
        LimitTransformer(
          ColumnarCollapseTransformStages.wrapInputIteratorTransformer(transformedShuffleExec),
          offset,
          limit)
      }

      val finalPlan =
        WholeStageTransformer(finalLimitPlan)(transformStageCounter.incrementAndGet())
      finalPlan.executeColumnar()

    }
  }
}

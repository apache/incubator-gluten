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

import org.apache.spark.rdd.{RDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{ColumnarShuffleExchangeExec, SparkPlan}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.sql.catalyst.plans.physical.SinglePartition

case class CollectLimitTransformer(child: SparkPlan, limit: Int)
  extends SparkPlan
{
  override def supportsColumnar: Boolean = true

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(s"This operator doesn't support doExecute().")
  }

  override def output: Seq[Attribute] = child.output

  override def children: Seq[SparkPlan] = Seq(child)

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[SparkPlan])
  : CollectLimitTransformer =
  {
    copy(child = newChildren(0), limit = limit)
  }

  protected override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val childRDD = child.executeColumnar()
    if (childRDD.getNumPartitions == 0) {
      // ParallelCollectionRDD is a private class, cannot be accessed here
      throw new UnsupportedOperationException(s"child RDD's partition is 0")
    } else if (childRDD.getNumPartitions == 1) {
      childRDD
    } else {
      val shuffle = ColumnarShuffleExchangeExec(SinglePartition, child)
      val shuffleRDD = shuffle.doExecuteColumnar()
      shuffleRDD.mapPartitions(_.take(limit))
    }
  }
}

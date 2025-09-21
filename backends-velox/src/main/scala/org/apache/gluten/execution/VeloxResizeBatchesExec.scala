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
package org.apache.gluten.execution

import org.apache.gluten.backendsapi.velox.VeloxBatchType
import org.apache.gluten.iterator.ClosableIterator
import org.apache.gluten.utils.VeloxBatchResizer

import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.vectorized.ColumnarBatch

import scala.collection.JavaConverters._

/**
 * An operator to resize input batches by appending the later batches to the one that comes earlier,
 * or splitting one batch to smaller ones.
 */
case class VeloxResizeBatchesExec(
    override val child: SparkPlan,
    minOutputBatchSize: Int,
    maxOutputBatchSize: Int)
  extends ColumnarToColumnarExec(VeloxBatchType, VeloxBatchType) {

  override protected def mapIterator(in: Iterator[ColumnarBatch]): Iterator[ColumnarBatch] = {
    VeloxBatchResizer.create(minOutputBatchSize, maxOutputBatchSize, in.asJava).asScala
  }

  override protected def closeIterator(out: Iterator[ColumnarBatch]): Unit = {
    out.asJava match {
      case c: ClosableIterator[ColumnarBatch] => c.close()
      case _ =>
    }
  }

  override protected def needRecyclePayload: Boolean = true

  override def outputPartitioning: Partitioning = child.outputPartitioning
  override def outputOrdering: Seq[SortOrder] = child.outputOrdering
  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
}

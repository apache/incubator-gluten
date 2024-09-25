package org.apache.gluten.execution

import org.apache.gluten.columnarbatch.ArrowBatches.{ArrowJavaBatch, ArrowNativeBatch}
import org.apache.gluten.columnarbatch.ColumnarBatches
import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.vectorized.ColumnarBatch

/** Converts input data with batch type [[ArrowNativeBatch]] to type [[ArrowJavaBatch]]. */
case class LoadArrowDataExec(override val child: SparkPlan)
  extends ColumnarToColumnarExec(ArrowNativeBatch, ArrowJavaBatch) {
  override protected def mapIterator(in: Iterator[ColumnarBatch]): Iterator[ColumnarBatch] = {
    in.map(b => ColumnarBatches.load(ArrowBufferAllocators.contextInstance, b))
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
}

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

import io.glutenproject.backendsapi.velox.ValidatorApiImpl
import io.glutenproject.columnarbatch.ColumnarBatches
import io.glutenproject.exec.Runtimes
import io.glutenproject.extension.GlutenPlan
import io.glutenproject.memory.arrowalloc.ArrowBufferAllocators
import io.glutenproject.memory.nmm.NativeMemoryManagers
import io.glutenproject.utils.{ArrowAbiUtil, Iterators}
import io.glutenproject.vectorized.VanillaColumnarToNativeColumnarJniWrapper
import org.apache.arrow.c.{ArrowArray, ArrowSchema, Data}
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.arrow.ArrowColumnarBatchConverter
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.utils.SparkArrowUtil
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.TaskResources

case class VanillaColumnarToVeloxColumnarExec(child: SparkPlan) extends GlutenPlan with UnaryExecNode {

  override def supportsColumnar: Boolean = true

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(s"This operator doesn't support doExecute().")
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    new ValidatorApiImpl().doSchemaValidate(schema).foreach {
      reason =>
        throw new UnsupportedOperationException(
          s"Input schema contains unsupported type when convert columnar to columnar for $schema " +
            s"due to $reason")
    }

    val numInputBatches = longMetric("numInputBatches")
    val numOutputBatches = longMetric("numOutputBatches")
    val convertTime = longMetric("convertTime")
    // Instead of creating a new config we are reusing columnBatchSize. In the future if we do
    // combine with some of the Arrow conversion tools we will need to unify some of the configs.
    val numRows = conf.columnBatchSize
    // This avoids calling `schema` in the RDD closure, so that we don't need to include the entire
    // plan (this) in the closure.
    val localSchema = schema
    child.execute().mapPartitions {
      rowIterator =>
        VanillaColumnarToVeloxColumnarExec.toColumnarBatchIterator(
          rowIterator.asInstanceOf[Iterator[ColumnarBatch]],
          localSchema,
          numInputBatches,
          numOutputBatches,
          convertTime,
          TaskContext.get())
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan = {
    copy(child = newChild)
  }

  override def output: Seq[Attribute] = child.output
}

object VanillaColumnarToVeloxColumnarExec {

  def toColumnarBatchIterator(it: Iterator[ColumnarBatch],
                              schema: StructType,
                              numInputBatches: SQLMetric,
                              numOutputBatches: SQLMetric,
                              convertTime: SQLMetric,
                              taskContext: TaskContext): Iterator[ColumnarBatch] = {
    if (it.isEmpty) {
      return Iterator.empty
    }

    val arrowSchema =
      SparkArrowUtil.toArrowSchema(schema, SQLConf.get.sessionLocalTimeZone)
    val jniWrapper = VanillaColumnarToNativeColumnarJniWrapper.create()
    val allocator = ArrowBufferAllocators.contextInstance()
    val cSchema = ArrowSchema.allocateNew(allocator)
    val c2cHandle =
      try {
        ArrowAbiUtil.exportSchema(allocator, arrowSchema, cSchema)
        jniWrapper.init(
          cSchema.memoryAddress(),
          NativeMemoryManagers
            .contextInstance("ColumnarToColumnar")
            .getNativeInstanceHandle)
      } finally {
        cSchema.close()
      }

    val converter = ArrowColumnarBatchConverter.create(arrowSchema, allocator)

    val res: Iterator[ColumnarBatch] = new Iterator[ColumnarBatch] {

      override def hasNext: Boolean = {
        it.hasNext
      }

      def nativeConvert(cb: ColumnarBatch): ColumnarBatch = {
        var arrowArray: ArrowArray = null
        TaskResources.addRecycler("ColumnarToColumnar_arrowArray", 100) {
          // Remind, remove isOpen here
          if (arrowArray != null) {
            arrowArray.close()
          }
        }

        numInputBatches += 1
        try {
          arrowArray = ArrowArray.allocateNew(allocator)
          converter.write(cb)
          converter.finish()
          Data.exportVectorSchemaRoot(allocator, converter.root, null, arrowArray)
          val handle = jniWrapper
            .nativeConvertVanillaColumnarToColumnar(c2cHandle, arrowArray.memoryAddress())
          ColumnarBatches.create(Runtimes.contextInstance(), handle)
        } finally {
          converter.reset()
          arrowArray.close()
          arrowArray = null
        }
      }

      override def next(): ColumnarBatch = {
        val currentBatch = it.next()
        val start = System.currentTimeMillis()
        val cb = nativeConvert(currentBatch)
        numOutputBatches += 1
        convertTime += System.currentTimeMillis() - start
        cb
      }
    }

    if (taskContext != null) {
      taskContext.addTaskCompletionListener[Unit] { _ =>
        jniWrapper.close(c2cHandle)
        allocator.close()
        converter.close()
      }
    }

    Iterators
      .wrap(res)
      .recycleIterator {
        jniWrapper.close(c2cHandle)
        allocator.close()
        converter.close()
      }
      .recyclePayload(_.close())
      .create()
  }
}

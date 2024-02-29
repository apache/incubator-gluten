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
import io.glutenproject.memory.arrowalloc.ArrowBufferAllocators
import io.glutenproject.memory.nmm.NativeMemoryManagers
import io.glutenproject.utils.{ArrowAbiUtil, Iterators}
import io.glutenproject.vectorized.VanillaColumnarToNativeColumnarJniWrapper

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.arrow.ArrowColumnarBatchConverter
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.utils.SparkArrowUtil
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.TaskResources

import org.apache.arrow.c.{ArrowArray, ArrowSchema, Data}

case class ColumnarToVeloxColumnarExec(child: SparkPlan) extends ColumnarToColumnarExecBase(child) {

  override def doExecuteColumnarInternal(): RDD[ColumnarBatch] = {
    new ValidatorApiImpl().doSchemaValidate(schema).foreach {
      reason =>
        throw new UnsupportedOperationException(
          s"Input schema contains unsupported type when performing columnar" +
            s" to columnar for $schema " + s"due to $reason")
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
    child.executeColumnar().mapPartitions {
      rowIterator =>
        ColumnarToVeloxColumnarExec.toColumnarBatchIterator(
          rowIterator,
          localSchema,
          numInputBatches,
          numOutputBatches,
          convertTime)
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan = {
    copy(child = newChild)
  }
}

object ColumnarToVeloxColumnarExec {

  def toColumnarBatchIterator(
      it: Iterator[ColumnarBatch],
      schema: StructType,
      numInputBatches: SQLMetric,
      numOutputBatches: SQLMetric,
      convertTime: SQLMetric): Iterator[ColumnarBatch] = {
    if (it.isEmpty) {
      return Iterator.empty
    }

    val arrowSchema =
      SparkArrowUtil.toArrowSchema(schema, SQLConf.get.sessionLocalTimeZone)
    val jniWrapper = VanillaColumnarToNativeColumnarJniWrapper.create()
    val allocator = ArrowBufferAllocators.contextInstance()
    val cSchema = ArrowSchema.allocateNew(allocator)
    ArrowAbiUtil.exportSchema(allocator, arrowSchema, cSchema)
    val c2cHandle = jniWrapper.init(
      cSchema.memoryAddress(),
      NativeMemoryManagers
        .contextInstance("ColumnarToColumnar")
        .getNativeInstanceHandle)

    val converter = ArrowColumnarBatchConverter.create(arrowSchema, allocator)

    TaskResources.addRecycler("ColumnarToColumnar_resourceClean", 100) {
      jniWrapper.close(c2cHandle)
      converter.close()
      cSchema.close()
    }

    val res: Iterator[ColumnarBatch] = new Iterator[ColumnarBatch] {

      var arrowArray: ArrowArray = null
      TaskResources.addRecycler("ColumnarToColumnar_arrowArray", 100) {
        if (arrowArray != null) {
          arrowArray.release()
          arrowArray.close()
          converter.reset()
        }
      }

      override def hasNext: Boolean = {
        if (arrowArray != null) {
          arrowArray.release()
          arrowArray.close()
          converter.reset()
          arrowArray = null
        }
        it.hasNext
      }

      def nativeConvert(cb: ColumnarBatch): ColumnarBatch = {
        numInputBatches += 1
        arrowArray = ArrowArray.allocateNew(allocator)
        converter.write(cb)
        converter.finish()
        Data.exportVectorSchemaRoot(allocator, converter.root, null, arrowArray)
        val handle = jniWrapper
          .nativeConvertVanillaColumnarToColumnar(c2cHandle, arrowArray.memoryAddress())
        ColumnarBatches.create(Runtimes.contextInstance(), handle)
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

    Iterators
      .wrap(res)
      .recycleIterator {
        jniWrapper.close(c2cHandle)
        converter.close()
        cSchema.close()
      }
      .recyclePayload(_.close())
      .create()
  }
}

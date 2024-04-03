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

import org.apache.gluten.columnarbatch.{ColumnarBatches, ColumnarBatchJniWrapper}
import org.apache.gluten.execution.{BaseColumnarCollectLimitExec, LimitTransformer, WholeStageTransformer}
import org.apache.gluten.extension.ValidationResult
import org.apache.gluten.memory.arrowalloc.ArrowBufferAllocators
import org.apache.gluten.memory.nmm.NativeMemoryManagers
import org.apache.gluten.utils.ArrowAbiUtil
import org.apache.gluten.vectorized.{ColumnarBatchSerializerJniWrapper, NativeColumnarToRowJniWrapper}

import org.apache.spark.rdd.{ParallelCollectionRDD, RDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.plans.physical.SinglePartition
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.utils.SparkArrowUtil
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.TaskResources
import org.apache.spark.util.io.{ChunkedByteBuffer, ChunkedByteBufferOutputStream}

import org.apache.arrow.c.ArrowSchema

import java.io.{DataInputStream, DataOutputStream}
import java.nio.ByteBuffer

import scala.collection.mutable.ArrayBuffer

/**
 * Support columnar collect limit and to be compatible with [[CollectLimitExec]].
 *
 *   - For `executeCollect`, we serialize ColumnarBatch to byte array first, then convert to unsafe
 *     row at driver side
 *   - For `execute`, we delegate to `ColumnarShuffleExchangeExec` and `LimitTransformer`
 */
case class ColumnarCollectLimitExec(child: SparkPlan, offset: Int, limit: Int)
  extends BaseColumnarCollectLimitExec {
  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)

  override protected def doValidateInternal(): ValidationResult = {
    LimitTransformer(child, offset, limit).doValidate()
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val childRDD = child.executeColumnar()
    val childRDDPartsNum = childRDD.getNumPartitions
    if (childRDD.getNumPartitions == 0) {
      new ParallelCollectionRDD(sparkContext, Seq.empty[ColumnarBatch], 1, Map.empty)
    } else {
      val requiresShuffle = childRDDPartsNum > 1
      val limitBeforeShuffleOffset = if (requiresShuffle) {
        // Local limit does not need offset
        0
      } else {
        offset
      }
      // The child should have been replaced by ColumnarCollapseTransformStages.
      val limitBeforeShuffle = child match {
        case wholeStage: WholeStageTransformer =>
          LimitTransformer(wholeStage.child, limitBeforeShuffleOffset, limit)
        case other =>
          // if the child it is not WholeStageTransformer, add the adapter first
          // so that, later we can wrap WholeStageTransformer.
          LimitTransformer(
            ColumnarCollapseTransformStages.wrapInputIteratorTransformer(other),
            limitBeforeShuffleOffset,
            limit)
      }

      val finalLimitPlan = if (requiresShuffle) {
        val limitStagePlan =
          WholeStageTransformer(limitBeforeShuffle)(
            ColumnarCollapseTransformStages.transformStageCounter.incrementAndGet())
        val shuffleExec = ShuffleExchangeExec(SinglePartition, limitStagePlan)
        val transformedShuffleExec =
          ColumnarShuffleExchangeExec(shuffleExec, limitStagePlan, shuffleExec.child.output)
        LimitTransformer(
          ColumnarCollapseTransformStages.wrapInputIteratorTransformer(transformedShuffleExec),
          offset,
          limit)
      } else {
        limitBeforeShuffle
      }
      val finalPlan =
        WholeStageTransformer(finalLimitPlan)(
          ColumnarCollapseTransformStages.transformStageCounter.incrementAndGet())
      finalPlan.executeColumnar()
    }
  }

  override def executeCollect(): Array[InternalRow] = {
    if (limit == 0) {
      return new Array[InternalRow](0)
    }

    val rows = if (limit > 0) {
      if (offset > 0) {
        incrementCollectAsUnsafeRow(limit).drop(offset)
      } else {
        incrementCollectAsUnsafeRow(limit)
      }
    } else {
      collectAsUnsafeRow().drop(offset)
    }
    rows
  }

  class BytesToUnsafeRow {
    private val serdeJni = ColumnarBatchSerializerJniWrapper.create()
    private val nmm = NativeMemoryManagers
      .contextInstance("Collect limit deserialize")
    private val timezoneId = conf.sessionLocalTimeZone
    private val localSchema = SparkArrowUtil.toArrowSchema(schema, timezoneId)
    private val arrowAlloc = ArrowBufferAllocators.contextInstance()
    private val cSchema = ArrowSchema.allocateNew(arrowAlloc)
    ArrowAbiUtil.exportSchema(arrowAlloc, localSchema, cSchema)
    private val serdeHandle = serdeJni
      .init(cSchema.memoryAddress(), nmm.getNativeInstanceHandle)
    cSchema.close()
    private val c2rJni = NativeColumnarToRowJniWrapper.create()
    private val c2rHandle = c2rJni.nativeColumnarToRowInit(nmm.getNativeInstanceHandle)
    private val cbJni = ColumnarBatchJniWrapper.create()

    private def getRequiredRows(localLimit: Int, existed: Int, numRows: Int): Int = {
      if (localLimit < 0) {
        numRows
      } else {
        numRows.min(localLimit - existed)
      }
    }

    def appendToUnsafeRowBuffer(
        buf: ArrayBuffer[InternalRow],
        localLimit: Int,
        chunkedByteBuffer: ChunkedByteBuffer): Unit = {
      val cbbis = chunkedByteBuffer.toInputStream()
      val ins = new DataInputStream(cbbis)
      try {
        var sizeOfNextBatch = ins.readInt()
        while (sizeOfNextBatch != -1 && (localLimit == -1 || buf.length < localLimit)) {
          val bytes = new Array[Byte](sizeOfNextBatch)
          ins.readFully(bytes)
          val batchHandle = serdeJni.deserialize(serdeHandle, bytes)
          try {
            val info = c2rJni.nativeColumnarToRowConvert(batchHandle, c2rHandle)
            val numRows = cbJni.numRows(batchHandle).toInt
            val requiredRows = getRequiredRows(localLimit, buf.length, numRows)
            var rowId = 0
            while (rowId < requiredRows) {
              val row = new UnsafeRow(output.size)
              val (offset, length) = (info.offsets(rowId), info.lengths(rowId))
              row.pointTo(null, info.memoryAddress + offset, length)
              buf.append(row)
              rowId += 1
            }
          } finally {
            cbJni.close(batchHandle)
          }
          sizeOfNextBatch = ins.readInt()
        }
      } finally {
        ins.close()
      }
    }

    def close(): Unit = {
      c2rJni.nativeClose(c2rHandle)
      serdeJni.close(serdeHandle)
    }
  }

  private def collectAsUnsafeRow(): Array[InternalRow] = {
    val byteArrayRDD = getByteArrayRdd(-1)
    TaskResources.runUnsafe {
      val bytesToUnsafeRow = new BytesToUnsafeRow()
      try {
        val results = ArrayBuffer[InternalRow]()
        byteArrayRDD.collect().foreach {
          bytes => bytesToUnsafeRow.appendToUnsafeRowBuffer(results, -1, bytes)
        }
        results.toArray
      } finally {
        bytesToUnsafeRow.close()
      }
    }
  }

  private def incrementCollectAsUnsafeRow(localLimit: Int): Array[InternalRow] = {
    val byteArrayRDD = getByteArrayRdd(limit)
    TaskResources.runUnsafe {
      val nativeCollect = new BytesToUnsafeRow()
      try {
        nativeExecuteTake(
          byteArrayRDD,
          localLimit,
          nativeCollect
        )
      } finally {
        nativeCollect.close()
      }
    }
  }

  private def getByteArrayRdd(localLimit: Int): RDD[ChunkedByteBuffer] = {
    child.executeColumnar().mapPartitionsInternal {
      iter =>
        val nativeMemoryManagerHandle = NativeMemoryManagers
          .contextInstance("Collect limit serialize")
          .getNativeInstanceHandle
        val serdeJni = ColumnarBatchSerializerJniWrapper.create()
        val cbbos = new ChunkedByteBufferOutputStream(1024 * 1024, ByteBuffer.allocate)
        val out = new DataOutputStream(cbbos)
        var counter = 0L
        try {
          while (iter.hasNext && (localLimit < 0 || counter < localLimit)) {
            val batch = iter.next()
            val results = serdeJni.serialize(
              Array(ColumnarBatches.getNativeHandle(batch)),
              nativeMemoryManagerHandle
            )
            if (results.getNumRows > 0) {
              out.writeInt(results.getSerialized.length)
              out.write(results.getSerialized)
              counter += results.getNumRows
            }
          }
          out.writeInt(-1)
          out.flush()
        } finally {
          out.close()
        }
        Iterator(cbbos.toChunkedByteBuffer)
    }
  }

  // Refer `SparkPlan.executeTake`
  private def nativeExecuteTake(
      numRowsAndArrayBytesRDD: RDD[ChunkedByteBuffer],
      localLimit: Int,
      bytesToUnsafeRow: BytesToUnsafeRow): Array[InternalRow] = {
    val buf = new ArrayBuffer[InternalRow]
    var partsScanned = 0
    val totalParts = numRowsAndArrayBytesRDD.partitions.length
    val limitScaleUpFactor = Math.max(conf.limitScaleUpFactor, 2)
    var submitJobs = 0
    while (buf.length < limit && partsScanned < totalParts) {
      // The number of partitions to try in this iteration. It is ok for this number to be
      // greater than totalParts because we actually cap it at totalParts in runJob.
      // Hard code config key to be compatible with older Spark version(3.2/3.3)
      var numPartsToTry = conf.getConfString("spark.sql.limit.initialNumPartitions", "1").toInt
      if (partsScanned > 0) {
        // If we didn't find any rows after the previous iteration, multiply by
        // limitScaleUpFactor and retry. Otherwise, interpolate the number of partitions we need
        // to try, but overestimate it by 50%. We also cap the estimation in the end.
        if (buf.isEmpty) {
          numPartsToTry = partsScanned * limitScaleUpFactor
        } else {
          val left = limit - buf.length
          // As left > 0, numPartsToTry is always >= 1
          numPartsToTry = Math.ceil(1.5 * left * partsScanned / buf.length).toInt
          numPartsToTry = Math.min(numPartsToTry, partsScanned * limitScaleUpFactor)
        }
      }

      val partsToScan = partsScanned.until(math.min(partsScanned + numPartsToTry, totalParts))
      val sc = sparkContext
      val res = sc.runJob(
        numRowsAndArrayBytesRDD,
        (it: Iterator[ChunkedByteBuffer]) => if (it.hasNext) it.next() else new ChunkedByteBuffer(),
        partsToScan)
      submitJobs += 1

      var partIndex = 0
      while (buf.length < limit && partIndex < res.length) {
        bytesToUnsafeRow.appendToUnsafeRowBuffer(buf, localLimit, res(partIndex))
        partIndex += 1
      }
      partsScanned += partsToScan.size
    }
    buf.toArray
  }
}

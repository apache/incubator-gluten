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

package org.apache.spark.sql.execution.utils

import scala.collection.JavaConverters._

import io.glutenproject.columnarbatch.ArrowColumnarBatches
import io.glutenproject.expression.{ArrowConverterUtils, ExpressionConverter, ExpressionTransformer}
import io.glutenproject.memory.arrowalloc.ArrowBufferAllocators
import io.glutenproject.substrait.SubstraitContext
import io.glutenproject.substrait.expression.ExpressionNode
import io.glutenproject.substrait.rel.RelBuilder
import io.glutenproject.vectorized.{ArrowWritableColumnVector, NativePartitioning}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, Schema}

import org.apache.spark.{Partitioner, RangePartitioner, ShuffleDependency}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.ColumnarShuffleDependency
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, BoundReference, UnsafeProjection}
import org.apache.spark.sql.catalyst.expressions.codegen.LazilyGeneratedOrdering
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.PartitionIdPassthrough
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.MutablePair
import org.apache.spark.util.memory.TaskMemoryResources

object VeloxExecUtil {
  // scalastyle:off argcount
  def genShuffleDependency(rdd: RDD[ColumnarBatch],
                           outputAttributes: Seq[Attribute],
                           newPartitioning: Partitioning,
                           serializer: Serializer,
                           writeMetrics: Map[String, SQLMetric],
                           dataSize: SQLMetric,
                           bytesSpilled: SQLMetric,
                           numInputRows: SQLMetric,
                           computePidTime: SQLMetric,
                           splitTime: SQLMetric,
                           spillTime: SQLMetric,
                           compressTime: SQLMetric,
                           prepareTime: SQLMetric
                          ): ShuffleDependency[Int, ColumnarBatch, ColumnarBatch] = {
    // scalastyle:on argcount
    // only used for fallback range partitioning
    val rangePartitioner: Option[Partitioner] = newPartitioning match {
      case RangePartitioning(sortingExpressions, numPartitions) =>
        // Extract only fields used for sorting to avoid collecting large fields that does not
        // affect sorting result when deciding partition bounds in RangePartitioner
        val rddForSampling = rdd.mapPartitionsInternal { iter =>
          // Internally, RangePartitioner runs a job on the RDD that samples keys to compute
          // partition bounds. To get accurate samples, we need to copy the mutable keys.
          iter.flatMap(batch => {
            val rows =
              ArrowColumnarBatches
                .ensureLoaded(ArrowBufferAllocators.contextInstance(), batch)
                .rowIterator.asScala
            val projection =
              UnsafeProjection.create(sortingExpressions.map(_.child), outputAttributes)
            val mutablePair = new MutablePair[InternalRow, Null]()
            rows.map(row => mutablePair.update(projection(row).copy(), null))
          })
        }
        // Construct ordering on extracted sort key.
        val orderingAttributes = sortingExpressions.zipWithIndex.map {
          case (ord, i) =>
            ord.copy(child = BoundReference(i, ord.dataType, ord.nullable))
        }
        implicit val ordering = new LazilyGeneratedOrdering(orderingAttributes)
        val part = new RangePartitioner(
          numPartitions,
          rddForSampling,
          ascending = true,
          samplePointsPerPartitionHint = SQLConf.get.rangeExchangeSampleSizePerPartition)
        Some(part)
      case _ => None
    }

    // only used for fallback range partitioning
    def computeAndAddPartitionId(cbIter: Iterator[ColumnarBatch],
                                 partitionKeyExtractor: InternalRow => Any
                                ): CloseablePairedColumnarBatchIterator = {
      CloseablePairedColumnarBatchIterator {
        cbIter
          .filter(cb => cb.numRows != 0 && cb.numCols != 0)
          .map {
            cb => ArrowColumnarBatches.ensureLoaded(ArrowBufferAllocators.contextInstance(), cb)
          }
          .map { cb =>
            val startTime = System.nanoTime()
            val pidVec = ArrowWritableColumnVector
              .allocateColumns(cb.numRows, new StructType().add("pid", IntegerType))
              .head
            (0 until cb.numRows).foreach { i =>
              val row = cb.getRow(i)
              val pid = rangePartitioner.get.getPartition(partitionKeyExtractor(row))
              pidVec.putInt(i, pid)
            }

            val newColumns = (pidVec +: (0 until cb.numCols).map(
              ArrowColumnarBatches
                .ensureLoaded(ArrowBufferAllocators.contextInstance(),
                  cb).column)).toArray
            newColumns.foreach(
              _.asInstanceOf[ArrowWritableColumnVector].getValueVector.setValueCount(cb.numRows))
            computePidTime.add(System.nanoTime() - startTime)
            (0, new ColumnarBatch(newColumns, cb.numRows))
          }
      }
    }

    def serializeSchema(fields: Seq[Field]): Array[Byte] = {
      val schema = new Schema(fields.asJava)
      ArrowConverterUtils.getSchemaBytesBuf(schema)
    }

    val arrowFields = outputAttributes.map(attr =>
      ArrowConverterUtils.createArrowField(attr)
    )

    val nativePartitioning: NativePartitioning = newPartitioning match {
      case SinglePartition =>
        new NativePartitioning("single", 1, serializeSchema(arrowFields))
      case RoundRobinPartitioning(n) =>
        new NativePartitioning("rr", n, serializeSchema(arrowFields))
      case HashPartitioning(exprs, n) =>
        // Function map is not expected to be used.
        val functionMap = new java.util.HashMap[String, java.lang.Long]()
        val exprNodeList = new java.util.ArrayList[ExpressionNode]()
        exprs.foreach(expr => {
          exprNodeList.add(ExpressionConverter
            .replaceWithExpressionTransformer(expr, outputAttributes)
            .asInstanceOf[ExpressionTransformer]
            .doTransform(functionMap))
        })
        val context: SubstraitContext = new SubstraitContext
        val projectRel = RelBuilder.makeProjectRel(
          null, exprNodeList, context, context.nextOperatorId)
        new NativePartitioning(
          "hash",
          n,
          serializeSchema(arrowFields),
          projectRel.toProtobuf().toByteArray)
      // range partitioning fall back to row-based partition id computation
      case RangePartitioning(orders, n) =>
        val pidField = Field.nullable("pid", new ArrowType.Int(32, true))
        new NativePartitioning("range", n, serializeSchema(pidField +: arrowFields))
    }

    val isRoundRobin = newPartitioning.isInstanceOf[RoundRobinPartitioning] &&
      newPartitioning.numPartitions > 1

    // RDD passed to ShuffleDependency should be the form of key-value pairs.
    // ColumnarShuffleWriter will compute ids from ColumnarBatch on native side
    // other than read the "key" part.
    // Thus in Columnar Shuffle we never use the "key" part.
    val isOrderSensitive = isRoundRobin && !SQLConf.get.sortBeforeRepartition

    val rddWithDummyKey: RDD[Product2[Int, ColumnarBatch]] = newPartitioning match {
      case RangePartitioning(sortingExpressions, _) =>
        rdd.mapPartitionsWithIndexInternal((_, cbIter) => {
          val partitionKeyExtractor: InternalRow => Any = {
            val projection =
              UnsafeProjection.create(sortingExpressions.map(_.child), outputAttributes)
            row => projection(row)
          }
          val newIter = computeAndAddPartitionId(cbIter, partitionKeyExtractor)

          TaskMemoryResources.addLeakSafeTaskCompletionListener[Unit] { _ =>
            newIter.closeAppendedVector()
          }

          newIter
        }, isOrderSensitive = isOrderSensitive)
      case _ =>
        rdd.mapPartitionsWithIndexInternal(
          (_, cbIter) =>
            cbIter.map { cb =>
              val acb = ArrowColumnarBatches
                .ensureLoaded(ArrowBufferAllocators.contextInstance(),
                  cb)
              (0 until cb.numCols).foreach(
                acb.column(_)
                  .asInstanceOf[ArrowWritableColumnVector]
                  .getValueVector
                  .setValueCount(acb.numRows))
              (0, acb)
            },
          isOrderSensitive = isOrderSensitive)
    }

    val dependency =
      new ColumnarShuffleDependency[Int, ColumnarBatch, ColumnarBatch](
        rddWithDummyKey,
        new PartitionIdPassthrough(newPartitioning.numPartitions),
        serializer,
        shuffleWriterProcessor =
          ShuffleExchangeExec.createShuffleWriteProcessor(writeMetrics),
        nativePartitioning = nativePartitioning,
        dataSize = dataSize,
        bytesSpilled = bytesSpilled,
        numInputRows = numInputRows,
        computePidTime = computePidTime,
        splitTime = splitTime,
        spillTime = spillTime,
        compressTime = compressTime,
        prepareTime = prepareTime)

    dependency
  }
}


case class CloseablePairedColumnarBatchIterator(iter: Iterator[(Int, ColumnarBatch)])
  extends Iterator[(Int, ColumnarBatch)]
    with Logging {

  private var cur: (Int, ColumnarBatch) = _

  override def hasNext: Boolean = {
    iter.hasNext
  }

  override def next(): (Int, ColumnarBatch) = {
    closeAppendedVector()
    if (iter.hasNext) {
      cur = iter.next()
      cur
    } else Iterator.empty.next()
  }

  def closeAppendedVector(): Unit = {
    if (cur != null) {
      logDebug("Close appended partition id vector")
      cur match {
        case (_, cb: ColumnarBatch) =>
          ArrowColumnarBatches
            .ensureLoaded(ArrowBufferAllocators.contextInstance(),
              cb).column(0).asInstanceOf[ArrowWritableColumnVector].close()
      }
      cur = null
    }
  }
}

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

import io.glutenproject.GlutenConfig
import io.glutenproject.expression.ConverterUtils
import io.glutenproject.vectorized.{BlockSplitIterator, CHNativeBlock, CloseablePartitionedBlockIterator, NativePartitioning}
import io.glutenproject.vectorized.BlockSplitIterator.IteratorOptions

import org.apache.spark.ShuffleDependency
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.ColumnarShuffleDependency
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.PartitionIdPassthrough
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

object CHExecUtil {

  def inferSparkDataType(typeName: String): DataType = typeName match {
    case "Date" => DateType
    case "Float" => FloatType
    case "Double" => DoubleType
    case "Integer" => IntegerType
    case "Long" => LongType
    case "Byte" => ByteType
    case "Short" => ShortType
    case "String" => StringType
    case "Binary" => BinaryType
  }
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
    val nativePartitioning: NativePartitioning = newPartitioning match {
      case SinglePartition => new NativePartitioning("single", 1, Array.empty[Byte])
      case RoundRobinPartitioning(n) =>
        new NativePartitioning("rr", n, Array.empty[Byte])
      case HashPartitioning(exprs, n) =>
        val fields = exprs.zipWithIndex.map {
          case (expr, i) =>
            val attr = ConverterUtils.getAttrFromExpr(expr)
            ConverterUtils.genColumnNameWithExprId(attr)
        }
        new NativePartitioning(
          "hash",
          n,
          null,
          fields.mkString(",").getBytes)
    }

    val isRoundRobin = newPartitioning.isInstanceOf[RoundRobinPartitioning] &&
      newPartitioning.numPartitions > 1

    // RDD passed to ShuffleDependency should be the form of key-value pairs.
    // ColumnarShuffleWriter will compute ids from ColumnarBatch on
    // native side other than read the "key" part.
    // Thus in Columnar Shuffle we never use the "key" part.
    val isOrderSensitive = isRoundRobin && !SQLConf.get.sortBeforeRepartition

    val rddWithpartitionKey: RDD[Product2[Int, ColumnarBatch]] =
      if (GlutenConfig.getConf.isUseColumnarShufflemanager) {
        newPartitioning match {
          case _ =>
            rdd.mapPartitionsWithIndexInternal(
              (_, cbIter) =>
                cbIter.map { cb =>
                  (0, cb)
                },
              isOrderSensitive = isOrderSensitive)
        }
      } else {
        val options = new IteratorOptions
        options.setExpr("")
        options.setBufferSize(8192)
        newPartitioning match {
          case HashPartitioning(exprs, n) =>
            rdd.mapPartitionsWithIndexInternal(
              (_, cbIter) => {
                options.setPartitionNum(n)
                val fields = exprs.zipWithIndex.map {
                  case (expr, i) =>
                    val attr = ConverterUtils.getAttrFromExpr(expr)
                    ConverterUtils.genColumnNameWithExprId(attr)
                }
                options.setExpr(fields.mkString(","))
                options.setName("hash")
                val iter = new Iterator[Product2[Int, ColumnarBatch]] with AutoCloseable {
                  val splitIterator = new BlockSplitIterator(
                    cbIter.map(cb =>
                      CHNativeBlock.fromColumnarBatch(cb)
                        .orElseThrow(() => new IllegalStateException("unsupported columnar batch"))
                        .blockAddress().asInstanceOf[java.lang.Long]).asJava, options)

                  override def hasNext: Boolean = splitIterator.hasNext

                  override def next(): Product2[Int, ColumnarBatch] =
                    (splitIterator.nextPartitionId(), splitIterator.next());

                  override def close(): Unit = splitIterator.close()
                }
                new CloseablePartitionedBlockIterator(iter)
              },
              isOrderSensitive = isOrderSensitive)
          case RoundRobinPartitioning(n) =>
            rdd.mapPartitionsWithIndexInternal(
              (_, cbIter) => {
                options.setPartitionNum(n)
                options.setName("rr")
                val iter = new Iterator[Product2[Int, ColumnarBatch]] with AutoCloseable {
                  val splitIterator = new BlockSplitIterator(
                    cbIter.map(cb =>
                      CHNativeBlock.fromColumnarBatch(cb)
                        .orElseThrow(() => new IllegalStateException("unsupported columnar batch"))
                        .blockAddress().asInstanceOf[java.lang.Long]).asJava, options)

                  override def hasNext: Boolean = splitIterator.hasNext

                  override def next(): Product2[Int, ColumnarBatch] =
                    (splitIterator.nextPartitionId(), splitIterator.next());

                  override def close(): Unit = splitIterator.close()
                }
                new CloseablePartitionedBlockIterator(iter)
              },
              isOrderSensitive = isOrderSensitive)
          case SinglePartition =>
            rdd.mapPartitionsWithIndexInternal(
              (_, cbIter) => {
                options.setPartitionNum(1)
                options.setName("rr")
                val iter = new Iterator[Product2[Int, ColumnarBatch]] with AutoCloseable {
                  val splitIterator = new BlockSplitIterator(
                    cbIter.map(cb =>
                      CHNativeBlock.fromColumnarBatch(cb)
                        .orElseThrow(() => new IllegalStateException("unsupported columnar batch"))
                        .blockAddress().asInstanceOf[java.lang.Long]).asJava, options)

                  override def hasNext: Boolean = splitIterator.hasNext

                  override def next(): Product2[Int, ColumnarBatch] =
                    (splitIterator.nextPartitionId(), splitIterator.next());

                  override def close(): Unit = splitIterator.close()
                }
                new CloseablePartitionedBlockIterator(iter)
              },
              isOrderSensitive = isOrderSensitive)
          case _ =>
            throw new UnsupportedOperationException(s"Unsupport operators.")
        }
      }

    val dependency =
      new ColumnarShuffleDependency[Int, ColumnarBatch, ColumnarBatch](
        rddWithpartitionKey,
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

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

import io.glutenproject.GlutenConfig
import io.glutenproject.expression.ConverterUtils
import io.glutenproject.vectorized._
import io.glutenproject.vectorized.BlockSplitIterator.IteratorOptions

import org.apache.spark.ShuffleDependency
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.ColumnarShuffleDependency
import org.apache.spark.shuffle.utils.RangePartitionerBoundsGenerator
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences, BoundReference, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.LazilyGeneratedOrdering
import org.apache.spark.sql.catalyst.plans.physical.{SinglePartition, _}
import org.apache.spark.sql.execution.PartitionIdPassthrough
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.MutablePair

import io.substrait.proto.Type

import scala.collection.JavaConverters._

object CHExecUtil extends Logging {

  def inferSparkDataType(substraitType: Array[Byte]): DataType = {
    val (datatype, nullable) =
      ConverterUtils.parseFromSubstraitType(Type.parseFrom(substraitType))
    datatype
  }

  def buildRangePartitionSampleRDD(
      rdd: RDD[ColumnarBatch],
      rangePartitioning: RangePartitioning,
      outputAttributes: Seq[Attribute]): RDD[MutablePair[InternalRow, Null]] = {
    rangePartitioning match {
      case RangePartitioning(sortingExpressions, _) =>
        val sampleRDD = rdd.mapPartitionsInternal {
          iter =>
            iter.flatMap(
              batch => {
                val jniWrapper = new BlockNativeConverter()
                val nativeBlock = CHNativeBlock.fromColumnarBatch(batch)
                val blockAddress = nativeBlock.get().blockAddress()
                val rowInfo = jniWrapper.convertColumnarToRow(blockAddress)

                // generate rows from a columnar batch
                val rows = new Iterator[InternalRow] {
                  var rowId = 0
                  val row = new UnsafeRow(batch.numCols())
                  var closed = false

                  override def hasNext: Boolean = {
                    val result = rowId < batch.numRows()
                    if (!result && !closed) {
                      jniWrapper.freeMemory(rowInfo.memoryAddress, rowInfo.totalSize)
                      closed = true
                    }
                    result
                  }

                  override def next: UnsafeRow = {
                    if (rowId >= batch.numRows()) throw new NoSuchElementException

                    val (offset, length) = (rowInfo.offsets(rowId), rowInfo.lengths(rowId))
                    row.pointTo(null, rowInfo.memoryAddress + offset, length.toInt)
                    rowId += 1
                    row
                  }
                }
                val projection =
                  UnsafeProjection.create(sortingExpressions.map(_.child), outputAttributes)
                val mutablePair = new MutablePair[InternalRow, Null]()
                rows.map(row => mutablePair.update(projection(row).copy(), null))
              })
        }
        sampleRDD
    }
  }

  private def buildPartitionedBlockIterator(
      cbIter: Iterator[ColumnarBatch],
      options: IteratorOptions): CloseablePartitionedBlockIterator = {
    val iter = new Iterator[Product2[Int, ColumnarBatch]] with AutoCloseable {
      val splitIterator = new BlockSplitIterator(
        cbIter
          .map(
            cb =>
              CHNativeBlock
                .fromColumnarBatch(cb)
                .orElseThrow(() => new IllegalStateException("unsupported columnar batch"))
                .blockAddress()
                .asInstanceOf[java.lang.Long])
          .asJava,
        options
      )
      override def hasNext: Boolean = splitIterator.hasNext
      override def next(): Product2[Int, ColumnarBatch] =
        (splitIterator.nextPartitionId(), splitIterator.next());
      override def close(): Unit = splitIterator.close();
    }
    new CloseablePartitionedBlockIterator(iter)
  }

  private def buildHashPartitioning(
      partitoining: HashPartitioning,
      childOutput: Seq[Attribute],
      output: Seq[Attribute]): NativePartitioning = {
    val hashFields = partitoining.expressions.map {
      case a: Attribute =>
        BindReferences
          .bindReference(a, childOutput)
          .asInstanceOf[BoundReference]
          .ordinal
    }
    val outputFields = if (output != null) {
      output.map {
        case a: Attribute =>
          BindReferences
            .bindReference(a, childOutput)
            .asInstanceOf[BoundReference]
            .ordinal
      }
    } else {
      Seq[Int]()
    }

    new NativePartitioning(
      "hash",
      partitoining.numPartitions,
      Array.empty[Byte],
      hashFields.mkString(",").getBytes(),
      outputFields.mkString(",").getBytes())
  }

  private def buildPartitioningOptions(nativePartitioning: NativePartitioning): IteratorOptions = {
    val options = new IteratorOptions
    options.setBufferSize(GlutenConfig.getConf.maxBatchSize)
    options.setName(nativePartitioning.getShortName)
    options.setPartitionNum(nativePartitioning.getNumPartitions)
    options.setExpr(new String(nativePartitioning.getExprList))
    options.setRequiredFields(if (nativePartitioning.getRequiredFields != null) {
      new String(nativePartitioning.getRequiredFields)
    } else {
      new String("")
    })
    options
  }

  // scalastyle:off argcount
  def genShuffleDependency(
      rdd: RDD[ColumnarBatch],
      childOutputAttributes: Seq[Attribute],
      projectOutputAttributes: Seq[Attribute],
      newPartitioning: Partitioning,
      serializer: Serializer,
      writeMetrics: Map[String, SQLMetric],
      metrics: Map[String, SQLMetric]): ShuffleDependency[Int, ColumnarBatch, ColumnarBatch] = {
    // scalastyle:on argcount
    lazy val requiredFields = if (projectOutputAttributes != null) {
      val outputFields = projectOutputAttributes.map {
        case a: Attribute =>
          BindReferences
            .bindReference(a, childOutputAttributes)
            .asInstanceOf[BoundReference]
            .ordinal
      }
      outputFields.mkString(",").getBytes()
    } else {
      Array.empty[Byte]
    }
    val nativePartitioning: NativePartitioning = newPartitioning match {
      case SinglePartition =>
        new NativePartitioning("single", 1, Array.empty[Byte], Array.empty[Byte], requiredFields)
      case RoundRobinPartitioning(n) =>
        new NativePartitioning("rr", n, Array.empty[Byte], Array.empty[Byte], requiredFields)
      case HashPartitioning(_, _) =>
        buildHashPartitioning(
          newPartitioning.asInstanceOf[HashPartitioning],
          childOutputAttributes,
          projectOutputAttributes)
      case RangePartitioning(sortingExpressions, numPartitions) =>
        val rddForSampling = buildRangePartitionSampleRDD(
          rdd,
          RangePartitioning(sortingExpressions, numPartitions),
          childOutputAttributes)

        // we let spark compute the range bounds here, and then pass to CH.
        val orderingAttributes = sortingExpressions.zipWithIndex.map {
          case (ord, i) =>
            ord.copy(child = BoundReference(i, ord.dataType, ord.nullable))
        }
        implicit val ordering = new LazilyGeneratedOrdering(orderingAttributes)
        val generator = new RangePartitionerBoundsGenerator(
          numPartitions,
          rddForSampling,
          sortingExpressions,
          childOutputAttributes)
        val orderingAndRangeBounds = generator.getRangeBoundsJsonString()
        val attributePos = if (projectOutputAttributes != null) {
          projectOutputAttributes.map(
            attr =>
              BindReferences
                .bindReference(attr, childOutputAttributes)
                .asInstanceOf[BoundReference]
                .ordinal)
        } else {
          Seq[Int]()
        }
        new NativePartitioning(
          "range",
          numPartitions,
          Array.empty[Byte],
          orderingAndRangeBounds.getBytes(),
          attributePos.mkString(",").getBytes)
      case p =>
        throw new IllegalStateException(s"Unknow partition type: ${p.getClass.toString}")
    }

    val isRoundRobin = newPartitioning.isInstanceOf[RoundRobinPartitioning] &&
      newPartitioning.numPartitions > 1

    // RDD passed to ShuffleDependency should be the form of key-value pairs.
    // ColumnarShuffleWriter will compute ids from ColumnarBatch on
    // native side other than read the "key" part.
    // Thus in Columnar Shuffle we never use the "key" part.
    val isOrderSensitive = isRoundRobin && !SQLConf.get.sortBeforeRepartition

    val rddWithpartitionKey: RDD[Product2[Int, ColumnarBatch]] =
      if (GlutenConfig.getConf.isUseColumnarShuffleManager) {
        newPartitioning match {
          case _ =>
            rdd.mapPartitionsWithIndexInternal(
              (_, cbIter) => cbIter.map(cb => (0, cb)),
              isOrderSensitive = isOrderSensitive)
        }
      } else {
        val options = buildPartitioningOptions(nativePartitioning)
        rdd.mapPartitionsWithIndexInternal(
          (_, cbIter) => {
            buildPartitionedBlockIterator(cbIter, options)
          },
          isOrderSensitive = isOrderSensitive
        )
      }

    val dependency =
      new ColumnarShuffleDependency[Int, ColumnarBatch, ColumnarBatch](
        rddWithpartitionKey,
        new PartitionIdPassthrough(newPartitioning.numPartitions),
        serializer,
        shuffleWriterProcessor = ShuffleExchangeExec.createShuffleWriteProcessor(writeMetrics),
        nativePartitioning = nativePartitioning,
        metrics = metrics
      )

    dependency
  }
}

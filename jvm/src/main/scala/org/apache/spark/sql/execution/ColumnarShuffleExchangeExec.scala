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

import java.util

import com.google.common.collect.Lists
import com.intel.oap.expression.{CodeGeneration, ConverterUtils, ExpressionConverter, ExpressionTransformer}
import com.intel.oap.substrait.expression.ExpressionNode
import com.intel.oap.substrait.rel.RelBuilder
import com.intel.oap.vectorized.{ArrowColumnarBatchSerializer, ArrowWritableColumnVector, NativePartitioning}
import org.apache.arrow.gandiva.expression.{TreeBuilder, TreeNode}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.{ColumnarShuffleDependency, ShuffleHandle}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.LazilyGeneratedOrdering
import org.apache.spark.sql.catalyst.expressions.{Attribute, BoundReference, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.execution.CoalesceExec.EmptyPartition
import org.apache.spark.sql.execution.datasources.v2.arrow.SparkMemoryUtils
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec.createShuffleWriteProcessor
import org.apache.spark.sql.execution.exchange._
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics, SQLShuffleReadMetricsReporter, SQLShuffleWriteMetricsReporter}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.{MutablePair, Utils}

import scala.collection.JavaConverters._
import scala.concurrent.Future
import org.apache.spark.sql.util.ArrowUtils

case class ColumnarShuffleExchangeExec(override val outputPartitioning: Partitioning,
                                       child: SparkPlan,
                                       shuffleOrigin: ShuffleOrigin = ENSURE_REQUIREMENTS)
  extends Exchange {

  private[sql] lazy val writeMetrics =
    SQLShuffleWriteMetricsReporter.createShuffleWriteMetrics(sparkContext)
  private[sql] lazy val readMetrics =
    SQLShuffleReadMetricsReporter.createShuffleReadMetrics(sparkContext)
  override lazy val metrics: Map[String, SQLMetric] = Map(
    "dataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size"),
    "bytesSpilled" -> SQLMetrics.createSizeMetric(sparkContext, "shuffle bytes spilled"),
    "computePidTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "totaltime_computepid"),
    "splitTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "totaltime_split"),
    "spillTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "shuffle spill time"),
    "compressTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "totaltime_compress"),
    "prepareTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "totaltime_prepare"),
    "avgReadBatchNumRows" -> SQLMetrics
      .createAverageMetric(sparkContext, "avg read batch num rows"),
    "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
    "numOutputRows" -> SQLMetrics
      .createMetric(sparkContext, "number of output rows")) ++ readMetrics ++ writeMetrics

  override def nodeName: String = "ColumnarExchange"
  override def output: Seq[Attribute] = child.output
  buildCheck()

  override def supportsColumnar: Boolean = true

  override def stringArgs =
    super.stringArgs ++ Iterator(s"[id=#$id]")
  //super.stringArgs ++ Iterator(output.map(o => s"${o}#${o.dataType.simpleString}"))

  def buildCheck(): Unit = {
    // check input datatype
    for (attr <- child.output) {
      try {
        ConverterUtils.createArrowField(attr)
      } catch {
        case e: UnsupportedOperationException =>
          throw new UnsupportedOperationException(
            s"${attr.dataType} is not supported in ColumnarShuffledExchangeExec.")
      }
    }
  }

  val serializer: Serializer = new ArrowColumnarBatchSerializer(
    schema,
    longMetric("avgReadBatchNumRows"),
    longMetric("numOutputRows"))

  @transient lazy val inputColumnarRDD: RDD[ColumnarBatch] = child.executeColumnar()

  // 'mapOutputStatisticsFuture' is only needed when enable AQE.
  @transient lazy val mapOutputStatisticsFuture: Future[MapOutputStatistics] = {
    if (inputColumnarRDD.getNumPartitions == 0) {
      Future.successful(null)
    } else {
      sparkContext.submitMapStage(columnarShuffleDependency)
    }
  }

  /**
   * A [[ShuffleDependency]] that will partition rows of its child based on
   * the partitioning scheme defined in `newPartitioning`. Those partitions of
   * the returned ShuffleDependency will be the input of shuffle.
   */
  @transient
  lazy val columnarShuffleDependency: ShuffleDependency[Int, ColumnarBatch, ColumnarBatch] = {
    ColumnarShuffleExchangeExec.prepareShuffleDependency(
      inputColumnarRDD,
      child.output,
      outputPartitioning,
      serializer,
      writeMetrics,
      longMetric("dataSize"),
      longMetric("bytesSpilled"),
      longMetric("numInputRows"),
      longMetric("computePidTime"),
      longMetric("splitTime"),
      longMetric("spillTime"),
      longMetric("compressTime"),
      longMetric("prepareTime"))
  }

  override def verboseString(maxFields: Int): String = toString(super.verboseString(maxFields))

  override def simpleString(maxFields: Int): String = toString(super.simpleString(maxFields))

  private def toString(original: String): String = {
    original + ", [OUTPUT] " + output.map {
      attr =>
        attr.name + ":" + attr.dataType
    }.toString()
  }

  var cachedShuffleRDD: ShuffledColumnarBatchRDD = _
  override def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException()
  }
  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    if (cachedShuffleRDD == null) {
      cachedShuffleRDD = new ShuffledColumnarBatchRDD(columnarShuffleDependency, readMetrics)
    }
    cachedShuffleRDD
  }

  // 'shuffleDependency' is only needed when enable AQE. Columnar shuffle will use 'columnarShuffleDependency'
  @transient
  lazy val shuffleDependency: ShuffleDependency[Int, InternalRow, InternalRow] =
  new ShuffleDependency[Int, InternalRow, InternalRow](
    _rdd = new ColumnarShuffleExchangeExec.DummyPairRDDWithPartitions(
      sparkContext,
      inputColumnarRDD.getNumPartitions),
    partitioner = columnarShuffleDependency.partitioner) {

    override val shuffleId: Int = columnarShuffleDependency.shuffleId

    override val shuffleHandle: ShuffleHandle = columnarShuffleDependency.shuffleHandle
  }

}

class ColumnarShuffleExchangeAdaptor(override val outputPartitioning: Partitioning,
                                     child: SparkPlan,
                                     shuffleOrigin: ShuffleOrigin = ENSURE_REQUIREMENTS)
  extends ShuffleExchangeExec(outputPartitioning, child) {

  private[sql] lazy val writeMetrics =
    SQLShuffleWriteMetricsReporter.createShuffleWriteMetrics(sparkContext)
  private[sql] override lazy val readMetrics =
    SQLShuffleReadMetricsReporter.createShuffleReadMetrics(sparkContext)
  override lazy val metrics: Map[String, SQLMetric] = Map(
    "dataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size"),
    "bytesSpilled" -> SQLMetrics.createSizeMetric(sparkContext, "shuffle bytes spilled"),
    "computePidTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "totaltime_computepid"),
    "splitTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "totaltime_split"),
    "spillTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "shuffle spill time"),
    "compressTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "totaltime_compress"),
    "prepareTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "totaltime_prepare"),
    "avgReadBatchNumRows" -> SQLMetrics
      .createAverageMetric(sparkContext, "avg read batch num rows"),
    "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
    "numOutputRows" -> SQLMetrics
      .createMetric(sparkContext, "number of output rows")) ++ readMetrics ++ writeMetrics

  override def nodeName: String = "ColumnarExchange"
  override def output: Seq[Attribute] = child.output

  override def supportsColumnar: Boolean = true

  override def stringArgs =
    super.stringArgs ++ Iterator(s"[id=#$id]")
  //super.stringArgs ++ Iterator(output.map(o => s"${o}#${o.dataType.simpleString}"))

  val serializer: Serializer = new ArrowColumnarBatchSerializer(
    schema,
    longMetric("avgReadBatchNumRows"),
    longMetric("numOutputRows"))

  @transient lazy val inputColumnarRDD: RDD[ColumnarBatch] = child.executeColumnar()

  // 'mapOutputStatisticsFuture' is only needed when enable AQE.
  @transient override lazy val mapOutputStatisticsFuture: Future[MapOutputStatistics] = {
    if (inputColumnarRDD.getNumPartitions == 0) {
      Future.successful(null)
    } else {
      sparkContext.submitMapStage(columnarShuffleDependency)
    }
  }

  /**
   * A [[ShuffleDependency]] that will partition rows of its child based on
   * the partitioning scheme defined in `newPartitioning`. Those partitions of
   * the returned ShuffleDependency will be the input of shuffle.
   */
  @transient
  lazy val columnarShuffleDependency: ShuffleDependency[Int, ColumnarBatch, ColumnarBatch] = {
    ColumnarShuffleExchangeExec.prepareShuffleDependency(
      inputColumnarRDD,
      child.output,
      outputPartitioning,
      serializer,
      writeMetrics,
      longMetric("dataSize"),
      longMetric("bytesSpilled"),
      longMetric("numInputRows"),
      longMetric("computePidTime"),
      longMetric("splitTime"),
      longMetric("spillTime"),
      longMetric("compressTime"),
      longMetric("prepareTime"))
  }

  var cachedShuffleRDD: ShuffledColumnarBatchRDD = _
  override def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException()
  }
  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    if (cachedShuffleRDD == null) {
      cachedShuffleRDD = new ShuffledColumnarBatchRDD(columnarShuffleDependency, readMetrics)
    }
    cachedShuffleRDD
  }

  // 'shuffleDependency' is only needed when enable AQE. Columnar shuffle will use 'columnarShuffleDependency'
  @transient
  override lazy val shuffleDependency: ShuffleDependency[Int, InternalRow, InternalRow] =
  new ShuffleDependency[Int, InternalRow, InternalRow](
    _rdd = new ColumnarShuffleExchangeExec.DummyPairRDDWithPartitions(
      sparkContext,
      inputColumnarRDD.getNumPartitions),
    partitioner = columnarShuffleDependency.partitioner) {

    override val shuffleId: Int = columnarShuffleDependency.shuffleId

    override val shuffleHandle: ShuffleHandle = columnarShuffleDependency.shuffleHandle
  }

  override def canEqual(other: Any): Boolean = other.isInstanceOf[ColumnarShuffleExchangeAdaptor]

  override def equals(other: Any): Boolean = other match {
    case that: ColumnarShuffleExchangeAdaptor =>
      (that canEqual this) && super.equals(that)
    case _ => false
  }

  override def verboseString(maxFields: Int): String = toString(super.verboseString(maxFields))

  override def simpleString(maxFields: Int): String = toString(super.simpleString(maxFields))

  private def toString(original: String): String = {
    original + ", [OUTPUT] " + output.map {
      attr =>
        attr.name + ":" + attr.dataType
    }.toString()
  }

}

object ColumnarShuffleExchangeExec extends Logging {

  class DummyPairRDDWithPartitions(@transient private val sc: SparkContext, numPartitions: Int)
    extends RDD[Product2[Int, InternalRow]](sc, Nil) {

    override def getPartitions: Array[Partition] =
      Array.tabulate(numPartitions)(i => EmptyPartition(i))

    override def compute(
                          split: Partition,
                          context: TaskContext): Iterator[Product2[Int, InternalRow]] = {
      throw new UnsupportedOperationException
    }
  }

  def prepareShuffleDependency(rdd: RDD[ColumnarBatch],
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
                               prepareTime: SQLMetric): ShuffleDependency[Int, ColumnarBatch, ColumnarBatch] = {
    val arrowFields = outputAttributes.map(attr => ConverterUtils.createArrowField(attr))
    def serializeSchema(fields: Seq[Field]): Array[Byte] = {
      val schema = new Schema(fields.asJava)
      ConverterUtils.getSchemaBytesBuf(schema)
    }

    // only used for fallback range partitioning
    val rangePartitioner: Option[Partitioner] = newPartitioning match {
      case RangePartitioning(sortingExpressions, numPartitions) =>
        // Extract only fields used for sorting to avoid collecting large fields that does not
        // affect sorting result when deciding partition bounds in RangePartitioner
        val rddForSampling = rdd.mapPartitionsInternal { iter =>
          // Internally, RangePartitioner runs a job on the RDD that samples keys to compute
          // partition bounds. To get accurate samples, we need to copy the mutable keys.
          iter.flatMap(batch => {
            val rows = batch.rowIterator.asScala
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
    def computeAndAddPartitionId(
                                  cbIter: Iterator[ColumnarBatch],
                                  partitionKeyExtractor: InternalRow => Any): CloseablePairedColumnarBatchIterator = {
      CloseablePairedColumnarBatchIterator {
        cbIter
          .filter(cb => cb.numRows != 0 && cb.numCols != 0)
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

            val newColumns = (pidVec +: (0 until cb.numCols).map(cb.column)).toArray
            newColumns.foreach(
              _.asInstanceOf[ArrowWritableColumnVector].getValueVector.setValueCount(cb.numRows))
            computePidTime.add(System.nanoTime() - startTime)
            (0, new ColumnarBatch(newColumns, cb.numRows))
          }
      }
    }

    val nativePartitioning: NativePartitioning = newPartitioning match {
      case SinglePartition => new NativePartitioning("single", 1, serializeSchema(arrowFields))
      case RoundRobinPartitioning(n) =>
        new NativePartitioning("rr", n, serializeSchema(arrowFields))
      case HashPartitioning(exprs, n) =>
        // Function map is not expected to be used.
        val functionMap = new java.util.HashMap[String, java.lang.Long]()
        val exprNodeList = new util.ArrayList[ExpressionNode]()
        exprs.foreach(expr => {
          if (!expr.isInstanceOf[Attribute]) {
            throw new UnsupportedOperationException(
              "Expressions are not supported in HashPartitioning.")
          }
          exprNodeList.add(ExpressionConverter
            .replaceWithExpressionTransformer(expr, outputAttributes)
            .asInstanceOf[ExpressionTransformer]
            .doTransform(functionMap))
        })
        val projectRel = RelBuilder.makeProjectRel(null, exprNodeList)
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
    // ColumnarShuffleWriter will compute ids from ColumnarBatch on native side other than read the "key" part.
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

          SparkMemoryUtils.addLeakSafeTaskCompletionListener[Unit] { _ =>
            newIter.closeAppendedVector()
          }

          newIter
        }, isOrderSensitive = isOrderSensitive)
      case _ =>
        rdd.mapPartitionsWithIndexInternal(
          (_, cbIter) =>
            cbIter.map { cb =>
              (0 until cb.numCols).foreach(
                cb.column(_)
                  .asInstanceOf[ArrowWritableColumnVector]
                  .getValueVector
                  .setValueCount(cb.numRows))
              (0, cb)
            },
          isOrderSensitive = isOrderSensitive)
    }

    val dependency =
      new ColumnarShuffleDependency[Int, ColumnarBatch, ColumnarBatch](
        rddWithDummyKey,
        new PartitionIdPassthrough(newPartitioning.numPartitions),
        serializer,
        shuffleWriterProcessor = createShuffleWriteProcessor(writeMetrics),
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

  def closeAppendedVector(): Unit = {
    if (cur != null) {
      logDebug("Close appended partition id vector")
      cur match {
        case (_, cb: ColumnarBatch) =>
          cb.column(0).asInstanceOf[ArrowWritableColumnVector].close()
      }
      cur = null
    }
  }

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
}

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

import java.util.UUID
import java.util.concurrent._

import com.google.common.collect.Lists
import com.intel.oap.expression._
import com.intel.oap.vectorized.{ArrowWritableColumnVector, ExpressionEvaluator}
import org.apache.arrow.gandiva.expression._
import org.apache.arrow.vector.types.pojo.{ArrowType, Field}
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastMode, _}
import org.apache.spark.sql.execution.datasources.v2.arrow.SparkMemoryUtils
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, _}
import org.apache.spark.sql.execution.joins.HashedRelationBroadcastMode
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SparkFatalException
import org.apache.spark.{SparkException, broadcast}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Promise
import scala.concurrent.duration.NANOSECONDS
import scala.util.control.NonFatal

case class ColumnarBroadcastExchangeExec(mode: BroadcastMode, child: SparkPlan) extends Exchange {

  override def supportsColumnar = true
  override def output: Seq[Attribute] = child.output

  private[sql] val runId: UUID = UUID.randomUUID

  override def outputPartitioning: Partitioning = BroadcastPartitioning(mode)

  override def doCanonicalize(): SparkPlan = {
    ColumnarBroadcastExchangeExec(mode.canonicalized, child.canonicalized)
  }

  @transient
  private val timeout: Long = SQLConf.get.broadcastTimeout

  override lazy val metrics = Map(
    "dataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size"),
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of Rows"),
    "totalTime" -> SQLMetrics.createTimingMetric(sparkContext, "totaltime_broadcastExchange"),
    "collectTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to collect"),
    "buildTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to build"),
    "broadcastTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to broadcast"))

  val buildKeyExprs: Seq[Expression] = mode match {
    case hashRelationMode: HashedRelationBroadcastMode =>
      hashRelationMode.key
    case _ =>
      throw new UnsupportedOperationException(
        s"ColumnarBroadcastExchange only support HashRelationMode")
  }

  @transient
  lazy val promise = Promise[broadcast.Broadcast[Any]]()

  @transient
  lazy val completionFuture: scala.concurrent.Future[broadcast.Broadcast[Any]] =
    promise.future

  @transient
  private[sql] lazy val relationFuture: java.util.concurrent.Future[broadcast.Broadcast[Any]] = {
    SQLExecution.withThreadLocalCaptured[broadcast.Broadcast[Any]](
      sqlContext.sparkSession,
      BroadcastExchangeExec.executionContext) {
      var relation: Any = null
      try {
        // Setup a job group here so later it may get cancelled by groupId if necessary.
        sparkContext.setJobGroup(
          runId.toString,
          s"broadcast exchange (runId $runId)",
          interruptOnCancel = true)
        val beforeCollect = System.nanoTime()

        ///////////////////// Collect Raw RecordBatches from all executors /////////////////
        val countsAndBytes = child
          .executeColumnar()
          .mapPartitions { iter =>
            var _numRows: Long = 0
            val _input = new ArrayBuffer[ColumnarBatch]()

            while (iter.hasNext) {
              val batch = iter.next
              (0 until batch.numCols).foreach(i =>
                batch.column(i).asInstanceOf[ArrowWritableColumnVector].retain())
              _numRows += batch.numRows
              _input += batch
            }
            val bytes = ConverterUtils.convertToNetty(_input.toArray)
            _input.foreach(_.close)

            Iterator((_numRows, bytes))
          }
          .collect
        ///////////////////////////////////////////////////////////////////////////
        val input = countsAndBytes.map(_._2)
        val size_raw = input.map(_.length).sum
        val hash_relation_schema = ConverterUtils.toArrowSchema(output)

        ///////////// After collect data to driver side, build hashmap here /////////////
        val beforeBuild = System.nanoTime()
        // FIXME
//        val hash_relation_function =
//          ColumnarConditionedProbeJoin.prepareHashBuildFunction(buildKeyExprs, output, 1, true)
        val hash_relation_function: TreeNode = null
        val hash_relation_expr =
          TreeBuilder.makeExpression(
            hash_relation_function,
            Field.nullable("result", new ArrowType.Int(32, true)))
        val hashRelationKernel = new ExpressionEvaluator()
        hashRelationKernel.build(
          hash_relation_schema,
          Lists.newArrayList(hash_relation_expr),
          null,
          true,
          SparkMemoryUtils.globalMemoryPool())
        val iter = ConverterUtils.convertFromNetty(output, input)
        var numRows: Long = 0
        val _input = new ArrayBuffer[ColumnarBatch]()
        while (iter.hasNext) {
          val batch = iter.next
          if (batch.numRows > 0) {
            _input += batch
            numRows += batch.numRows
            val dep_rb = ConverterUtils.createArrowRecordBatch(batch)
            hashRelationKernel.evaluate(dep_rb)
            ConverterUtils.releaseArrowRecordBatch(dep_rb)
          }
        }
        // Note: Do not close this object manually. GC will take over that via Cleaner for ColumnarHashedRelation
        val hashRelationResultIterator = hashRelationKernel.finishByIterator()
        val hashRelationObj = hashRelationResultIterator.nextHashRelationObject()
        hashRelationKernel.close()
        relation =
          new ColumnarHashedRelation(hashRelationObj, _input.toArray, size_raw).asReadOnlyCopy
        val dataSize = relation.asInstanceOf[ColumnarHashedRelation].size

        longMetric("buildTime") += NANOSECONDS.toMillis(System.nanoTime() - beforeBuild)

        /////////////////////////////////////////////////////////////////////////////

        if (numRows >= BroadcastExchangeExec.MAX_BROADCAST_TABLE_ROWS) {
          throw new SparkException(
            s"Cannot broadcast the table over ${BroadcastExchangeExec.MAX_BROADCAST_TABLE_ROWS} rows: $numRows rows")
        }

        longMetric("collectTime") += NANOSECONDS.toMillis(System.nanoTime() - beforeCollect)

        longMetric("numOutputRows") += numRows
        longMetric("dataSize") += dataSize
        if (dataSize >= BroadcastExchangeExec.MAX_BROADCAST_TABLE_BYTES) {
          throw new SparkException(
            s"Cannot broadcast the table that is larger than 8GB: ${dataSize >> 30} GB")
        }

        val beforeBroadcast = System.nanoTime()

        // Broadcast the relation
        val broadcasted = sparkContext.broadcast(relation)
        longMetric("broadcastTime") += NANOSECONDS.toMillis(System.nanoTime() - beforeBroadcast)
        longMetric("totalTime").merge(longMetric("collectTime"))
        longMetric("totalTime").merge(longMetric("broadcastTime"))
        val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
        SQLMetrics.postDriverMetricUpdates(sparkContext, executionId, metrics.values.toSeq)
        promise.success(broadcasted)
        broadcasted
      } catch {
        // SPARK-24294: To bypass scala bug: https://github.com/scala/bug/issues/9554, we throw
        // SparkFatalException, which is a subclass of Exception. ThreadUtils.awaitResult
        // will catch this exception and re-throw the wrapped fatal throwable.
        case oe: OutOfMemoryError =>
          val ex = new SparkFatalException(
            new OutOfMemoryError(
              "Not enough memory to build and broadcast the table to all " +
                "worker nodes. As a workaround, you can either disable broadcast by setting " +
                s"${SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key} to -1 or increase the spark " +
                s"driver memory by setting ${SparkLauncher.DRIVER_MEMORY} to a higher value.")
              .initCause(oe.getCause))
          promise.failure(ex)
          throw ex
        case e if !NonFatal(e) =>
          val ex = new SparkFatalException(e)
          promise.failure(ex)
          throw ex
        case e: Throwable =>
          promise.failure(e)
          throw e
      }
    }
  }

  override def doPrepare(): Unit = {
    // Materialize the future.
    relationFuture
  }

  override def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(
      "BroadcastExchange does not support the execute() code path.")
  }

  override protected[sql] def doExecuteBroadcast[T](): broadcast.Broadcast[T] = {
    try {
      relationFuture.get(timeout, TimeUnit.SECONDS).asInstanceOf[broadcast.Broadcast[T]]
    } catch {
      case ex: TimeoutException =>
        logError(s"Could not execute broadcast in $timeout secs.", ex)
        if (!relationFuture.isDone) {
          sparkContext.cancelJobGroup(runId.toString)
          relationFuture.cancel(true)
        }
        throw new SparkException(
          s"Could not execute broadcast in $timeout secs. " +
            s"You can increase the timeout for broadcasts via ${SQLConf.BROADCAST_TIMEOUT.key} or " +
            s"disable broadcast join by setting ${SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key} to -1",
          ex)
    }
  }

}

class ColumnarBroadcastExchangeAdaptor(mode: BroadcastMode, child: SparkPlan)
    extends BroadcastExchangeExec(mode, child) {
  val plan: ColumnarBroadcastExchangeExec = new ColumnarBroadcastExchangeExec(mode, child)

  override def supportsColumnar = true
  override def nodeName: String = plan.nodeName
  override def output: Seq[Attribute] = plan.output

  override val runId: UUID = plan.runId

  override def outputPartitioning: Partitioning = plan.outputPartitioning

  override def doCanonicalize(): SparkPlan = plan.doCanonicalize()

  @transient
  private val timeout: Long = SQLConf.get.broadcastTimeout

  override lazy val metrics = plan.metrics

  val buildKeyExprs: Seq[Expression] = plan.buildKeyExprs

  @transient
  private lazy val promise = plan.promise

  @transient
  lazy override val completionFuture: scala.concurrent.Future[broadcast.Broadcast[Any]] =
    plan.completionFuture

  @transient
  override lazy val relationFuture
      : java.util.concurrent.Future[broadcast.Broadcast[Any]] =
    plan.relationFuture

  override protected def doPrepare(): Unit = plan.doPrepare()

  override protected def doExecute(): RDD[InternalRow] = plan.doExecute()

  override protected[sql] def doExecuteBroadcast[T](): broadcast.Broadcast[T] =
    plan.doExecuteBroadcast[T]()

  override def canEqual(other: Any): Boolean = other.isInstanceOf[ColumnarShuffleExchangeAdaptor]

  override def equals(other: Any): Boolean = other match {
    case that: ColumnarShuffleExchangeAdaptor =>
      (that canEqual this) && super.equals(that)
    case _ => false
  }
}

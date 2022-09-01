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
import java.util.concurrent.{TimeoutException, TimeUnit}

import scala.concurrent.Promise
import scala.concurrent.duration.NANOSECONDS
import scala.util.control.NonFatal

import io.glutenproject.backendsapi.BackendsApiManager

import org.apache.spark.{broadcast, SparkException}
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastMode, BroadcastPartitioning, Partitioning}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeLike, BroadcastExchangeExec, Exchange}
import org.apache.spark.sql.execution.joins.HashedRelationBroadcastMode
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.SparkFatalException

case class ColumnarBroadcastExchangeExec(mode: BroadcastMode, child: SparkPlan) extends Exchange {
  override lazy val metrics = Map(
    "dataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size"),
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of Rows"),
    "totalTime" -> SQLMetrics.createTimingMetric(sparkContext, "totaltime_broadcastExchange"),
    "collectTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to collect"),
    "broadcastTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to broadcast"))
  @transient
  lazy val promise = Promise[broadcast.Broadcast[Any]]()
  @transient
  lazy val completionFuture: scala.concurrent.Future[broadcast.Broadcast[Any]] =
    promise.future
  @transient
  private[sql] lazy val relationFuture: java.util.concurrent.Future[broadcast.Broadcast[Any]] = {
    SQLExecution.withThreadLocalCaptured[broadcast.Broadcast[Any]](
      session,
      BroadcastExchangeExec.executionContext) {
      try {
        // Setup a job group here so later it may get cancelled by groupId if necessary.
        sparkContext.setJobGroup(
          runId.toString,
          s"broadcast exchange (runId $runId)",
          interruptOnCancel = true)

        val relation = BackendsApiManager.getSparkPlanExecApiInstance.createBroadcastRelation(
          mode,
          child,
          longMetric("numOutputRows"),
          longMetric("dataSize"))
        val beforeCollect = System.nanoTime()
        val beforeBroadcast = System.nanoTime()
        longMetric("collectTime") += NANOSECONDS.toMillis(beforeBroadcast - beforeCollect)

        // Broadcast the relation
        val broadcasted = sparkContext.broadcast(relation.asInstanceOf[Any])
        longMetric("broadcastTime") += NANOSECONDS.toMillis(System.nanoTime() - beforeBroadcast)
        longMetric("totalTime").merge(longMetric("collectTime"))
        longMetric("totalTime").merge(longMetric("broadcastTime"))

        // Update driver metrics
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
  // Shouldn't be used.
  val buildKeyExprs: Seq[Expression] = mode match {
    case hashRelationMode: HashedRelationBroadcastMode =>
      hashRelationMode.key
    case _ =>
      throw new UnsupportedOperationException(
        s"ColumnarBroadcastExchange only support HashRelationMode")
  }
  private[sql] val runId: UUID = UUID.randomUUID
  @transient
  private val timeout: Long = SQLConf.get.broadcastTimeout

  override def supportsColumnar: Boolean = true

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = BroadcastPartitioning(mode)

  override def doCanonicalize(): SparkPlan = {
    ColumnarBroadcastExchangeExec(mode.canonicalized, child.canonicalized)
  }

  // FIXME
  def doValidate(): Boolean = true

  override def doPrepare(): Unit = {
    // Materialize the future.
    relationFuture
  }

  override def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(
      "ColumnarBroadcastExchange does not support the execute() code path.")
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
          s"""
             |Could not execute broadcast in $timeout secs.
             |You can increase the timeout for broadcasts via
             |${SQLConf.BROADCAST_TIMEOUT.key} or disable broadcast join
             |by setting ${SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key} to -1
            """.stripMargin,
          ex)
    }
  }
  override protected def withNewChildInternal(newChild: SparkPlan): ColumnarBroadcastExchangeExec =
    copy(child = newChild)
}

case class ColumnarBroadcastExchangeAdaptor(mode: BroadcastMode, child: SparkPlan)
  extends BroadcastExchangeLike {
  val plan: ColumnarBroadcastExchangeExec = new ColumnarBroadcastExchangeExec(mode, child)
  override lazy val metrics: Map[String, SQLMetric] = plan.metrics
  @transient
  lazy override val completionFuture: scala.concurrent.Future[broadcast.Broadcast[Any]] =
    plan.completionFuture
  @transient
  override lazy val relationFuture: java.util.concurrent.Future[broadcast.Broadcast[Any]] =
    plan.relationFuture
  @transient
  private lazy val promise = plan.promise
  override val runId: UUID = plan.runId
  val buildKeyExprs: Seq[Expression] = plan.buildKeyExprs
  @transient
  private val timeout: Long = SQLConf.get.broadcastTimeout

  override def supportsColumnar: Boolean = true

  override def nodeName: String = plan.nodeName

  override def output: Seq[Attribute] = plan.output

  override def outputPartitioning: Partitioning = plan.outputPartitioning

  override def doCanonicalize(): SparkPlan = plan.doCanonicalize()

  // Ported from BroadcastExchangeExec
  override def runtimeStatistics: Statistics = {
    val dataSize = metrics("dataSize").value
    val rowCount = metrics("numOutputRows").value
    Statistics(dataSize, Some(rowCount))
  }

  override def canEqual(other: Any): Boolean = other.isInstanceOf[ColumnarShuffleExchangeAdaptor]

  override def equals(other: Any): Boolean = other match {
    case that: ColumnarShuffleExchangeAdaptor =>
      (that canEqual this) && super.equals(that)
    case _ => false
  }

  override def hashCode(): Int = super.hashCode()

  override protected def doPrepare(): Unit = plan.doPrepare()

  override protected def doExecute(): RDD[InternalRow] = plan.doExecute()

  override protected[sql] def doExecuteBroadcast[T](): broadcast.Broadcast[T] =
    plan.doExecuteBroadcast[T]()

  protected def withNewChildInternal(newChild: SparkPlan): ColumnarBroadcastExchangeAdaptor =
    copy(child = newChild)
}

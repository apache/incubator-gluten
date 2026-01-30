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
package org.apache.spark.sql.delta.stats

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.backendsapi.velox.VeloxBatchType
import org.apache.gluten.columnarbatch.ColumnarBatches
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution._
import org.apache.gluten.expression.{ConverterUtils, TransformerState}
import org.apache.gluten.extension.columnar.heuristic.HeuristicTransform
import org.apache.gluten.extension.columnar.offload.OffloadOthers
import org.apache.gluten.extension.columnar.rewrite.PullOutPreProject
import org.apache.gluten.extension.columnar.transition.Convention
import org.apache.gluten.extension.columnar.validator.{Validator, Validators}
import org.apache.gluten.iterator.Iterators
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.plan.PlanBuilder
import org.apache.gluten.vectorized.{ColumnarBatchInIterator, NativePlanEvaluator}

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, Projection, UnsafeProjection}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Complete, DeclarativeAggregate}
import org.apache.spark.sql.execution.{ColumnarCollapseTransformStages, LeafExecNode}
import org.apache.spark.sql.execution.aggregate.SortAggregateExec
import org.apache.spark.sql.execution.datasources.{BasicWriteJobStatsTracker, WriteJobStatsTracker, WriteTaskStats, WriteTaskStatsTracker}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.{SerializableConfiguration, SparkDirectoryUtil}

import com.google.common.collect.Lists
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import java.util.UUID
import java.util.concurrent.{Callable, Executors, ExecutorService, SynchronousQueue, TimeUnit}

import scala.collection.JavaConverters._
import scala.collection.mutable

/** Gluten's stats tracker with vectorized aggregation inside to produce statistics efficiently. */
private[stats] class GlutenDeltaJobStatsTracker(val delegate: DeltaJobStatisticsTracker)
  extends WriteJobStatsTracker {
  import GlutenDeltaJobStatsTracker._

  @transient private val hadoopConf: Configuration = {
    val clazz = classOf[DeltaJobStatisticsTracker]
    val method = clazz.getDeclaredField("hadoopConf")
    method.setAccessible(true)
    method.get(delegate).asInstanceOf[Configuration]
  }
  @transient private val path = delegate.path
  private val dataCols = delegate.dataCols
  private val statsColExpr = delegate.statsColExpr

  private val srlHadoopConf = new SerializableConfiguration(hadoopConf)
  private val rootUri = path.getFileSystem(hadoopConf).makeQualified(path).toUri

  override def newTaskInstance(): WriteTaskStatsTracker = {
    val rootPath = new Path(rootUri)
    val hadoopConf = srlHadoopConf.value
    new GlutenDeltaTaskStatsTracker(dataCols, statsColExpr, rootPath, hadoopConf)
  }

  override def processStats(stats: Seq[WriteTaskStats], jobCommitTime: Long): Unit = {
    delegate.processStats(stats, jobCommitTime)
  }
}

object GlutenDeltaJobStatsTracker extends Logging {
  def apply(tracker: WriteJobStatsTracker): WriteJobStatsTracker = tracker match {
    case tracker: BasicWriteJobStatsTracker =>
      new GlutenDeltaJobStatsRowCountingTracker(tracker)
    case tracker: DeltaJobStatisticsTracker =>
      new GlutenDeltaJobStatsTracker(tracker)
    case tracker =>
      logWarning(
        "Gluten Delta: Creating fallback job stats tracker," +
          " this involves frequent columnar-to-row conversions which may cause performance" +
          " issues.")
      new GlutenDeltaJobStatsFallbackTracker(tracker)
  }

  /** A columnar-based statistics collection for Gluten + Delta Lake. */
  private class GlutenDeltaTaskStatsTracker(
      dataCols: Seq[Attribute],
      statsColExpr: Expression,
      rootPath: Path,
      hadoopConf: Configuration)
    extends WriteTaskStatsTracker {
    private val resultThreadRunner = Executors.newSingleThreadExecutor()
    private val accumulators = mutable.Map[String, VeloxTaskStatsAccumulator]()
    private val evaluator = NativePlanEvaluator.create(
      BackendsApiManager.getBackendName,
      Map.empty[String, String].asJava)

    override def newPartition(partitionValues: InternalRow): Unit = {}

    override def newFile(filePath: String): Unit = {
      accumulators.getOrElseUpdate(
        filePath,
        new VeloxTaskStatsAccumulator(evaluator, resultThreadRunner, dataCols, statsColExpr)
      )
    }

    override def closeFile(filePath: String): Unit = {
      accumulators(filePath).setFinished()
    }

    override def newRow(filePath: String, row: InternalRow): Unit = {
      row match {
        case _: PlaceholderRow =>
        case t: TerminalRow =>
          accumulators(filePath).appendColumnarBatch(t.batch())
      }
    }

    override def getFinalStats(taskCommitTime: Long): WriteTaskStats = {
      val stats = accumulators.map {
        case (path, acc) =>
          new Path(path).getName -> acc.toJson
      }.toMap
      DeltaFileStatistics(stats)
    }
  }

  private class VeloxTaskStatsAccumulator(
      evaluator: NativePlanEvaluator,
      resultThreadRunner: ExecutorService,
      dataCols: Seq[Attribute],
      statsColExpr: Expression) {
    private val c2r = new VeloxColumnarToRowExec.Converter(new SQLMetric("convertTime"))
    private var resultRequested: Boolean = false
    private val inputBatchQueue = new SynchronousQueue[ColumnarBatch]()
    private val aggregates: Seq[AggregateExpression] = statsColExpr.collect {
      case ae: AggregateExpression if ae.aggregateFunction.isInstanceOf[DeclarativeAggregate] =>
        assert(ae.mode == Complete)
        ae
    }
    private val resultExpr: Expression = statsColExpr.transform {
      case ae: AggregateExpression if ae.aggregateFunction.isInstanceOf[DeclarativeAggregate] =>
        ae.aggregateFunction.asInstanceOf[DeclarativeAggregate].evaluateExpression
    }
    protected val aggBufferAttrs: Seq[Attribute] = statsColExpr.flatMap {
      case ae: AggregateExpression if ae.aggregateFunction.isInstanceOf[DeclarativeAggregate] =>
        ae.aggregateFunction.asInstanceOf[DeclarativeAggregate].aggBufferAttributes
      case _ => None
    }
    private val getStats: Projection = UnsafeProjection.create(
      exprs = Seq(resultExpr),
      inputSchema = aggBufferAttrs
    )
    private val taskContext = TaskContext.get()
    private val outIterator: Iterator[ColumnarBatch] = {
      // Constructs an input iterator receiving the input rows first.
      val inputIterator = new ColumnarBatchInIterator(
        BackendsApiManager.getBackendName,
        Iterators
          .wrap(new Iterator[ColumnarBatch] {
            private var batch: ColumnarBatch = _

            override def hasNext: Boolean = {
              assert(batch == null)
              while (!Thread.currentThread().isInterrupted) {
                val tmp =
                  try {
                    inputBatchQueue.poll(100, TimeUnit.MILLISECONDS)
                  } catch {
                    case _: InterruptedException =>
                      Thread.currentThread().interrupt()
                      return false;
                  }
                if (tmp != null) {
                  batch = tmp
                  return true
                }
                if (resultRequested) {
                  return false
                }
              }
              throw new IllegalStateException()
            }

            override def next(): ColumnarBatch = {
              assert(batch != null)
              try {
                batch
              } finally {
                batch = null
              }
            }
          })
          .recyclePayload(b => b.close())
          .create()
          .asJava
      )

      val inputNode = StatisticsInputNode(dataCols)

      val aggOp = SortAggregateExec(
        None,
        false,
        None,
        Seq.empty,
        aggregates,
        Seq(AttributeReference("stats", StringType)()),
        0,
        Seq.empty,
        inputNode
      )
      // Invoke the legacy transform rule to get a local Velox aggregation query plan.
      val offloads = Seq(OffloadOthers()).map(_.toStrcitRule())
      val validatorBuilder: GlutenConfig => Validator = conf =>
        Validators.newValidator(conf, offloads)
      val rewrites = Seq(PullOutPreProject)
      val config = GlutenConfig.get
      val transformRule =
        HeuristicTransform.WithRewrites(validatorBuilder(config), rewrites, offloads)
      val aggTransformer = ColumnarCollapseTransformStages(config)(transformRule(aggOp))
        .asInstanceOf[WholeStageTransformer]
        .child
        .asInstanceOf[RegularHashAggregateExecTransformer]
      val substraitContext = new SubstraitContext
      TransformerState.enterValidation
      val transformedNode =
        try {
          aggTransformer.transform(substraitContext)
        } finally {
          TransformerState.finishValidation
        }
      val outNames = aggTransformer.output.map(ConverterUtils.genColumnNameWithExprId).asJava
      val planNode =
        PlanBuilder.makePlan(substraitContext, Lists.newArrayList(transformedNode.root), outNames)

      val spillDirPath = SparkDirectoryUtil
        .get()
        .namespace("gluten-spill")
        .mkChildDirRoundRobin(UUID.randomUUID.toString)
        .getAbsolutePath
      val nativeOutItr = evaluator
        .createKernelWithBatchIterator(
          planNode.toProtobuf.toByteArray,
          new Array[Array[Byte]](0),
          Seq(inputIterator).asJava,
          0,
          BackendsApiManager.getSparkPlanExecApiInstance.rewriteSpillPath(spillDirPath)
        )
      Iterators
        .wrap(nativeOutItr.asScala)
        .recyclePayload(b => b.close())
        .recycleIterator(nativeOutItr.close())
        .create()
    }

    private val resultThreadName =
      s"Gluten Delta Statistics Writer - ${System.identityHashCode(this)}"
    private val resJsonFuture = resultThreadRunner.submit(new Callable[String] {
      override def call(): String = {
        Thread.currentThread().setName(resultThreadName)
        TaskContext.setTaskContext(taskContext)
        val rows = outIterator.flatMap(batch => c2r.toRowIterator(batch)).toSeq
        assert(
          rows.size == 1,
          "Only one single output row is expected from the global aggregation.")
        val row = rows.head
        val jsonStats = getStats(row).getString(0)
        jsonStats
      }
    })

    def appendColumnarBatch(inputBatch: ColumnarBatch): Unit = {
      // Retains the input batch so it will be shared by the data writer thread
      // and the statistic writer thread.
      ColumnarBatches.retain(inputBatch)
      inputBatchQueue.put(inputBatch)
    }

    def setFinished(): Unit = {
      resultRequested = true
      resJsonFuture.get() // Blocking wait until the task is released.
    }

    def toJson: String = {
      val jsonStats = resJsonFuture.get()
      jsonStats
    }
  }

  private case class StatisticsInputNode(override val output: Seq[Attribute])
    extends GlutenPlan
    with LeafExecNode {
    override def batchType(): Convention.BatchType = VeloxBatchType
    override def rowType0(): Convention.RowType = Convention.RowType.None
    override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException()
    override protected def doExecuteColumnar(): RDD[ColumnarBatch] =
      throw new UnsupportedOperationException()
  }
}

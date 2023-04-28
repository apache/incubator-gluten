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

import java.util.concurrent.TimeUnit.NANOSECONDS
import scala.collection.mutable.HashMap
import io.glutenproject.GlutenConfig
import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.metrics.MetricsUpdater

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeReference, BoundReference, DynamicPruningExpression, Expression, PlanExpression, Predicate}
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.{FileSourceScanExec, InSubqueryExec, ScalarSubquery, SparkPlan, SQLExecution}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, PartitionDirectory}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.collection.BitSet

class FileSourceScanExecTransformer(@transient relation: HadoopFsRelation,
                                    output: Seq[Attribute],
                                    requiredSchema: StructType,
                                    partitionFilters: Seq[Expression],
                                    optionalBucketSet: Option[BitSet],
                                    optionalNumCoalescedBuckets: Option[Int],
                                    dataFilters: Seq[Expression],
                                    tableIdentifier: Option[TableIdentifier],
                                    disableBucketedScan: Boolean = false)
  extends FileSourceScanExec(
    relation,
    output,
    requiredSchema,
    partitionFilters,
    optionalBucketSet,
    optionalNumCoalescedBuckets,
    dataFilters,
    tableIdentifier,
    disableBucketedScan)
    with BasicScanExecTransformer {

  // Note: "metrics" is made transient to avoid sending driver-side metrics to tasks.
  @transient override lazy val metrics =
    BackendsApiManager.getMetricsApiInstance
    .genFileSourceScanTransformerMetrics(sparkContext) ++ staticMetrics

  /** SQL metrics generated only for scans using dynamic partition pruning. */
  protected lazy val staticMetrics = if (partitionFilters.exists(FileSourceScanExecTransformer
    .isDynamicPruningFilter)) {
    Map("staticFilesNum" -> SQLMetrics.createMetric(sparkContext, "static number of files read"),
      "staticFilesSize" -> SQLMetrics.createSizeMetric(sparkContext, "static size of files read"))
  } else {
    Map.empty[String, SQLMetric]
  }

  override lazy val supportsColumnar: Boolean = {
    /*
    relation.fileFormat
      .supportBatch(relation.sparkSession, schema) && GlutenConfig.getConf.enableColumnarIterator
    */
    GlutenConfig.getConf.enableColumnarIterator
  }

  override def filterExprs(): Seq[Expression] = dataFilters

  override def outputAttributes(): Seq[Attribute] = output

  override def getPartitions: Seq[Seq[InputPartition]] =
    BackendsApiManager.getTransformerApiInstance.genInputPartitionSeq(
      relation, dynamicallySelectedPartitions).map(Seq(_))

  override def getFlattenPartitions: Seq[InputPartition] =
    BackendsApiManager.getTransformerApiInstance.genInputPartitionSeq(
      relation, dynamicallySelectedPartitions)

  override def getPartitionSchemas: StructType = relation.partitionSchema

  override def getInputFilePaths: Seq[String] = relation.location.inputFiles.toSeq

  override def equals(other: Any): Boolean = other match {
    case that: FileSourceScanExecTransformer =>
      (that canEqual this) && super.equals(that)
    case _ => false
  }

  override def canEqual(other: Any): Boolean = other.isInstanceOf[FileSourceScanExecTransformer]

  override def hashCode(): Int = super.hashCode()

  override def columnarInputRDDs: Seq[RDD[ColumnarBatch]] = {
    Seq()
  }

  override def getBuildPlans: Seq[(SparkPlan, SparkPlan)] = {
    Seq((this, null))
  }

  override def getStreamedLeafPlan: SparkPlan = {
    this
  }

  override def doValidateInternal(): Boolean = {
    // Bucketing table has `bucketId` in filename, should apply this in backends
    if (!bucketedScan) {
      super.doValidateInternal()
    } else {
      false
    }
  }

  protected override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    doExecuteColumnarInternal()
  }

  override def metricsUpdater(): MetricsUpdater =
    BackendsApiManager.getMetricsApiInstance.genFileSourceScanTransformerMetricsUpdater(metrics)

  // The codes below are copied from FileSourceScanExec in Spark,
  // all of them are private.
  protected lazy val driverMetrics: HashMap[String, Long] = HashMap.empty

  /**
   * Send the driver-side metrics. Before calling this function, selectedPartitions has
   * been initialized. See SPARK-26327 for more details.
   */
  protected def sendDriverMetrics(): Unit = {
    driverMetrics.foreach(e => metrics(e._1).add(e._2))
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    SQLMetrics.postDriverMetricUpdates(sparkContext, executionId,
      metrics.filter(e => driverMetrics.contains(e._1)).values.toSeq)
  }

  protected def setFilesNumAndSizeMetric(
     partitions: Seq[PartitionDirectory],
     static: Boolean): Unit = {
    val filesNum = partitions.map(_.files.size.toLong).sum
    val filesSize = partitions.map(_.files.map(_.getLen).sum).sum
    if (!static || !partitionFilters.exists(FileSourceScanExecTransformer.isDynamicPruningFilter)) {
      driverMetrics("numFiles") = filesNum
      driverMetrics("filesSize") = filesSize
    } else {
      driverMetrics("staticFilesNum") = filesNum
      driverMetrics("staticFilesSize") = filesSize
    }
    if (relation.partitionSchema.nonEmpty) {
      driverMetrics("numPartitions") = partitions.length
    }
  }

  @transient override lazy val selectedPartitions: Array[PartitionDirectory] = {
    val optimizerMetadataTimeNs = relation.location.metadataOpsTimeNs.getOrElse(0L)
    val startTime = System.nanoTime()
    val ret =
      relation.location.listFiles(
        partitionFilters.filterNot(FileSourceScanExecTransformer.isDynamicPruningFilter),
        dataFilters)
    setFilesNumAndSizeMetric(ret, true)
    val timeTakenMs = NANOSECONDS.toMillis(
      (System.nanoTime() - startTime) + optimizerMetadataTimeNs)
    driverMetrics("metadataTime") = timeTakenMs
    ret
  }.toArray

  // We can only determine the actual partitions at runtime when a dynamic partition filter is
  // present. This is because such a filter relies on information that is only available at run
  // time (for instance the keys used in the other side of a join).
  @transient lazy val dynamicallySelectedPartitions: Array[PartitionDirectory] = {
    val dynamicPartitionFilters = partitionFilters.filter(
      FileSourceScanExecTransformer.isDynamicPruningFilter)
    val selected = if (dynamicPartitionFilters.nonEmpty) {
      // When it includes some DynamicPruningExpression,
      // it needs to execute InSubqueryExec first,
      // because doTransform path can't execute 'doExecuteColumnar' which will
      // execute prepare subquery first.
      dynamicPartitionFilters.foreach {
        case DynamicPruningExpression(inSubquery: InSubqueryExec) =>
          executeInSubqueryForDynamicPruningExpression(inSubquery)
        case e: Expression =>
          e.foreach {
            case s: ScalarSubquery => s.updateResult()
            case _ =>
          }
        case _ =>
      }
      val startTime = System.nanoTime()
      // call the file index for the files matching all filters except dynamic partition filters
      val predicate = dynamicPartitionFilters.reduce(And)
      val partitionColumns = relation.partitionSchema
      val boundPredicate = Predicate.create(predicate.transform {
        case a: AttributeReference =>
          val index = partitionColumns.indexWhere(a.name == _.name)
          BoundReference(index, partitionColumns(index).dataType, nullable = true)
      }, Nil)
      val ret = selectedPartitions.filter(p => boundPredicate.eval(p.values))
      setFilesNumAndSizeMetric(ret, false)
      val timeTakenMs = (System.nanoTime() - startTime) / 1000 / 1000
      driverMetrics("pruningTime") = timeTakenMs
      ret
    } else {
      selectedPartitions
    }
    sendDriverMetrics()
    selected
  }

  override val nodeNamePrefix: String = "NativeFile"
}

object FileSourceScanExecTransformer {
  def isDynamicPruningFilter(e: Expression): Boolean =
    e.find(_.isInstanceOf[PlanExpression[_]]).isDefined
}

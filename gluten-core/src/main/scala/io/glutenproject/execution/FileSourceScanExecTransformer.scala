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

import io.glutenproject.GlutenConfig
import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.expression.ConverterUtils
import io.glutenproject.extension.ValidationResult
import io.glutenproject.metrics.{GlutenTimeMetric, MetricsUpdater}
import io.glutenproject.substrait.SubstraitContext
import io.glutenproject.substrait.rel.LocalFilesNode.ReadFileFormat
import io.glutenproject.substrait.rel.ReadRelNode

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeReference, BoundReference, DynamicPruningExpression, Expression, PlanExpression, Predicate}
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.{FileSourceScanExecShim, InSubqueryExec, ScalarSubquery, SparkPlan, SQLExecution}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, PartitionDirectory}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.collection.BitSet

import java.util.concurrent.TimeUnit.NANOSECONDS

import scala.collection.{mutable, JavaConverters}

class FileSourceScanExecTransformer(
    @transient relation: HadoopFsRelation,
    output: Seq[Attribute],
    requiredSchema: StructType,
    partitionFilters: Seq[Expression],
    optionalBucketSet: Option[BitSet],
    optionalNumCoalescedBuckets: Option[Int],
    dataFilters: Seq[Expression],
    tableIdentifier: Option[TableIdentifier],
    disableBucketedScan: Boolean = false)
  extends FileSourceScanExecShim(
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
  @transient override lazy val metrics: Map[String, SQLMetric] =
    BackendsApiManager.getMetricsApiInstance
      .genFileSourceScanTransformerMetrics(sparkContext) ++ staticMetrics

  /** SQL metrics generated only for scans using dynamic partition pruning. */
  private lazy val staticMetrics =
    if (partitionFilters.exists(FileSourceScanExecTransformer.isDynamicPruningFilter)) {
      Map(
        "staticFilesNum" -> SQLMetrics.createMetric(sparkContext, "static number of files read"),
        "staticFilesSize" -> SQLMetrics.createSizeMetric(sparkContext, "static size of files read")
      )
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

  override def getPartitions: Seq[InputPartition] =
    BackendsApiManager.getTransformerApiInstance.genInputPartitionSeq(
      relation,
      dynamicallySelectedPartitions,
      output,
      optionalBucketSet,
      optionalNumCoalescedBuckets,
      disableBucketedScan)

  override def getPartitionSchemas: StructType = relation.partitionSchema

  override def getDataSchemas: StructType = relation.dataSchema

  override def getInputFilePaths: Seq[String] = {
    if (BackendsApiManager.isVeloxBackend) {
      Seq.empty[String]
    } else {
      relation.location.inputFiles.toSeq
    }
  }

  override def equals(other: Any): Boolean = other match {
    case that: FileSourceScanExecTransformer =>
      that.canEqual(this) && super.equals(that)
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

  override protected def doValidateInternal(): ValidationResult = {
    // Bucketing table has `bucketId` in filename, should apply this in backends
    // TODO Support bucketed scan
    if (bucketedScan && !BackendsApiManager.getSettings.supportBucketScan()) {
      throw new UnsupportedOperationException("Bucketed scan is unsupported for now.")
    }

    if (hasMetadataColumns) {
      return ValidationResult.notOk(s"Unsupported metadataColumns scan in native.")
    }

    if (hasFieldIds) {
      // Spark read schema expects field Ids , the case didn't support yet by native.
      return ValidationResult.notOk(
        s"Unsupported matching schema column names " +
          s"by field ids in native scan.")
    }
    super.doValidateInternal()
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    doExecuteColumnarInternal()
  }

  override def metricsUpdater(): MetricsUpdater =
    BackendsApiManager.getMetricsApiInstance.genFileSourceScanTransformerMetricsUpdater(metrics)

  // The codes below are copied from FileSourceScanExec in Spark,
  // all of them are private.
  protected lazy val driverMetrics: mutable.HashMap[String, Long] = mutable.HashMap.empty

  /**
   * Send the driver-side metrics. Before calling this function, selectedPartitions has been
   * initialized. See SPARK-26327 for more details.
   */
  protected def sendDriverMetrics(): Unit = {
    driverMetrics.foreach(e => metrics(e._1).add(e._2))
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    SQLMetrics.postDriverMetricUpdates(
      sparkContext,
      executionId,
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
    GlutenTimeMetric.withNanoTime {
      val ret =
        relation.location.listFiles(
          partitionFilters.filterNot(FileSourceScanExecTransformer.isDynamicPruningFilter),
          dataFilters)
      setFilesNumAndSizeMetric(ret, static = true)
      ret
    }(t => driverMetrics("metadataTime") = NANOSECONDS.toMillis(t + optimizerMetadataTimeNs))
  }.toArray

  // We can only determine the actual partitions at runtime when a dynamic partition filter is
  // present. This is because such a filter relies on information that is only available at run
  // time (for instance the keys used in the other side of a join).
  @transient lazy val dynamicallySelectedPartitions: Array[PartitionDirectory] = {
    val dynamicPartitionFilters =
      partitionFilters.filter(FileSourceScanExecTransformer.isDynamicPruningFilter)
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
      GlutenTimeMetric.withMillisTime {
        // call the file index for the files matching all filters except dynamic partition filters
        val predicate = dynamicPartitionFilters.reduce(And)
        val partitionColumns = relation.partitionSchema
        val boundPredicate = Predicate.create(
          predicate.transform {
            case a: AttributeReference =>
              val index = partitionColumns.indexWhere(a.name == _.name)
              BoundReference(index, partitionColumns(index).dataType, nullable = true)
          },
          Nil
        )
        val ret = selectedPartitions.filter(p => boundPredicate.eval(p.values))
        setFilesNumAndSizeMetric(ret, static = false)
        ret
      }(t => driverMetrics("pruningTime") = t)
    } else {
      selectedPartitions
    }
    sendDriverMetrics()
    selected
  }

  override val nodeNamePrefix: String = "NativeFile"

  override val nodeName: String = {
    s"NativeScan $relation ${tableIdentifier.map(_.unquotedString).getOrElse("")}"
  }

  override def doTransform(context: SubstraitContext): TransformContext = {
    val transformCtx = super.doTransform(context)
    if (ConverterUtils.getFileFormat(this) == ReadFileFormat.TextReadFormat) {
      var options: Map[String, String] = Map()
      relation.options.foreach {
        case ("delimiter", v) => options += ("field_delimiter" -> v)
        case ("quote", v) => options += ("quote" -> v)
        case ("header", v) =>
          val cnt = if (v == "true") 1 else 0
          options += ("header" -> cnt.toString)
        case ("escape", v) => options += ("escape" -> v)
        case ("nullvalue", v) => options += ("nullValue" -> v)
        case (_, _) =>
      }

      val readRelNode = transformCtx.root.asInstanceOf[ReadRelNode]
      readRelNode.setDataSchema(getDataSchemas)
      readRelNode.setProperties(JavaConverters.mapAsJavaMap(options))
    }
    transformCtx
  }

  @transient override lazy val fileFormat: ReadFileFormat =
    relation.fileFormat.getClass.getSimpleName match {
      case "OrcFileFormat" => ReadFileFormat.OrcReadFormat
      case "ParquetFileFormat" => ReadFileFormat.ParquetReadFormat
      case "DwrfFileFormat" => ReadFileFormat.DwrfReadFormat
      case "DeltaMergeTreeFileFormat" => ReadFileFormat.MergeTreeReadFormat
      case "CSVFileFormat" => ReadFileFormat.TextReadFormat
      case _ => ReadFileFormat.UnknownFormat
    }

  override def doCanonicalize(): FileSourceScanExecTransformer = {
    val canonicalized = super.doCanonicalize()
    new FileSourceScanExecTransformer(
      canonicalized.relation,
      canonicalized.output,
      canonicalized.requiredSchema,
      canonicalized.partitionFilters,
      canonicalized.optionalBucketSet,
      canonicalized.optionalNumCoalescedBuckets,
      canonicalized.dataFilters,
      canonicalized.tableIdentifier,
      canonicalized.disableBucketedScan
    )
  }
}

object FileSourceScanExecTransformer {
  private def isDynamicPruningFilter(e: Expression): Boolean =
    e.find(_.isInstanceOf[PlanExpression[_]]).isDefined
}

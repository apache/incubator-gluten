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

package org.apache.spark.sql.hive

import io.glutenproject.GlutenConfig
import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.execution.{BasicScanExecTransformer, TransformContext}
import io.glutenproject.metrics.MetricsUpdater
import io.glutenproject.sql.shims.SparkShimLoader
import io.glutenproject.substrait.SubstraitContext
import io.glutenproject.substrait.rel.ReadRelNode
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.{ExternalCatalogUtils, HiveTableRelation}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, DynamicPruningExpression, Expression}
import org.apache.spark.sql.connector.read.{InputPartition, SupportsRuntimeFiltering}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.{CatalogFileIndex, DataSourceStrategy, InMemoryFileIndex}
import org.apache.spark.sql.execution.datasources.{DataSourceStrategy, FileStatusCache, InMemoryFileIndex, PartitionPath, PartitionSpec}
import org.apache.spark.sql.execution.datasources.v2.FileScan
import org.apache.spark.sql.execution.datasources.v2.json.JsonScan
import org.apache.spark.sql.execution.datasources.v2.text.TextScan
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.hive.execution.HiveTableScanExec
import org.apache.spark.sql.types.{ArrayType, MapType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.sql.hive.HiveTableScanExecTransformer._

import scala.collection.JavaConverters
import scala.collection.mutable.ArrayBuffer

class HiveTableScanExecTransformer(requestedAttributes: Seq[Attribute],
    relation: HiveTableRelation,
    partitionPruningPred: Seq[Expression])(session: SparkSession)
  extends HiveTableScanExec(requestedAttributes, relation, partitionPruningPred)(session)
  with BasicScanExecTransformer {

  @transient lazy val scan: Option[FileScan] = getHiveTableFileScan

  def getScan: Option[FileScan] = scan

  @transient override lazy val metrics: Map[String, SQLMetric] =
    BackendsApiManager.getMetricsApiInstance.genHiveTableScanTransformerMetrics(sparkContext)

  override def filterExprs(): Seq[Expression] = Seq.empty

  override def outputAttributes(): Seq[Attribute] = output

  override def getPartitions: Seq[Seq[InputPartition]] = filteredPartitions

  override def getFlattenPartitions: Seq[InputPartition] = filteredPartitions.flatten

  override def getPartitionSchemas: StructType = relation.tableMeta.partitionSchema

  override def getInputFilePaths: Seq[String] = {
    val inputPaths = scan match {
      case Some(fileScan) => fileScan.fileIndex.inputFiles.toSeq
      case _ => Seq.empty
    }
    inputPaths
  }

  override def columnarInputRDDs: Seq[RDD[ColumnarBatch]] = {
    Seq.empty
  }

  override def getBuildPlans: Seq[(SparkPlan, SparkPlan)] = {
    Seq((this, null))
  }

  override def getStreamedLeafPlan: SparkPlan = {
    this
  }

  override def metricsUpdater(): MetricsUpdater =
    BackendsApiManager.getMetricsApiInstance.genHiveTableScanTransformerMetricsUpdater(metrics)

  override def supportsColumnar(): Boolean = GlutenConfig.getConf.enableColumnarIterator

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    doExecuteColumnarInternal()
  }

  private val filteredPartitions: Seq[Seq[InputPartition]] = {
    scan match {
      case Some(fileScan) =>
        val dataSourceFilters = partitionPruningPred.flatMap {
          case DynamicPruningExpression(e) => DataSourceStrategy.translateRuntimeFilter(e)
          case _ => None
        }
        if (dataSourceFilters.nonEmpty) {
          // the cast is safe as runtime filters are only assigned if the scan can be filtered
          val filterableScan = fileScan.asInstanceOf[SupportsRuntimeFiltering]
          filterableScan.filter(dataSourceFilters.toArray)
          // call toBatch again to get filtered partitions
          val newPartitions = fileScan.toBatch.planInputPartitions()
          newPartitions.map(Seq(_))
        } else {
          fileScan.toBatch().planInputPartitions().map(Seq(_))
        }
      case _ =>
        Seq.empty
    }
  }

  private def getFileIndex: InMemoryFileIndex = {
    val tableMeta = relation.tableMeta
    val sessionState = session.sessionState
    val fileStatusCache = FileStatusCache.getOrCreate(session)
    if (tableMeta.partitionColumnNames.nonEmpty) {
      val startTime = System.nanoTime()
      val selectedPartitions = ExternalCatalogUtils.listPartitionsByFilter(
        sessionState.conf,
        sessionState.catalog,
        tableMeta,
        partitionPruningPred)
      val partitions = selectedPartitions.map {
        p =>
          val path = new Path(p.location)
          val fs = path.getFileSystem(sessionState.newHadoopConf())
          PartitionPath(
            p.toRow(tableMeta.partitionSchema, sessionState.conf.sessionLocalTimeZone),
            path.makeQualified(fs.getUri, fs.getWorkingDirectory))
      }
      val partitionSpec = PartitionSpec(tableMeta.partitionSchema, partitions)
      val timeNs = System.nanoTime() - startTime
      new InMemoryFileIndex(
        session,
        rootPathsSpecified = partitionSpec.partitions.map(_.path),
        parameters = Map.empty,
        userSpecifiedSchema = Some(partitionSpec.partitionColumns),
        fileStatusCache = fileStatusCache,
        userSpecifiedPartitionSpec = Some(partitionSpec),
        metadataOpsTimeNs = Some(timeNs)
      )
    } else {
      new InMemoryFileIndex(
        session,
        tableMeta.storage.locationUri.map(new Path(_)).toSeq,
        parameters = tableMeta.storage.properties,
        userSpecifiedSchema = None,
        fileStatusCache = fileStatusCache
      )
    }
  }

  private def getHiveTableFileScan: Option[FileScan] = {
    val tableMeta = relation.tableMeta
    val defaultTableSize = session.sessionState.conf.defaultSizeInBytes
    val catalogFileIndex = new CatalogFileIndex(
      session,
      tableMeta,
      tableMeta.stats.map(_.sizeInBytes.toLong).getOrElse(defaultTableSize))
    val fileIndex = catalogFileIndex.filterPartitions(partitionPruningPred)
    val planOutput = output.asInstanceOf[Seq[AttributeReference]]
    var hasComplexType = false
    val outputFieldTypes = new ArrayBuffer[StructField]()
    planOutput.foreach(
      x => {
        hasComplexType = if (!hasComplexType) {
          x.dataType.isInstanceOf[StructType] ||
            x.dataType.isInstanceOf[MapType] ||
            x.dataType.isInstanceOf[ArrayType]
        } else hasComplexType
        outputFieldTypes.append(StructField(x.name, x.dataType, x.nullable))
      })

    tableMeta.storage.inputFormat match {
      case Some("org.apache.hadoop.mapred.TextInputFormat") =>
        tableMeta.storage.serde match {
          case Some("org.openx.data.jsonserde.JsonSerDe") =>
            Some(
              JsonScan(
                session,
                fileIndex,
                tableMeta.schema,
                StructType(outputFieldTypes.toArray),
                tableMeta.partitionSchema,
                new CaseInsensitiveStringMap(JavaConverters.mapAsJavaMap(tableMeta.properties)),
                Array.empty,
                partitionPruningPred,
                Seq.empty
              ))
          case _ =>
            val scan = SparkShimLoader.getSparkShims.getTextScan(
              session,
              fileIndex,
              tableMeta.schema,
              StructType(outputFieldTypes.toArray),
              tableMeta.partitionSchema,
              new CaseInsensitiveStringMap(JavaConverters.mapAsJavaMap(tableMeta.properties)),
              partitionPruningPred,
              Seq.empty
            )
            if (!hasComplexType) {
              Some(scan)
            } else {
              None
            }
        }
      case _ => None
    }
  }

  def createDefaultTextOption(): Map[String, String] = {
    var options: Map[String, String] = Map()
    options += ("field_delimiter" -> DEFAULT_FIELD_DELIMITER.toString)
    options += ("nullValue" -> NULL_VALUE.toString)
    options
  }

  override def doTransform(context: SubstraitContext): TransformContext = {
    val transformCtx = super.doTransform(context)
    if (
      transformCtx.root != null
        && transformCtx.root.isInstanceOf[ReadRelNode]
        && scan.isDefined && scan.get.isInstanceOf[TextScan]
    ) {
      val properties = relation.tableMeta.storage.properties ++ relation.tableMeta.properties
      var options: Map[String, String] = createDefaultTextOption()
      // property key string read from org.apache.hadoop.hive.serde.serdeConstants
      properties.foreach {
        case ("separatorChar", v) => options += ("field_delimiter" -> v)
        case ("field.delim", v) => options += ("field_delimiter" -> v)
        case ("quoteChar", v) => options += ("quote" -> v)
        case ("quote.delim", v) => options += ("quote" -> v)
        case ("skip.header.line.count", v) => options += ("header" -> v)
        case ("escapeChar", v) => options += ("escape" -> v)
        case ("escape.delim", v) => options += ("escape" -> v)
        case ("serialization.null.format", v) => options += ("nullValue" -> v)
        case (_, _) =>
      }
      val readRelNode = transformCtx.root.asInstanceOf[ReadRelNode]
      readRelNode.setDataSchema(relation.tableMeta.dataSchema)
      readRelNode.setProperties(JavaConverters.mapAsJavaMap(options))
    }
    transformCtx
  }

  override def nodeName: String = s"NativeScan hive ${relation.tableMeta.qualifiedName}"

  override def canEqual(other: Any): Boolean = other.isInstanceOf[HiveTableScanExecTransformer]

  override def equals(other: Any): Boolean = other match {
    case that: HiveTableScanExecTransformer =>
      that.canEqual(this) &&
        scan == that.scan &&
        metrics == that.metrics &&
        filteredPartitions == that.filteredPartitions
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(super.hashCode(), scan, metrics, filteredPartitions)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}

object HiveTableScanExecTransformer {

  val NULL_VALUE: Char = 0x00
  val DEFAULT_FIELD_DELIMITER: Char = 0x01

  def isHiveTableScan(plan: SparkPlan): Boolean = {
    plan.isInstanceOf[HiveTableScanExec]
  }

  def validate(plan: SparkPlan): Boolean = {
    plan match {
      case hiveTableScan: HiveTableScanExec =>
        val hiveTableScanTransformer = new HiveTableScanExecTransformer(
          hiveTableScan.requestedAttributes,
          hiveTableScan.relation,
          hiveTableScan.partitionPruningPred)(hiveTableScan.session)
        hiveTableScanTransformer.scan.isDefined && hiveTableScanTransformer.doValidate()
      case _ => false
    }
  }

  def apply(plan: SparkPlan): HiveTableScanExecTransformer = {
    plan match {
      case hiveTableScan: HiveTableScanExec =>
        new HiveTableScanExecTransformer(
          hiveTableScan.requestedAttributes,
          hiveTableScan.relation,
          hiveTableScan.partitionPruningPred)(hiveTableScan.session)
      case _ => throw new UnsupportedOperationException(
        s"Can't transform HiveTableScanExecTransformer from ${plan.getClass.getSimpleName}")
    }
  }
}
